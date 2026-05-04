from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.events.kafka_consumer import (
    handle_payment_completed,
    handle_inventory_released,
)
from app.models.order import Base, Order


def create_test_db():
    engine = create_engine("sqlite:///:memory:")
    TestingSessionLocal = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
    )
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal


def test_handle_payment_completed_updates_order_status_to_completed(monkeypatch):
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    order = Order(
        id=1,
        product_name="Laptop",
        quantity=2,
        status="PENDING",
    )
    db.add(order)
    db.commit()
    db.close()

    monkeypatch.setattr(
        "app.events.kafka_consumer.SessionLocal",
        TestingSessionLocal,
    )

    event = {
        "event_id": "payment-event-001",
        "event_type": "PaymentCompleted",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 1,
            "product_name": "Laptop",
            "quantity": 2,
            "status": "PAID",
        },
    }

    handle_payment_completed(event)

    db = TestingSessionLocal()
    updated_order = db.query(Order).filter(Order.id == 1).first()

    assert updated_order.status == "COMPLETED"

    db.close()


def test_handle_inventory_released_updates_order_status_and_publishes_order_cancelled(monkeypatch):
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    order = Order(
        id=2,
        product_name="Laptop",
        quantity=6,
        status="PENDING",
    )
    db.add(order)
    db.commit()
    db.close()

    published_events = []

    def fake_publish_event(topic: str, event: dict):
        published_events.append({
            "topic": topic,
            "event": event,
        })

    monkeypatch.setattr(
        "app.events.kafka_consumer.SessionLocal",
        TestingSessionLocal,
    )
    monkeypatch.setattr(
        "app.events.kafka_consumer.publish_event",
        fake_publish_event,
    )

    event = {
        "event_id": "inventory-released-event-001",
        "event_type": "InventoryReleased",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 2,
            "product_name": "Laptop",
            "quantity": 6,
            "status": "RELEASED",
        },
    }

    handle_inventory_released(event)

    db = TestingSessionLocal()
    updated_order = db.query(Order).filter(Order.id == 2).first()

    assert updated_order.status == "CANCELLED"

    assert len(published_events) == 1
    assert published_events[0]["topic"] == "order-events"

    published_event = published_events[0]["event"]

    assert published_event["event_type"] == "OrderCancelled"
    assert published_event["source"] == "order-service"
    assert published_event["correlation_id"] == "correlation-002"
    assert published_event["causation_id"] == "inventory-released-event-001"

    assert published_event["payload"]["order_id"] == 2
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 6
    assert published_event["payload"]["status"] == "CANCELLED"
    assert "reason" in published_event["payload"]

    db.close()