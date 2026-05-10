from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.events.kafka_consumer import (
    handle_inventory_released,
    handle_payment_completed,
    handle_shipping_created,
)
from app.models.order import Base, Order, OrderStatus
from app.models.outbox_event import OutboxEvent


def create_test_db():
    engine = create_engine("sqlite:///:memory:")
    TestingSessionLocal = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
    )
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal


def test_handle_payment_completed_updates_order_status_to_payment_completed(monkeypatch):
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    order = Order(
        id=1,
        product_name="Laptop",
        quantity=2,
        status=OrderStatus.INVENTORY_RESERVED,
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

    assert updated_order.status == OrderStatus.PAYMENT_COMPLETED

    db.close()


def test_handle_shipping_created_updates_order_status_to_completed(monkeypatch):
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    order = Order(
        id=2,
        product_name="Laptop",
        quantity=2,
        status=OrderStatus.PAYMENT_COMPLETED,
    )
    db.add(order)
    db.commit()
    db.close()

    monkeypatch.setattr(
        "app.events.kafka_consumer.SessionLocal",
        TestingSessionLocal,
    )

    event = {
        "event_id": "shipping-event-001",
        "event_type": "ShippingCreated",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 2,
            "product_name": "Laptop",
            "quantity": 2,
            "status": "SHIPPING_CREATED",
        },
    }

    handle_shipping_created(event)

    db = TestingSessionLocal()
    updated_order = db.query(Order).filter(Order.id == 2).first()

    assert updated_order.status == OrderStatus.COMPLETED

    db.close()


def test_handle_inventory_released_updates_order_status_and_writes_order_cancelled_to_outbox(monkeypatch):
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    order = Order(
        id=3,
        product_name="Laptop",
        quantity=6,
        status=OrderStatus.INVENTORY_RESERVED,
    )
    db.add(order)
    db.commit()
    db.close()

    monkeypatch.setattr(
        "app.events.kafka_consumer.SessionLocal",
        TestingSessionLocal,
    )

    event = {
        "event_id": "inventory-released-event-001",
        "event_type": "InventoryReleased",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 3,
            "product_name": "Laptop",
            "quantity": 6,
            "status": "RELEASED",
        },
    }

    handle_inventory_released(event)

    db = TestingSessionLocal()
    updated_order = db.query(Order).filter(Order.id == 3).first()

    assert updated_order.status == OrderStatus.CANCELLED

    outbox_event = db.query(OutboxEvent).filter(
        OutboxEvent.event_type == "OrderCancelled"
    ).first()

    assert outbox_event is not None
    assert outbox_event.topic == "order-events"
    assert outbox_event.payload["event_type"] == "OrderCancelled"
    assert outbox_event.payload["source"] == "order-service"
    assert outbox_event.payload["correlation_id"] == "correlation-002"
    assert outbox_event.payload["causation_id"] == "inventory-released-event-001"

    assert outbox_event.payload["payload"]["order_id"] == 3
    assert outbox_event.payload["payload"]["product_name"] == "Laptop"
    assert outbox_event.payload["payload"]["quantity"] == 6
    assert outbox_event.payload["payload"]["status"] == OrderStatus.CANCELLED
    assert "reason" in outbox_event.payload["payload"]

    db.close()