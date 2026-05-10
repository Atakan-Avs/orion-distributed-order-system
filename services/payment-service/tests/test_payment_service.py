from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.outbox_event import Base, OutboxEvent
from app.services.payment_service import process_payment


def create_test_db():
    engine = create_engine("sqlite:///:memory:")

    TestingSessionLocal = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
    )

    Base.metadata.create_all(bind=engine)

    return TestingSessionLocal


def test_process_payment_writes_payment_completed_to_outbox():
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    event = {
        "event_id": "inventory-event-001",
        "event_type": "InventoryReserved",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 1,
            "product_name": "Laptop",
            "quantity": 2,
            "status": "RESERVED",
        },
    }

    process_payment(db, event)
    db.commit()

    outbox_event = db.query(OutboxEvent).first()

    assert outbox_event is not None
    assert outbox_event.topic == "payment-events"
    assert outbox_event.event_type == "PaymentCompleted"

    assert outbox_event.payload["event_type"] == "PaymentCompleted"
    assert outbox_event.payload["source"] == "payment-service"
    assert outbox_event.payload["correlation_id"] == "correlation-001"
    assert outbox_event.payload["causation_id"] == "inventory-event-001"

    assert outbox_event.payload["payload"]["order_id"] == 1
    assert outbox_event.payload["payload"]["product_name"] == "Laptop"
    assert outbox_event.payload["payload"]["quantity"] == 2
    assert outbox_event.payload["payload"]["status"] == "PAID"

    db.close()


def test_process_payment_writes_payment_failed_to_outbox_when_quantity_is_greater_than_five():
    TestingSessionLocal = create_test_db()
    db = TestingSessionLocal()

    event = {
        "event_id": "inventory-event-002",
        "event_type": "InventoryReserved",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 2,
            "product_name": "Laptop",
            "quantity": 6,
            "status": "RESERVED",
        },
    }

    process_payment(db, event)
    db.commit()

    outbox_event = db.query(OutboxEvent).first()

    assert outbox_event is not None
    assert outbox_event.topic == "payment-events"
    assert outbox_event.event_type == "PaymentFailed"

    assert outbox_event.payload["event_type"] == "PaymentFailed"
    assert outbox_event.payload["source"] == "payment-service"
    assert outbox_event.payload["correlation_id"] == "correlation-002"
    assert outbox_event.payload["causation_id"] == "inventory-event-002"

    assert outbox_event.payload["payload"]["order_id"] == 2
    assert outbox_event.payload["payload"]["product_name"] == "Laptop"
    assert outbox_event.payload["payload"]["quantity"] == 6
    assert outbox_event.payload["payload"]["status"] == "PAYMENT_FAILED"
    assert "reason" in outbox_event.payload["payload"]

    db.close()