from app.services.shipping_service import create_shipping


class FakeDB:
    def __init__(self):
        self.added_items = []

    def add(self, item):
        self.added_items.append(item)


def test_create_shipping_adds_shipping_created_event_to_outbox():
    db = FakeDB()

    payment_completed_event = {
        "event_id": "payment-event-123",
        "event_type": "PaymentCompleted",
        "correlation_id": "correlation-123",
        "payload": {
            "order_id": 1,
            "payment_status": "PAID",
        },
    }

    create_shipping(db, payment_completed_event)

    assert len(db.added_items) == 1

    outbox_event = db.added_items[0]

    assert outbox_event.topic == "shipping-events"
    assert outbox_event.event_type == "ShippingCreated"

    payload = outbox_event.payload

    assert payload["event_type"] == "ShippingCreated"
    assert payload["source"] == "shipping-service"
    assert payload["version"] == 1

    assert payload["correlation_id"] == "correlation-123"
    assert payload["causation_id"] == "payment-event-123"

    assert payload["payload"]["order_id"] == 1
    assert payload["payload"]["status"] == "SHIPPED"

    assert "event_id" in payload
    assert "occurred_at" in payload


def test_create_shipping_generates_correlation_id_when_missing():
    db = FakeDB()

    payment_completed_event = {
        "event_id": "payment-event-456",
        "event_type": "PaymentCompleted",
        "payload": {
            "order_id": 2,
        },
    }

    create_shipping(db, payment_completed_event)

    outbox_event = db.added_items[0]
    payload = outbox_event.payload

    assert payload["correlation_id"] is not None
    assert payload["causation_id"] == "payment-event-456"
    assert payload["payload"]["order_id"] == 2
    assert payload["payload"]["status"] == "SHIPPED"


def test_create_shipping_handles_missing_payload_gracefully():
    db = FakeDB()

    payment_completed_event = {
        "event_id": "payment-event-789",
        "event_type": "PaymentCompleted",
        "correlation_id": "correlation-789",
    }

    create_shipping(db, payment_completed_event)

    outbox_event = db.added_items[0]
    payload = outbox_event.payload

    assert payload["event_type"] == "ShippingCreated"
    assert payload["correlation_id"] == "correlation-789"
    assert payload["causation_id"] == "payment-event-789"

    assert payload["payload"]["order_id"] is None
    assert payload["payload"]["status"] == "SHIPPED"