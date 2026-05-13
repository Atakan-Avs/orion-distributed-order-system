from app.services.notification_service import (
    send_shipping_notification,
    send_cancellation_notification,
)


class FakeDB:
    def __init__(self):
        self.added_items = []

    def add(self, item):
        self.added_items.append(item)


def test_send_shipping_notification_adds_notification_sent_event_to_outbox():
    db = FakeDB()

    shipping_created_event = {
        "event_id": "shipping-event-001",
        "event_type": "ShippingCreated",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 1,
            "status": "SHIPPED",
        },
    }

    send_shipping_notification(db, shipping_created_event)

    assert len(db.added_items) == 1

    outbox_event = db.added_items[0]

    assert outbox_event.topic == "notification-events"
    assert outbox_event.event_type == "NotificationSent"

    payload = outbox_event.payload

    assert payload["event_type"] == "NotificationSent"
    assert payload["source"] == "notification-service"
    assert payload["correlation_id"] == "correlation-001"
    assert payload["causation_id"] == "shipping-event-001"

    assert payload["payload"]["order_id"] == 1
    assert payload["payload"]["message"] == "Your order has been shipped successfully."
    assert payload["payload"]["status"] == "SENT"

    assert "event_id" in payload
    assert "occurred_at" in payload


def test_send_cancellation_notification_adds_notification_sent_event_to_outbox():
    db = FakeDB()

    order_cancelled_event = {
        "event_id": "order-cancelled-event-001",
        "event_type": "OrderCancelled",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 2,
            "reason": "Order cancelled because payment failed and inventory was released.",
        },
    }

    send_cancellation_notification(db, order_cancelled_event)

    assert len(db.added_items) == 1

    outbox_event = db.added_items[0]

    assert outbox_event.topic == "notification-events"
    assert outbox_event.event_type == "NotificationSent"

    payload = outbox_event.payload

    assert payload["event_type"] == "NotificationSent"
    assert payload["source"] == "notification-service"
    assert payload["correlation_id"] == "correlation-002"
    assert payload["causation_id"] == "order-cancelled-event-001"

    assert payload["payload"]["order_id"] == 2
    assert payload["payload"]["status"] == "SENT"
    assert "Your order has been cancelled" in payload["payload"]["message"]
    assert "payment failed" in payload["payload"]["message"]


def test_send_cancellation_notification_uses_default_reason_when_missing():
    db = FakeDB()

    order_cancelled_event = {
        "event_id": "order-cancelled-event-002",
        "event_type": "OrderCancelled",
        "correlation_id": "correlation-003",
        "payload": {
            "order_id": 3,
        },
    }

    send_cancellation_notification(db, order_cancelled_event)

    outbox_event = db.added_items[0]
    payload = outbox_event.payload

    assert payload["payload"]["order_id"] == 3
    assert payload["payload"]["status"] == "SENT"
    assert "Order was cancelled." in payload["payload"]["message"]


def test_send_shipping_notification_handles_missing_payload_gracefully():
    db = FakeDB()

    shipping_created_event = {
        "event_id": "shipping-event-002",
        "event_type": "ShippingCreated",
        "correlation_id": "correlation-004",
    }

    send_shipping_notification(db, shipping_created_event)

    outbox_event = db.added_items[0]
    payload = outbox_event.payload

    assert payload["event_type"] == "NotificationSent"
    assert payload["correlation_id"] == "correlation-004"
    assert payload["causation_id"] == "shipping-event-002"
    assert payload["payload"]["order_id"] is None
    assert payload["payload"]["status"] == "SENT"