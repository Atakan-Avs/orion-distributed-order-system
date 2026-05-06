from app.services.notification_service import (
    send_shipping_notification,
    send_cancellation_notification,
)


def test_send_shipping_notification_creates_notification_event(monkeypatch):
    created_events = []

    def fake_create_event(
        event_type: str,
        payload: dict,
        source: str,
        correlation_id: str = None,
        causation_id: str = None,
    ):
        event = {
            "event_type": event_type,
            "payload": payload,
            "source": source,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
        }
        created_events.append(event)
        return event

    monkeypatch.setattr(
        "app.services.notification_service.create_event",
        fake_create_event,
    )

    event = {
        "event_id": "shipping-event-001",
        "event_type": "ShippingCreated",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 1,
            "status": "SHIPPED",
        },
    }

    send_shipping_notification(event)

    assert len(created_events) == 1

    notification_event = created_events[0]

    assert notification_event["event_type"] == "NotificationSent"
    assert notification_event["source"] == "notification-service"
    assert notification_event["correlation_id"] == "correlation-001"
    assert notification_event["causation_id"] == "shipping-event-001"

    assert notification_event["payload"]["order_id"] == 1
    assert notification_event["payload"]["message"] == "Your order has been shipped successfully."
    assert notification_event["payload"]["status"] == "SENT"


def test_send_cancellation_notification_creates_notification_event(monkeypatch):
    created_events = []

    def fake_create_event(
        event_type: str,
        payload: dict,
        source: str,
        correlation_id: str = None,
        causation_id: str = None,
    ):
        event = {
            "event_type": event_type,
            "payload": payload,
            "source": source,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
        }
        created_events.append(event)
        return event

    monkeypatch.setattr(
        "app.services.notification_service.create_event",
        fake_create_event,
    )

    event = {
        "event_id": "order-cancelled-event-001",
        "event_type": "OrderCancelled",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 2,
            "reason": "Order cancelled because payment failed and inventory was released.",
        },
    }

    send_cancellation_notification(event)

    assert len(created_events) == 1

    notification_event = created_events[0]

    assert notification_event["event_type"] == "NotificationSent"
    assert notification_event["source"] == "notification-service"
    assert notification_event["correlation_id"] == "correlation-002"
    assert notification_event["causation_id"] == "order-cancelled-event-001"

    assert notification_event["payload"]["order_id"] == 2
    assert notification_event["payload"]["status"] == "SENT"
    assert "Your order has been cancelled" in notification_event["payload"]["message"]
    assert "payment failed" in notification_event["payload"]["message"]