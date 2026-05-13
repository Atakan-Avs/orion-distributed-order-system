from app.core.event_factory import create_event
from app.models.outbox_event import OutboxEvent


def send_shipping_notification(db, event: dict):
    print(f"[Notification Service] ShippingCreated event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")

    notification_event = create_event(
        event_type="NotificationSent",
        source="notification-service",
        payload={
            "order_id": order_id,
            "message": "Your order has been shipped successfully.",
            "status": "SENT",
        },
        correlation_id=event.get("correlation_id"),
        causation_id=event.get("event_id"),
    )

    outbox_event = OutboxEvent(
        topic="notification-events",
        event_type="NotificationSent",
        payload=notification_event,
    )

    db.add(outbox_event)

    print(f"[Notification Service] NotificationSent added to outbox: {notification_event}")


def send_cancellation_notification(db, event: dict):
    print(f"[Notification Service] OrderCancelled event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")
    reason = payload.get("reason", "Order was cancelled.")

    notification_event = create_event(
        event_type="NotificationSent",
        source="notification-service",
        payload={
            "order_id": order_id,
            "message": f"Your order has been cancelled. Reason: {reason}",
            "status": "SENT",
        },
        correlation_id=event.get("correlation_id"),
        causation_id=event.get("event_id"),
    )

    outbox_event = OutboxEvent(
        topic="notification-events",
        event_type="NotificationSent",
        payload=notification_event,
    )

    db.add(outbox_event)

    print(f"[Notification Service] NotificationSent added to outbox: {notification_event}")