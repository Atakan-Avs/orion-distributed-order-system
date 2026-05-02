from app.core.event_factory import create_event


def send_notification(event: dict):
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

    print(f"[Notification Service] Notification sent: {notification_event}")