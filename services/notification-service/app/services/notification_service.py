def send_notification(event: dict):
    print(f"[Notification Service] ShippingCreated event received: {event}")

    notification = {
        "event_type": "NotificationSent",
        "order_id": event.get("order_id"),
        "message": "Your order has been shipped successfully.",
        "status": "SENT",
    }

    print(f"[Notification Service] Notification sent: {notification}")