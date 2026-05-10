from app.core.event_factory import create_event
from app.models.outbox_event import OutboxEvent


def create_shipping(db, event: dict):
    print(f"[Shipping Service] PaymentCompleted received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")

    shipping_event = create_event(
        event_type="ShippingCreated",
        source="shipping-service",
        payload={
            "order_id": order_id,
            "status": "SHIPPED",
        },
        correlation_id=event.get("correlation_id"),
        causation_id=event.get("event_id"),
    )

    outbox_event = OutboxEvent(
        topic="shipping-events",
        event_type="ShippingCreated",
        payload=shipping_event,
    )

    db.add(outbox_event)

    print(f"[Shipping Service] ShippingCreated added to outbox: {shipping_event}")