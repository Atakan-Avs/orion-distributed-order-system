from app.events.kafka_producer import publish_event
from app.core.event_factory import create_event


def create_shipping(event: dict):
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

    publish_event("shipping-events", shipping_event)

    print(f"[Shipping Service] ShippingCreated published: {shipping_event}")