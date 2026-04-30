from app.events.kafka_producer import publish_event


def create_shipping(event: dict):
    print(f"[Shipping Service] PaymentCompleted received: {event}")

    shipping_event = {
        "event_type": "ShippingCreated",
        "order_id": event.get("order_id"),
        "status": "SHIPPED",
    }

    publish_event("shipping-events", shipping_event)

    print(f"[Shipping Service] ShippingCreated published: {shipping_event}")