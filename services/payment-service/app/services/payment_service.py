from app.events.kafka_producer import publish_event
from app.core.event_factory import create_event


def process_payment(event: dict):
    print(f"[Payment Service] InventoryReserved event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")
    product_name = payload.get("product_name")
    quantity = payload.get("quantity")

    payment_event = create_event(
        event_type="PaymentCompleted",
        source="payment-service",
        payload={
            "order_id": order_id,
            "product_name": product_name,
            "quantity": quantity,
            "status": "PAID",
        },
        correlation_id=event.get("correlation_id"),
        causation_id=event.get("event_id"),
    )

    publish_event("payment-events", payment_event)

    print(f"[Payment Service] PaymentCompleted event published: {payment_event}")