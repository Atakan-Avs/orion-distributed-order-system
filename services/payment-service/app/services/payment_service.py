from app.events.kafka_producer import publish_event
from app.core.event_factory import create_event


def process_payment(event: dict):
    print(f"[Payment Service] InventoryReserved event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")
    product_name = payload.get("product_name")
    quantity = payload.get("quantity")

    # Demo failure rule:
    # If quantity is greater than 5, payment fails.
    # This allows us to test Saga compensation flow easily.
    if quantity and quantity > 5:
        payment_failed_event = create_event(
            event_type="PaymentFailed",
            source="payment-service",
            payload={
                "order_id": order_id,
                "product_name": product_name,
                "quantity": quantity,
                "status": "PAYMENT_FAILED",
                "reason": "Payment declined because quantity is greater than allowed limit.",
            },
            correlation_id=event.get("correlation_id"),
            causation_id=event.get("event_id"),
        )

        publish_event("payment-events", payment_failed_event)

        print(f"[Payment Service] PaymentFailed event published: {payment_failed_event}")
        return

    payment_completed_event = create_event(
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

    publish_event("payment-events", payment_completed_event)

    print(f"[Payment Service] PaymentCompleted event published: {payment_completed_event}")