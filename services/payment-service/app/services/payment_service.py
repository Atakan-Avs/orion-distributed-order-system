from app.core.event_factory import create_event
from app.models.outbox_event import OutboxEvent


def process_payment(db, event: dict):
    print(f"[Payment Service] InventoryReserved event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")
    product_name = payload.get("product_name")
    quantity = payload.get("quantity")

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

        outbox_event = OutboxEvent(
            topic="payment-events",
            event_type="PaymentFailed",
            payload=payment_failed_event,
        )

        db.add(outbox_event)

        print(f"[Payment Service] PaymentFailed added to outbox: {payment_failed_event}")
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

    outbox_event = OutboxEvent(
        topic="payment-events",
        event_type="PaymentCompleted",
        payload=payment_completed_event,
    )

    db.add(outbox_event)

    print(f"[Payment Service] PaymentCompleted added to outbox: {payment_completed_event}")