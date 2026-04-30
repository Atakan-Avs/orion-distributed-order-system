from app.events.kafka_producer import publish_event


def process_payment(event: dict):
    print(f"[Payment Service] InventoryReserved event received: {event}")

    order_id = event.get("order_id")
    product_name = event.get("product_name")
    quantity = event.get("quantity")

    payment_event = {
        "event_type": "PaymentCompleted",
        "order_id": order_id,
        "product_name": product_name,
        "quantity": quantity,
        "status": "PAID",
    }

    publish_event("payment-events", payment_event)

    print(f"[Payment Service] PaymentCompleted event published: {payment_event}")