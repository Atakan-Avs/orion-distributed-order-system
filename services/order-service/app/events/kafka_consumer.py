import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.core.event_factory import create_event
from app.db.session import SessionLocal
from app.events.kafka_producer import publish_event
from app.models.order import Order


def start_consumer():
    consumer = KafkaConsumer(
        "payment-events",
        "inventory-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="order-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    print("[Order Service] Listening to payment-events and inventory-events...")

    for message in consumer:
        event = message.value
        event_type = event.get("event_type")

        if event_type == "PaymentCompleted":
            handle_payment_completed(event)

        elif event_type == "InventoryReleased":
            handle_inventory_released(event)


def handle_payment_completed(event: dict):
    db = SessionLocal()

    try:
        payload = event.get("payload", {})
        order_id = payload.get("order_id")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order:
            order.status = "COMPLETED"
            db.commit()

            print(f"[Order Service] Order {order_id} COMPLETED ✅")

    finally:
        db.close()


def handle_inventory_released(event: dict):
    db = SessionLocal()

    try:
        payload = event.get("payload", {})

        order_id = payload.get("order_id")
        product_name = payload.get("product_name")
        quantity = payload.get("quantity")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order:
            order.status = "CANCELLED"
            db.commit()

            order_cancelled_event = create_event(
                event_type="OrderCancelled",
                source="order-service",
                payload={
                    "order_id": order_id,
                    "product_name": product_name,
                    "quantity": quantity,
                    "status": "CANCELLED",
                    "reason": "Order cancelled because payment failed and inventory was released.",
                },
                correlation_id=event.get("correlation_id"),
                causation_id=event.get("event_id"),
            )

            publish_event("order-events", order_cancelled_event)

            print(f"[Order Service] Order {order_id} CANCELLED due to payment failure ❌")
            print(f"[Order Service] OrderCancelled event published: {order_cancelled_event}")

    finally:
        db.close()