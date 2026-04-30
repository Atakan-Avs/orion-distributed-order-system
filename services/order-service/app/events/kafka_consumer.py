import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.db.session import SessionLocal
from app.models.order import Order


def start_consumer():
    consumer = KafkaConsumer(
        "payment-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="order-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    print("[Order Service] Listening to payment-events...")

    for message in consumer:
        event = message.value

        if event.get("event_type") == "PaymentCompleted":
            handle_payment_completed(event)


def handle_payment_completed(event):
    db = SessionLocal()

    try:
        order_id = event.get("order_id")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order:
            order.status = "COMPLETED"
            db.commit()

            print(f"[Order Service] Order {order_id} COMPLETED ✅")

    finally:
        db.close()