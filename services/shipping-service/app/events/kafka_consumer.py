import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.services.shipping_service import create_shipping


def start_consumer():
    consumer = KafkaConsumer(
        "payment-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="shipping-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    print("[Shipping Service] Listening to payment-events...")

    for message in consumer:
        event = message.value

        if event.get("event_type") == "PaymentCompleted":
            create_shipping(event)