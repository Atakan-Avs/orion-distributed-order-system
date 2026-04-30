import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.services.payment_service import process_payment


def start_consumer():
    consumer = KafkaConsumer(
        "inventory-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="payment-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    print("[Payment Service] Listening to inventory-events topic...")

    for message in consumer:
        event = message.value

        if event.get("event_type") == "InventoryReserved":
            process_payment(event)