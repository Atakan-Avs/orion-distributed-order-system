import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.services.inventory_service import reserve_inventory


def start_consumer():
    consumer = KafkaConsumer(
        "order-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="inventory-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print("[Inventory Service] Listening to order-events topic...")

    for message in consumer:
        event = message.value

        if event.get("event_type") == "OrderCreated":
            reserve_inventory(event)