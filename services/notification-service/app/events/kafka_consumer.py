import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.services.notification_service import send_notification


def start_consumer():
    consumer = KafkaConsumer(
        "shipping-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="notification-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    print("[Notification Service] Listening to shipping-events...")

    for message in consumer:
        event = message.value

        if event.get("event_type") == "ShippingCreated":
            send_notification(event)