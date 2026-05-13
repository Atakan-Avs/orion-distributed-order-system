import json

from kafka import KafkaProducer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    api_version=(2, 8, 0),
)


def publish_event(topic: str, event: dict):
    producer.send(topic, value=event)
    producer.flush()

    print(f"[Notification Kafka Producer] Event published to topic={topic}")