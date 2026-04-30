import json
from kafka import KafkaProducer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def publish_event(topic: str, event: dict):
    producer = create_producer()
    producer.send(topic, event)
    producer.flush()
    producer.close()