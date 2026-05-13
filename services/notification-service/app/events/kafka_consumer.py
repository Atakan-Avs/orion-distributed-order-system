import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.db.database import SessionLocal
from app.services.notification_service import (
    send_shipping_notification,
    send_cancellation_notification,
)
from app.services.idempotency_service import is_event_processed, mark_event_processed


def start_consumer():
    consumer = KafkaConsumer(
        "shipping-events",
        "order-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="notification-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    print("[Notification Service] Listening to shipping-events and order-events...")

    for message in consumer:
        event = message.value

        event_id = event.get("event_id")
        event_type = event.get("event_type")

        if event_type not in ["ShippingCreated", "OrderCancelled"]:
            continue

        if not event_id:
            print("[Notification Service] Event skipped: missing event_id")
            continue

        db = SessionLocal()

        try:
            if is_event_processed(db, event_id):
                print(f"[Notification Service] Duplicate event skipped: {event_id}")
                continue

            if event_type == "ShippingCreated":
                send_shipping_notification(db, event)

            elif event_type == "OrderCancelled":
                send_cancellation_notification(db, event)

            mark_event_processed(
                db=db,
                event_id=event_id,
                event_type=event_type,
            )

            db.commit()

        except Exception as error:
            db.rollback()
            print(f"[Notification Service] Error while processing event: {error}")

        finally:
            db.close()