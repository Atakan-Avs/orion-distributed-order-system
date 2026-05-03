import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.db.database import SessionLocal
from app.services.payment_service import process_payment
from app.services.idempotency_service import is_event_processed, mark_event_processed


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

        event_id = event.get("event_id")
        event_type = event.get("event_type")

        if event_type != "InventoryReserved":
            continue

        if not event_id:
            print("[Payment Service] Event skipped: missing event_id")
            continue

        db = SessionLocal()

        try:
            if is_event_processed(db, event_id):
                print(f"[Payment Service] Duplicate event skipped: {event_id}")
                continue

            process_payment(event)

            mark_event_processed(
                db=db,
                event_id=event_id,
                event_type=event_type,
            )

        except Exception as error:
            db.rollback()
            print(f"[Payment Service] Error while processing event: {error}")

        finally:
            db.close()