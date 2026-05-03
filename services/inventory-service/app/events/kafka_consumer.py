import json
from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.db.database import SessionLocal
from app.services.inventory_service import reserve_inventory
from app.services.idempotency_service import is_event_processed, mark_event_processed


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

        event_id = event.get("event_id")
        event_type = event.get("event_type")

        if event_type != "OrderCreated":
            continue

        if not event_id:
            print("[Inventory Service] Event skipped: missing event_id")
            continue

        db = SessionLocal()

        try:
            if is_event_processed(db, event_id):
                print(f"[Inventory Service] Duplicate event skipped: {event_id}")
                continue

            reserve_inventory(event)

            mark_event_processed(
                db=db,
                event_id=event_id,
                event_type=event_type,
            )

        except Exception as error:
            db.rollback()
            print(f"[Inventory Service] Error while processing event: {error}")

        finally:
            db.close()