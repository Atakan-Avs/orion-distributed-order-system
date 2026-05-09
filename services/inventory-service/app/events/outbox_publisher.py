import time
from datetime import datetime

from app.db.database import SessionLocal
from app.events.kafka_producer import publish_event
from app.models.outbox_event import OutboxEvent

MAX_RETRY_COUNT = 5


def start_outbox_publisher():
    print("[Inventory Outbox Publisher] Started...")

    while True:
        db = SessionLocal()

        try:
            events = (
                db.query(OutboxEvent)
                .filter(OutboxEvent.processed == False)
                .filter(OutboxEvent.failed == False)
                .all()
            )

            for event in events:
                try:
                    publish_event(event.topic, event.payload)

                    event.processed = True
                    event.processed_at = datetime.utcnow()
                    event.last_error = None

                except Exception as error:
                    event.retry_count += 1
                    event.last_error = str(error)

                    if event.retry_count >= MAX_RETRY_COUNT:
                        event.failed = True
                        event.failed_at = datetime.utcnow()

            db.commit()

        except Exception as error:
            db.rollback()
            print(f"[Inventory Outbox Publisher] Error: {error}")

        finally:
            db.close()

        time.sleep(3)