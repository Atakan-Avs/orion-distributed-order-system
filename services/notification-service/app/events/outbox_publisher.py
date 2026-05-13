import time
from datetime import UTC, datetime

from app.db.database import SessionLocal
from app.events.kafka_producer import publish_event
from app.models.outbox_event import OutboxEvent

MAX_RETRY_COUNT = 5
DLQ_TOPIC = "notification-events-dlq"


def publish_to_dlq(event: OutboxEvent, error: Exception):
    dlq_event = {
        "original_outbox_id": event.id,
        "original_topic": event.topic,
        "event_type": event.event_type,
        "payload": event.payload,
        "error": str(error),
        "failed_at": datetime.now(UTC).isoformat(),
        "retry_count": event.retry_count,
    }

    publish_event(DLQ_TOPIC, dlq_event)

    print(
        f"[Notification Outbox Publisher] Event sent to DLQ. "
        f"outbox_id={event.id} dlq_topic={DLQ_TOPIC}"
    )


def start_outbox_publisher():
    print("[Notification Outbox Publisher] Started...")

    while True:
        db = SessionLocal()

        try:
            events = (
                db.query(OutboxEvent)
                .filter(OutboxEvent.processed == False)
                .filter(OutboxEvent.failed == False)
                .order_by(OutboxEvent.created_at.asc())
                .limit(10)
                .all()
            )

            for event in events:
                try:
                    publish_event(event.topic, event.payload)

                    event.processed = True
                    event.processed_at = datetime.now(UTC)
                    event.last_error = None

                    print(
                        f"[Notification Outbox Publisher] Published "
                        f"event_type={event.event_type}"
                    )

                except Exception as error:
                    event.retry_count += 1
                    event.last_error = str(error)

                    if event.retry_count >= MAX_RETRY_COUNT:
                        try:
                            publish_to_dlq(event, error)

                        except Exception as dlq_error:
                            event.last_error = (
                                f"Original publish error: {error}; "
                                f"DLQ publish error: {dlq_error}"
                            )

                            print(
                                f"[Notification Outbox Publisher] DLQ publish failed. "
                                f"outbox_id={event.id} error={dlq_error}"
                            )

                        event.failed = True
                        event.failed_at = datetime.now(UTC)

                        print(
                            f"[Notification Outbox Publisher] Event moved to failed state. "
                            f"outbox_id={event.id} retry_count={event.retry_count}"
                        )

                    else:
                        print(
                            f"[Notification Outbox Publisher] Publish failed. "
                            f"outbox_id={event.id} retry_count={event.retry_count} error={error}"
                        )

            db.commit()

        except Exception as error:
            db.rollback()
            print(f"[Notification Outbox Publisher] Error: {error}")

        finally:
            db.close()

        time.sleep(3)