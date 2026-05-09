from threading import Thread

from app.db.database import engine, Base
from app.events.kafka_consumer import start_consumer
from app.events.outbox_publisher import start_outbox_publisher
from app.models.processed_event import ProcessedEvent
from app.models.outbox_event import OutboxEvent


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)

    print("[Inventory Service] Database tables created.")

    consumer_thread = Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    outbox_thread = Thread(target=start_outbox_publisher, daemon=True)
    outbox_thread.start()

    consumer_thread.join()