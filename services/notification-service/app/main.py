from app.events.kafka_consumer import start_consumer

from app.db.database import engine, Base
from app.models.processed_event import ProcessedEvent


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)

    print("[Notification Service] Database tables created.")

    start_consumer()