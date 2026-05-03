from app.events.kafka_consumer import start_consumer

from app.db.database import engine, Base
from app.models.processed_event import ProcessedEvent  # model import şart


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)

    print("[Inventory Service] Database tables created.")

    start_consumer()