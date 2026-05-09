from threading import Thread

from fastapi import FastAPI

from app.api.routes import router
from app.db.session import engine
from app.events.kafka_consumer import start_consumer
from app.events.outbox_publisher import start_outbox_publisher
from app.models.order import Base
from app.models.outbox_event import OutboxEvent

app = FastAPI(title="Order Service")

Base.metadata.create_all(bind=engine)

app.include_router(router)


@app.on_event("startup")
def startup_event():
    consumer_thread = Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    outbox_thread = Thread(target=start_outbox_publisher, daemon=True)
    outbox_thread.start()