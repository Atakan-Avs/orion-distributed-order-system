from fastapi import FastAPI
from threading import Thread

from app.api.routes import router
from app.models.order import Base
from app.db.session import engine
from app.events.kafka_consumer import start_consumer

app = FastAPI(title="Order Service")

Base.metadata.create_all(bind=engine)

app.include_router(router)


@app.on_event("startup")
def start_kafka_consumer():
    thread = Thread(target=start_consumer, daemon=True)
    thread.start()