from contextlib import asynccontextmanager
from threading import Thread

from fastapi import FastAPI

from app.api.routes import router
from app.events.kafka_consumer import start_consumer
from app.events.outbox_publisher import start_outbox_publisher
from app.middleware.correlation import CorrelationIdMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_thread = Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    outbox_thread = Thread(target=start_outbox_publisher, daemon=True)
    outbox_thread.start()

    yield


app = FastAPI(
    title="Order Service",
    lifespan=lifespan,
)

app.add_middleware(CorrelationIdMiddleware)
app.include_router(router)