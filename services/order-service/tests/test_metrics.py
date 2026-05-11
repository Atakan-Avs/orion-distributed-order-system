from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.routes import get_db
from app.main import app
from app.models.order import Base as OrderBase
from app.models.order import Order, OrderStatus
from app.models.outbox_event import Base as OutboxBase
from app.models.outbox_event import OutboxEvent


engine = create_engine(
    "sqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

OrderBase.metadata.create_all(bind=engine)
OutboxBase.metadata.create_all(bind=engine)

client = TestClient(app)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_metrics_endpoint_returns_order_and_outbox_metrics():
    app.dependency_overrides[get_db] = override_get_db

    db = TestingSessionLocal()

    db.query(Order).delete()
    db.query(OutboxEvent).delete()
    db.commit()

    completed_order = Order(
        product_name="Laptop",
        quantity=2,
        status=OrderStatus.COMPLETED,
    )

    cancelled_order = Order(
        product_name="Phone",
        quantity=6,
        status=OrderStatus.CANCELLED,
    )

    pending_outbox_event = OutboxEvent(
        topic="order-events",
        event_type="OrderCreated",
        payload={"test": "pending"},
        processed=False,
        failed=False,
    )

    failed_outbox_event = OutboxEvent(
        topic="order-events",
        event_type="OrderCancelled",
        payload={"test": "failed"},
        processed=False,
        failed=True,
        retry_count=5,
        last_error="Kafka error",
    )

    db.add(completed_order)
    db.add(cancelled_order)
    db.add(pending_outbox_event)
    db.add(failed_outbox_event)
    db.commit()
    db.close()

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")

    metrics_text = response.text

    assert "orion_orders_created_total 2" in metrics_text
    assert "orion_saga_completed_total 1" in metrics_text
    assert "orion_saga_cancelled_total 1" in metrics_text
    assert "orion_outbox_pending_total 1" in metrics_text
    assert "orion_outbox_failed_total 1" in metrics_text

    app.dependency_overrides.clear()