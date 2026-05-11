from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker

from app.api.routes import get_db
from app.main import app
from app.models.order import Base as OrderBase
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


app.dependency_overrides[get_db] = override_get_db


def test_list_failed_outbox_events_returns_only_failed_events():
    app.dependency_overrides[get_db] = override_get_db
    db = TestingSessionLocal()

    db.query(OutboxEvent).delete()
    db.commit()

    failed_event = OutboxEvent(
        topic="order-events",
        event_type="OrderCancelled",
        payload={"test": "failed"},
        processed=False,
        failed=True,
        retry_count=5,
        last_error="Kafka connection error",
    )

    successful_event = OutboxEvent(
        topic="order-events",
        event_type="OrderCreated",
        payload={"test": "success"},
        processed=True,
        failed=False,
    )

    db.add(failed_event)
    db.add(successful_event)
    db.commit()
    db.close()

    response = client.get("/admin/outbox/failed")

    assert response.status_code == 200

    data = response.json()

    assert len(data) == 1
    assert data[0]["event_type"] == "OrderCancelled"
    assert data[0]["failed"] is True
    assert data[0]["retry_count"] == 5
    assert data[0]["last_error"] == "Kafka connection error"


def test_retry_outbox_event_resets_failed_state():
    app.dependency_overrides[get_db] = override_get_db
    db = TestingSessionLocal()

    db.query(OutboxEvent).delete()
    db.commit()

    failed_event = OutboxEvent(
        topic="payment-events",
        event_type="PaymentCompleted",
        payload={"test": "retry"},
        processed=False,
        failed=True,
        retry_count=5,
        last_error="Temporary Kafka failure",
    )

    db.add(failed_event)
    db.commit()
    db.refresh(failed_event)

    event_id = failed_event.id

    db.close()

    response = client.post(f"/admin/outbox/{event_id}/retry")

    assert response.status_code == 200

    data = response.json()

    assert data["failed"] is False
    assert data["retry_count"] == 0
    assert data["last_error"] is None

    db = TestingSessionLocal()

    updated_event = (
        db.query(OutboxEvent)
        .filter(OutboxEvent.id == event_id)
        .first()
    )

    assert updated_event.failed is False
    assert updated_event.retry_count == 0
    assert updated_event.last_error is None

    db.close()