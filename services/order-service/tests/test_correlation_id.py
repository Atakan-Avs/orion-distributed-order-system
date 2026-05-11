from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.routes import get_db
from app.main import app
from app.models.order import Base as OrderBase
from app.models.outbox_event import (
    Base as OutboxBase,
    OutboxEvent,
)


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


def test_create_order_propagates_correlation_id():
    app.dependency_overrides[get_db] = override_get_db

    correlation_id = "test-correlation-id-123"

    response = client.post(
        "/orders",
        json={
            "product_name": "Laptop",
            "quantity": 2,
        },
        headers={
            "X-Correlation-ID": correlation_id,
        },
    )

    assert response.status_code == 200

    db = TestingSessionLocal()

    outbox_event = db.query(OutboxEvent).first()

    assert outbox_event is not None

    payload = outbox_event.payload

    assert payload["correlation_id"] == correlation_id

    db.close()

    app.dependency_overrides.clear()