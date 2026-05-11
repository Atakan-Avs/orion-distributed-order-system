from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health_endpoint_returns_ok_status():
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["service"] == "order-service"
    assert data["status"] == "ok"