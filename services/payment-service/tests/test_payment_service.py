from app.services.payment_service import process_payment


def test_process_payment_publishes_payment_completed(monkeypatch):
    published_events = []

    def fake_publish_event(topic: str, event: dict):
        published_events.append({
            "topic": topic,
            "event": event,
        })

    monkeypatch.setattr(
        "app.services.payment_service.publish_event",
        fake_publish_event,
    )

    event = {
        "event_id": "inventory-event-001",
        "event_type": "InventoryReserved",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 1,
            "product_name": "Laptop",
            "quantity": 2,
            "status": "RESERVED",
        },
    }

    process_payment(event)

    assert len(published_events) == 1
    assert published_events[0]["topic"] == "payment-events"

    published_event = published_events[0]["event"]

    assert published_event["event_type"] == "PaymentCompleted"
    assert published_event["source"] == "payment-service"
    assert published_event["correlation_id"] == "correlation-001"
    assert published_event["causation_id"] == "inventory-event-001"

    assert published_event["payload"]["order_id"] == 1
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 2
    assert published_event["payload"]["status"] == "PAID"


def test_process_payment_publishes_payment_failed_when_quantity_is_greater_than_five(monkeypatch):
    published_events = []

    def fake_publish_event(topic: str, event: dict):
        published_events.append({
            "topic": topic,
            "event": event,
        })

    monkeypatch.setattr(
        "app.services.payment_service.publish_event",
        fake_publish_event,
    )

    event = {
        "event_id": "inventory-event-002",
        "event_type": "InventoryReserved",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 2,
            "product_name": "Laptop",
            "quantity": 6,
            "status": "RESERVED",
        },
    }

    process_payment(event)

    assert len(published_events) == 1
    assert published_events[0]["topic"] == "payment-events"

    published_event = published_events[0]["event"]

    assert published_event["event_type"] == "PaymentFailed"
    assert published_event["source"] == "payment-service"
    assert published_event["correlation_id"] == "correlation-002"
    assert published_event["causation_id"] == "inventory-event-002"

    assert published_event["payload"]["order_id"] == 2
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 6
    assert published_event["payload"]["status"] == "PAYMENT_FAILED"
    assert "reason" in published_event["payload"]