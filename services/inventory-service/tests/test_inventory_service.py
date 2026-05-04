from app.services.inventory_service import reserve_inventory, release_inventory


def test_reserve_inventory_publishes_inventory_reserved(monkeypatch):
    published_events = []

    def fake_publish_event(topic: str, event: dict):
        published_events.append({
            "topic": topic,
            "event": event,
        })

    monkeypatch.setattr(
        "app.services.inventory_service.publish_event",
        fake_publish_event,
    )

    event = {
        "event_id": "order-event-001",
        "event_type": "OrderCreated",
        "correlation_id": "correlation-001",
        "payload": {
            "order_id": 1,
            "product_name": "Laptop",
            "quantity": 2,
            "status": "PENDING",
        },
    }

    reserve_inventory(event)

    assert len(published_events) == 1
    assert published_events[0]["topic"] == "inventory-events"

    published_event = published_events[0]["event"]

    assert published_event["event_type"] == "InventoryReserved"
    assert published_event["source"] == "inventory-service"
    assert published_event["correlation_id"] == "correlation-001"
    assert published_event["causation_id"] == "order-event-001"

    assert published_event["payload"]["order_id"] == 1
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 2
    assert published_event["payload"]["status"] == "RESERVED"


def test_release_inventory_publishes_inventory_released(monkeypatch):
    published_events = []

    def fake_publish_event(topic: str, event: dict):
        published_events.append({
            "topic": topic,
            "event": event,
        })

    monkeypatch.setattr(
        "app.services.inventory_service.publish_event",
        fake_publish_event,
    )

    event = {
        "event_id": "payment-failed-event-001",
        "event_type": "PaymentFailed",
        "correlation_id": "correlation-002",
        "payload": {
            "order_id": 2,
            "product_name": "Laptop",
            "quantity": 6,
            "status": "PAYMENT_FAILED",
        },
    }

    release_inventory(event)

    assert len(published_events) == 1
    assert published_events[0]["topic"] == "inventory-events"

    published_event = published_events[0]["event"]

    assert published_event["event_type"] == "InventoryReleased"
    assert published_event["source"] == "inventory-service"
    assert published_event["correlation_id"] == "correlation-002"
    assert published_event["causation_id"] == "payment-failed-event-001"

    assert published_event["payload"]["order_id"] == 2
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 6
    assert published_event["payload"]["status"] == "RELEASED"