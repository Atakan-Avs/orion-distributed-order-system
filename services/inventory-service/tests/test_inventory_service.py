from app.models.outbox_event import OutboxEvent
from app.services.inventory_service import release_inventory, reserve_inventory


class FakeDB:
    def __init__(self):
        self.added_items = []

    def add(self, item):
        self.added_items.append(item)


def test_reserve_inventory_writes_inventory_reserved_to_outbox():
    db = FakeDB()

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

    reserve_inventory(db, event)

    assert len(db.added_items) == 1

    outbox_event = db.added_items[0]

    assert isinstance(outbox_event, OutboxEvent)
    assert outbox_event.topic == "inventory-events"
    assert outbox_event.event_type == "InventoryReserved"

    published_event = outbox_event.payload

    assert published_event["event_type"] == "InventoryReserved"
    assert published_event["source"] == "inventory-service"
    assert published_event["correlation_id"] == "correlation-001"
    assert published_event["causation_id"] == "order-event-001"

    assert published_event["payload"]["order_id"] == 1
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 2
    assert published_event["payload"]["status"] == "RESERVED"


def test_release_inventory_writes_inventory_released_to_outbox():
    db = FakeDB()

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

    release_inventory(db, event)

    assert len(db.added_items) == 1

    outbox_event = db.added_items[0]

    assert isinstance(outbox_event, OutboxEvent)
    assert outbox_event.topic == "inventory-events"
    assert outbox_event.event_type == "InventoryReleased"

    published_event = outbox_event.payload

    assert published_event["event_type"] == "InventoryReleased"
    assert published_event["source"] == "inventory-service"
    assert published_event["correlation_id"] == "correlation-002"
    assert published_event["causation_id"] == "payment-failed-event-001"

    assert published_event["payload"]["order_id"] == 2
    assert published_event["payload"]["product_name"] == "Laptop"
    assert published_event["payload"]["quantity"] == 6
    assert published_event["payload"]["status"] == "RELEASED"