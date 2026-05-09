from app.core.event_factory import create_event
from app.models.outbox_event import OutboxEvent


def reserve_inventory(db, event: dict):
    print(f"[Inventory Service] OrderCreated event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")
    product_name = payload.get("product_name")
    quantity = payload.get("quantity")

    inventory_event = create_event(
        event_type="InventoryReserved",
        source="inventory-service",
        payload={
            "order_id": order_id,
            "product_name": product_name,
            "quantity": quantity,
            "status": "RESERVED",
        },
        correlation_id=event.get("correlation_id"),
        causation_id=event.get("event_id"),
    )

    outbox_event = OutboxEvent(
        topic="inventory-events",
        event_type="InventoryReserved",
        payload=inventory_event,
    )

    db.add(outbox_event)

    print(
        f"[Inventory Service] InventoryReserved added to outbox: "
        f"{inventory_event}"
    )


def release_inventory(db, event: dict):
    print(f"[Inventory Service] PaymentFailed event received: {event}")

    payload = event.get("payload", {})

    order_id = payload.get("order_id")
    product_name = payload.get("product_name")
    quantity = payload.get("quantity")

    release_event = create_event(
        event_type="InventoryReleased",
        source="inventory-service",
        payload={
            "order_id": order_id,
            "product_name": product_name,
            "quantity": quantity,
            "status": "RELEASED",
        },
        correlation_id=event.get("correlation_id"),
        causation_id=event.get("event_id"),
    )

    outbox_event = OutboxEvent(
        topic="inventory-events",
        event_type="InventoryReleased",
        payload=release_event,
    )

    db.add(outbox_event)

    print(
        f"[Inventory Service] InventoryReleased added to outbox: "
        f"{release_event}"
    )