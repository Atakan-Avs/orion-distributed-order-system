from app.events.kafka_producer import publish_event
from app.core.event_factory import create_event


def reserve_inventory(event: dict):
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

    publish_event("inventory-events", inventory_event)

    print(f"[Inventory Service] InventoryReserved event published: {inventory_event}")


def release_inventory(event: dict):
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

    publish_event("inventory-events", release_event)

    print(f"[Inventory Service] InventoryReleased event published: {release_event}")