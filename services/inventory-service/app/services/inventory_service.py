from app.events.kafka_producer import publish_event


def reserve_inventory(event: dict):
    print(f"[Inventory Service] OrderCreated event received: {event}")

    order_id = event.get("order_id")
    product_name = event.get("product_name")
    quantity = event.get("quantity")

    inventory_event = {
        "event_type": "InventoryReserved",
        "order_id": order_id,
        "product_name": product_name,
        "quantity": quantity,
        "status": "RESERVED",
    }

    publish_event("inventory-events", inventory_event)

    print(f"[Inventory Service] InventoryReserved event published: {inventory_event}")