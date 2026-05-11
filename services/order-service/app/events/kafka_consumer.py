import json

from kafka import KafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS
from app.core.event_factory import create_event
from app.core.logger import log_event
from app.db.session import SessionLocal
from app.models.order import Order, OrderStatus
from app.models.outbox_event import OutboxEvent


TERMINAL_STATUSES = {
    OrderStatus.COMPLETED,
    OrderStatus.CANCELLED,
}


def start_consumer():
    consumer = KafkaConsumer(
        "inventory-events",
        "payment-events",
        "shipping-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="order-service-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        api_version=(2, 8, 0),
    )

    log_event(
        "Order service consumer started",
        topics=[
            "inventory-events",
            "payment-events",
            "shipping-events",
        ],
    )

    for message in consumer:
        event = message.value
        event_type = event.get("event_type")

        if event_type == "InventoryReserved":
            handle_inventory_reserved(event)

        elif event_type == "PaymentCompleted":
            handle_payment_completed(event)

        elif event_type == "ShippingCreated":
            handle_shipping_created(event)

        elif event_type == "InventoryReleased":
            handle_inventory_released(event)


def handle_inventory_reserved(event: dict):
    db = SessionLocal()

    try:
        payload = event.get("payload", {})
        order_id = payload.get("order_id")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order and order.status not in TERMINAL_STATUSES:
            order.status = OrderStatus.INVENTORY_RESERVED
            db.commit()

            log_event(
                "Order inventory reserved",
                event_type=event.get("event_type"),
                correlation_id=event.get("correlation_id"),
                causation_id=event.get("event_id"),
                order_id=order_id,
                status=OrderStatus.INVENTORY_RESERVED,
            )

    except Exception as error:
        db.rollback()

        log_event(
            "Error while marking inventory reserved",
            error=str(error),
            event_type=event.get("event_type"),
            correlation_id=event.get("correlation_id"),
        )

    finally:
        db.close()


def handle_payment_completed(event: dict):
    db = SessionLocal()

    try:
        payload = event.get("payload", {})
        order_id = payload.get("order_id")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order and order.status not in TERMINAL_STATUSES:
            order.status = OrderStatus.PAYMENT_COMPLETED
            db.commit()

            log_event(
                "Order marked as payment completed",
                event_type=event.get("event_type"),
                correlation_id=event.get("correlation_id"),
                causation_id=event.get("event_id"),
                order_id=order_id,
                status=OrderStatus.PAYMENT_COMPLETED,
            )

    except Exception as error:
        db.rollback()

        log_event(
            "Error while marking payment completed",
            error=str(error),
            event_type=event.get("event_type"),
            correlation_id=event.get("correlation_id"),
        )

    finally:
        db.close()


def handle_shipping_created(event: dict):
    db = SessionLocal()

    try:
        payload = event.get("payload", {})
        order_id = payload.get("order_id")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order and order.status not in TERMINAL_STATUSES:
            order.status = OrderStatus.COMPLETED
            db.commit()

            log_event(
                "Order marked as completed after shipping",
                event_type=event.get("event_type"),
                correlation_id=event.get("correlation_id"),
                causation_id=event.get("event_id"),
                order_id=order_id,
                status=OrderStatus.COMPLETED,
            )

    except Exception as error:
        db.rollback()

        log_event(
            "Error while completing order after shipping",
            error=str(error),
            event_type=event.get("event_type"),
            correlation_id=event.get("correlation_id"),
        )

    finally:
        db.close()


def handle_inventory_released(event: dict):
    db = SessionLocal()

    try:
        payload = event.get("payload", {})

        order_id = payload.get("order_id")
        product_name = payload.get("product_name")
        quantity = payload.get("quantity")

        order = db.query(Order).filter(Order.id == order_id).first()

        if order and order.status != OrderStatus.COMPLETED:
            order.status = OrderStatus.CANCELLED

            order_cancelled_event = create_event(
                event_type="OrderCancelled",
                source="order-service",
                payload={
                    "order_id": order_id,
                    "product_name": product_name,
                    "quantity": quantity,
                    "status": OrderStatus.CANCELLED,
                    "reason": (
                        "Order cancelled because payment failed "
                        "and inventory was released."
                    ),
                },
                correlation_id=event.get("correlation_id"),
                causation_id=event.get("event_id"),
            )

            outbox_event = OutboxEvent(
                topic="order-events",
                event_type="OrderCancelled",
                payload=order_cancelled_event,
            )

            db.add(outbox_event)
            db.commit()

            log_event(
                "Order cancelled after compensation flow",
                event_type=event.get("event_type"),
                correlation_id=event.get("correlation_id"),
                causation_id=event.get("event_id"),
                order_id=order_id,
                status=OrderStatus.CANCELLED,
            )

            log_event(
                "OrderCancelled event added to outbox",
                event_type="OrderCancelled",
                correlation_id=event.get("correlation_id"),
                order_id=order_id,
                outbox_topic="order-events",
            )

    except Exception as error:
        db.rollback()

        log_event(
            "Error while cancelling order",
            error=str(error),
            event_type=event.get("event_type"),
            correlation_id=event.get("correlation_id"),
        )

    finally:
        db.close()