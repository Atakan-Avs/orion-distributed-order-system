from sqlalchemy.orm import Session

from app.core.event_factory import create_event
from app.models.order import Order
from app.models.outbox_event import OutboxEvent


def create_order(db: Session, product_name: str, quantity: int):
    order = Order(product_name=product_name, quantity=quantity)

    db.add(order)
    db.flush()

    event = create_event(
        event_type="OrderCreated",
        source="order-service",
        payload={
            "order_id": order.id,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": order.status,
        },
    )

    outbox_event = OutboxEvent(
        topic="order-events",
        event_type="OrderCreated",
        payload=event,
    )

    db.add(outbox_event)
    db.commit()
    db.refresh(order)

    return order


def get_orders(db: Session):
    return db.query(Order).all()