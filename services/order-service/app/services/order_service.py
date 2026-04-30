from sqlalchemy.orm import Session

from app.models.order import Order
from app.events.kafka_producer import publish_event


def create_order(db: Session, product_name: str, quantity: int):
    order = Order(product_name=product_name, quantity=quantity)
    db.add(order)
    db.commit()
    db.refresh(order)

    event = {
        "event_type": "OrderCreated",
        "order_id": order.id,
        "product_name": order.product_name,
        "quantity": order.quantity,
        "status": order.status,
    }

    publish_event("order-events", event)

    return order


def get_orders(db: Session):
    return db.query(Order).all()