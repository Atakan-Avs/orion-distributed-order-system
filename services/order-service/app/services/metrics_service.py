from sqlalchemy.orm import Session

from app.models.order import Order, OrderStatus
from app.models.outbox_event import OutboxEvent


def get_order_metrics(db: Session) -> str:
    orders_created_total = db.query(Order).count()

    saga_completed_total = (
        db.query(Order)
        .filter(Order.status == OrderStatus.COMPLETED)
        .count()
    )

    saga_cancelled_total = (
        db.query(Order)
        .filter(Order.status == OrderStatus.CANCELLED)
        .count()
    )

    outbox_pending_total = (
        db.query(OutboxEvent)
        .filter(OutboxEvent.processed.is_(False))
        .filter(OutboxEvent.failed.is_(False))
        .count()
    )

    outbox_failed_total = (
        db.query(OutboxEvent)
        .filter(OutboxEvent.failed.is_(True))
        .count()
    )

    return "\n".join(
        [
            f"orion_orders_created_total {orders_created_total}",
            f"orion_saga_completed_total {saga_completed_total}",
            f"orion_saga_cancelled_total {saga_cancelled_total}",
            f"orion_outbox_pending_total {outbox_pending_total}",
            f"orion_outbox_failed_total {outbox_failed_total}",
            "",
        ]
    )