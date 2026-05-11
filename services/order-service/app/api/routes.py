from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.schemas.order import OrderCreate, OrderResponse
from app.services.order_service import create_order, get_orders
from app.services.outbox_service import (
    get_failed_outbox_events,
    retry_failed_outbox_event,
)

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/orders", response_model=OrderResponse)
def create_order_route(order: OrderCreate, db: Session = Depends(get_db)):
    return create_order(db, order.product_name, order.quantity)


@router.get("/orders", response_model=list[OrderResponse])
def list_orders(db: Session = Depends(get_db)):
    return get_orders(db)


@router.get("/admin/outbox/failed")
def list_failed_outbox_events(db: Session = Depends(get_db)):
    events = get_failed_outbox_events(db)

    return [
        {
            "id": event.id,
            "topic": event.topic,
            "event_type": event.event_type,
            "processed": event.processed,
            "failed": event.failed,
            "retry_count": event.retry_count,
            "last_error": event.last_error,
            "created_at": event.created_at,
            "failed_at": event.failed_at,
        }
        for event in events
    ]


@router.post("/admin/outbox/{event_id}/retry")
def retry_outbox_event(event_id: int, db: Session = Depends(get_db)):
    event = retry_failed_outbox_event(db, event_id)

    if event is None:
        raise HTTPException(status_code=404, detail="Outbox event not found")

    return {
        "message": "Outbox event retry scheduled",
        "event_id": event.id,
        "processed": event.processed,
        "failed": event.failed,
        "retry_count": event.retry_count,
        "last_error": event.last_error,
    }