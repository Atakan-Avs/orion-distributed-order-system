from sqlalchemy.orm import Session

from app.models.outbox_event import OutboxEvent


def retry_failed_outbox_event(db: Session, event_id: int):
    event = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()

    if event is None:
        return None

    if event.processed:
        return event

    event.failed = False
    event.retry_count = 0
    event.last_error = None
    event.failed_at = None

    db.commit()
    db.refresh(event)
    
    return event


def get_failed_outbox_events(db: Session):
    return (
        db.query(OutboxEvent)
        .filter(OutboxEvent.failed == True)
        .order_by(OutboxEvent.failed_at.desc())
        .all()
    )