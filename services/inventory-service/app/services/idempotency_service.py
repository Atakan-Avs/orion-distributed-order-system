from sqlalchemy.orm import Session

from app.models.processed_event import ProcessedEvent


def is_event_processed(db: Session, event_id: str) -> bool:
    event = db.query(ProcessedEvent).filter_by(event_id=event_id).first()
    return event is not None


def mark_event_processed(db: Session, event_id: str, event_type: str):
    processed_event = ProcessedEvent(
        event_id=event_id,
        event_type=event_type
    )
    db.add(processed_event)
    db.commit()