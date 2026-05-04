from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from app.db.database import Base


class ProcessedEvent(Base):
    __tablename__ = "notification_processed_events"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    event_type = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)