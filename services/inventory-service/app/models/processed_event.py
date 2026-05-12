from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Integer, String

from app.db.database import Base


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    event_type = Column(String)
    processed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))