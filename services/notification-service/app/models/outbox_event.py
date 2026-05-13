from datetime import UTC, datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, JSON, String, Text

from app.db.database import Base


class OutboxEvent(Base):
    __tablename__ = "notification_outbox_events"

    id = Column(Integer, primary_key=True, index=True)

    topic = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)

    processed = Column(Boolean, default=False, nullable=False)
    failed = Column(Boolean, default=False, nullable=False)

    retry_count = Column(Integer, default=0, nullable=False)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    failed_at = Column(DateTime(timezone=True), nullable=True)