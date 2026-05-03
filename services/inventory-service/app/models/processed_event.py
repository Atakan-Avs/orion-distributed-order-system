from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

from app.db.database import Base


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True, nullable=False)
    event_type = Column(String)
    processed_at = Column(DateTime, default=datetime.utcnow)