import uuid
from datetime import UTC, datetime

def create_event(
    event_type: str,
    payload: dict,
    source: str,
    correlation_id: str = None,
    causation_id: str = None,
):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "source": source,
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "causation_id": causation_id,
        "payload": payload,
    }