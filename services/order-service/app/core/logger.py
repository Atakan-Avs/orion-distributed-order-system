import json
import logging
from datetime import UTC, datetime


logger = logging.getLogger("order-service")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def log_event(message: str, **kwargs):
    log_data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "service": "order-service",
        "message": message,
        **kwargs,
    }

    logger.info(json.dumps(log_data, default=str))