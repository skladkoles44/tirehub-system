import logging
logger = logging.getLogger(__name__)

def emit_event(event_type, metadata):
    logger.info(f"EVENT {event_type} {metadata}")
