from dataclasses import dataclass
from capture.notifications import decode_publish, KeepAlive
from capture.availability import decode_availability
from capture.archive import accept_notification
from capture.republish import repair_pending
from capture.acknowledgments import flush_acknowledgments
from capture.completeness import audit


@dataclass(frozen=True)
class PumpResult:
    processed: int
    state: object
    last_keepalive: int | None = None


def pump(handle, port, event_limit):
    if type(event_limit) is not int or event_limit < 0:
        raise ValueError('event_limit must be a nonnegative integer')
    handle.require_source(port)
    processed = 0
    last_keepalive = None
    while processed < event_limit:
        event = port.publish()
        if event is None:
            break
        decoded = decode_publish(event, handle.registry)
        hint = decode_availability(event)
        if isinstance(decoded, KeepAlive):
            last_keepalive = decoded.next_sequence
        else:
            accept_notification(handle, decoded)
        processed += 1
        repair_pending(handle, port, hint)
        flush_acknowledgments(handle, port)
    return PumpResult(processed, audit(handle), last_keepalive)
