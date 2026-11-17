from dataclasses import dataclass
from capture.positions import validate_sequence
from capture.values import decode_observation
from capture.port import PublishEvent


@dataclass(frozen=True)
class DataNotification:
    sequence: int
    observations: tuple


@dataclass(frozen=True)
class KeepAlive:
    next_sequence: int


def decode_publish(event, registry):
    if not isinstance(event, PublishEvent):
        raise ValueError('expected typed Publish event')
    sequence = validate_sequence(event.sequence)
    if event.kind == 'keepalive':
        if event.items:
            raise ValueError('keepalive cannot carry observations')
        return KeepAlive(sequence)
    if event.kind != 'data':
        raise ValueError('unknown Publish event kind')
    observations = tuple(decode_observation(item, registry) for item in event.items)
    return DataNotification(sequence, observations)


def decode_republish(reply, registry):
    if reply.status != 'data' or reply.notification is None:
        raise ValueError('Republish reply has no data')
    result = decode_publish(reply.notification, registry)
    if not isinstance(result, DataNotification) or result.sequence != reply.sequence:
        raise ValueError('Republish notification does not match requested sequence')
    return result
