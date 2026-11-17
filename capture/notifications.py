from dataclasses import dataclass
from capture.positions import validate_sequence
from capture.values import decode_source_items, source_from_observation
from capture.port import PublishEvent

INITIAL_CONFIG_ID = 'initial'


@dataclass(frozen=True)
class SourceNotification:
    """A data notification validated only against mapping-independent rules."""
    sequence: int
    config_id: str
    items: tuple


@dataclass(frozen=True)
class DataNotification:
    sequence: int
    observations: tuple
    config_id: str = INITIAL_CONFIG_ID

    @property
    def source_items(self):
        return tuple(source_from_observation(item) for item in self.observations)


@dataclass(frozen=True)
class KeepAlive:
    next_sequence: int


def event_binding(event):
    config_id = getattr(event, 'config_id', INITIAL_CONFIG_ID)
    if not isinstance(config_id, str) or not config_id:
        raise ValueError('config_id must be a nonempty string')
    return config_id


def decode_versioned_publish(event, versions=None):
    """Validate a wire Publish event without interpreting it against a mapping."""
    if not isinstance(event, PublishEvent):
        raise ValueError('expected typed Publish event')
    sequence = validate_sequence(event.sequence)
    config_id = event_binding(event)
    if event.kind == 'keepalive':
        if config_id != INITIAL_CONFIG_ID:
            raise ValueError('keepalive cannot carry a configuration binding')
        if event.items:
            raise ValueError('keepalive cannot carry observations')
        return KeepAlive(sequence)
    if event.kind != 'data':
        raise ValueError('unknown Publish event kind')
    return SourceNotification(sequence, config_id,
                              decode_source_items(event.items))


def decode_publish(event, registry):
    """Original public decoder: always bound to the initial configuration."""
    if not isinstance(event, PublishEvent):
        raise ValueError('expected typed Publish event')
    sequence = validate_sequence(event.sequence)
    if event_binding(event) != INITIAL_CONFIG_ID:
        raise ValueError('notification is bound to another configuration version')
    if event.kind == 'keepalive':
        if event.items:
            raise ValueError('keepalive cannot carry observations')
        return KeepAlive(sequence)
    if event.kind != 'data':
        raise ValueError('unknown Publish event kind')
    from capture.values import decode_observation
    observations = tuple(decode_observation(item, registry) for item in event.items)
    return DataNotification(sequence, observations)


def decode_republish(reply, versions=None):
    if reply.status != 'data' or reply.notification is None:
        raise ValueError('Republish reply has no data')
    result = decode_versioned_publish(reply.notification)
    if not isinstance(result, SourceNotification):
        raise ValueError('Republish notification is not data')
    if result.sequence != reply.sequence:
        raise ValueError('Republish notification does not match requested sequence')
    return result
