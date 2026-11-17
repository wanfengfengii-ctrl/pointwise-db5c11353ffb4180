from collections import deque
from copy import deepcopy
from dataclasses import dataclass


class PortDisconnected(ConnectionError):
    pass


@dataclass(frozen=True)
class PublishEvent:
    kind: str
    sequence: int
    items: tuple = ()
    available: tuple = ()


@dataclass(frozen=True)
class RepublishReply:
    status: str
    sequence: int
    source_id: str
    subscription_id: str
    notification: PublishEvent | None = None


@dataclass(frozen=True)
class AcknowledgmentReply:
    status: str
    sequence: int
    source_id: str
    subscription_id: str


class DeterministicPort:
    def __init__(self, source_id, subscription_id, events=(), republish=None, acknowledgments=None):
        self.source_id = source_id
        self.subscription_id = subscription_id
        self.events = deque(deepcopy(list(events)))
        self.republish_scripts = {
            int(seq): deque(deepcopy(list(replies)))
            for seq, replies in (republish or {}).items()
        }
        self.ack_scripts = {
            int(seq): deque(replies) for seq, replies in (acknowledgments or {}).items()
        }
        self.retransmissions = {}
        self.confirmed = set()
        self.ack_calls = []
        self.republish_calls = []
        self.publish_calls = 0
        self.before_remote = None

    def _boundary(self, operation, sequence):
        if self.before_remote:
            self.before_remote(operation, sequence)

    def publish(self):
        self._boundary('publish', None)
        self.publish_calls += 1
        if not self.events:
            return None
        value = self.events.popleft()
        if isinstance(value, dict) and value.get('kind') == 'disconnect':
            raise PortDisconnected('scripted Publish disconnect')
        event = self._event(value)
        if event.kind == 'data':
            self.retransmissions[event.sequence] = deepcopy(event)
        return event

    @staticmethod
    def _event(value):
        if isinstance(value, PublishEvent):
            return deepcopy(value)
        if not isinstance(value, dict):
            raise ValueError('Publish script must be an object')
        if set(value) - {'kind', 'sequence', 'items', 'available'}:
            raise ValueError('unknown Publish fields')
        return PublishEvent(value.get('kind'), value.get('sequence'),
                            tuple(deepcopy(value.get('items', ()))),
                            tuple(value.get('available', ())))

    def republish(self, sequence):
        self._boundary('republish', sequence)
        self.republish_calls.append(sequence)
        script = self.republish_scripts.get(sequence)
        if script:
            value = script.popleft()
            status = value['status'] if isinstance(value, dict) else value
            if status == 'disconnect':
                raise PortDisconnected('scripted Republish disconnect')
            event = None
            if status == 'data':
                event = self._event(value['notification'])
                self.retransmissions[event.sequence] = deepcopy(event)
            return RepublishReply(status, sequence, self.source_id, self.subscription_id, event)
        event = self.retransmissions.get(sequence)
        status = 'data' if event is not None else 'temporary'
        return RepublishReply(status, sequence, self.source_id, self.subscription_id,
                              deepcopy(event))

    def acknowledge(self, sequence, position=None):
        self._boundary('acknowledge', sequence)
        key = (position, sequence)
        script = self.ack_scripts.get(sequence)
        status = script.popleft() if script else ('already' if key in self.confirmed else 'success')
        accepted = status in ('success', 'already', 'lost')
        self.ack_calls.append({'sequence': sequence, 'position': position,
                               'status': status, 'accepted': accepted})
        if accepted:
            self.confirmed.add(key)
            self.retransmissions.pop(sequence, None)
        if status == 'lost':
            raise PortDisconnected('acknowledgment accepted; reply lost')
        if status == 'disconnect':
            raise PortDisconnected('acknowledgment disconnected before acceptance')
        return AcknowledgmentReply(status, sequence, self.source_id, self.subscription_id)
