from dataclasses import dataclass
import json
from capture.config_versions import (INITIAL_CONFIG_ID, canonical_items,
                                     stored_versions)
from capture.configuration import validate_spec, canonical_spec
from capture.items import ItemRegistry
from capture.positions import PositionWindow
from capture.values import decode_observation
from capture.pending import PendingPosition


@dataclass(frozen=True)
class StoredNotification:
    offset: int
    sequence: int
    observations: tuple
    config_id: str = INITIAL_CONFIG_ID


@dataclass(frozen=True)
class StoredCandidate:
    offset: int
    sequence: int
    config_id: str
    state: str


@dataclass(frozen=True)
class StoredGap:
    offset: int
    sequence: int
    source_id: str
    reason: str


@dataclass(frozen=True)
class StoredAcknowledgment:
    offset: int
    sequence: int
    state: str
    attempts: int
    receipt: str | None


@dataclass(frozen=True)
class CaptureSnapshot:
    spec: object
    frontier: int
    high: int
    notifications: tuple
    gaps: tuple
    pending: tuple
    acknowledgments: tuple
    candidates: tuple = ()
    versions: tuple = ()

    def items_for(self, config_id):
        for identifier, items in self.versions:
            if identifier == config_id:
                return items
        raise ValueError('unknown configuration version')


def load_snapshot(handle):
    sub = handle.subscription_id
    with handle.db.snapshot() as con:
        row = con.execute('SELECT * FROM subscriptions WHERE id=?', (sub,)).fetchone()
        if row is None:
            raise ValueError('unknown subscription')
        spec = validate_spec(json.loads(row['config']))
        if (canonical_spec(spec) != canonical_spec(handle.spec) or
                row['source'] != spec.source_id or row['initial'] != spec.initial_sequence):
            raise ValueError('subscription metadata inconsistent')
        versions = stored_versions(con, sub)
        if not versions:
            legacy_candidate = con.execute(
                'SELECT 1 FROM candidates WHERE sub=? LIMIT 1', (sub,)).fetchone()
            if legacy_candidate is not None:
                raise ValueError('initial configuration version is missing')
            # Legacy database written before configuration versions existed.
            versions = ((INITIAL_CONFIG_ID, spec.items),)
        by_id = dict(versions)
        if INITIAL_CONFIG_ID not in by_id:
            raise ValueError('initial configuration version is missing')
        if by_id[INITIAL_CONFIG_ID] != spec.items:
            raise ValueError('initial configuration version is inconsistent')
        data = []
        for value in con.execute('SELECT * FROM notifications WHERE sub=? ORDER BY pos', (sub,)):
            config_id = value['config_id']
            if config_id not in by_id:
                raise ValueError('notification binds an unknown configuration version')
            registry = ItemRegistry(by_id[config_id])
            observations = []
            for item in json.loads(value['payload']):
                decoded = decode_observation(
                    {key: item[key] for key in ('item_id', 'value', 'quality', 'timestamp')},
                    registry)
                if (decoded.device, decoded.channel, decoded.unit) != (
                        item['device'], item['channel'], item['unit']):
                    raise ValueError('stored observation metadata inconsistent')
                observations.append(decoded)
            data.append(StoredNotification(value['pos'], value['seq'],
                                           tuple(observations), config_id))
        candidates = tuple(
            StoredCandidate(r['pos'], r['seq'], r['config_id'], r['state'])
            for r in con.execute('SELECT * FROM candidates WHERE sub=? ORDER BY pos', (sub,)))
        gaps = tuple(StoredGap(r['pos'], r['seq'], r['source'], r['reason'])
                     for r in con.execute('SELECT * FROM gaps WHERE sub=? ORDER BY pos', (sub,)))
        pending = tuple(PendingPosition(r['pos'], r['seq'], r['attempts'])
                        for r in con.execute('SELECT * FROM pending WHERE sub=? ORDER BY pos',
                                             (sub,)))
        acks = tuple(StoredAcknowledgment(r['pos'], r['seq'], r['state'], r['attempts'],
                                          r['receipt'])
                     for r in con.execute(
                         'SELECT * FROM acknowledgments WHERE sub=? ORDER BY pos', (sub,)))
        snapshot = CaptureSnapshot(spec, row['frontier'], row['high'], tuple(data), gaps,
                                   pending, acks, candidates, tuple(versions))
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot):
    if not 0 <= snapshot.frontier <= snapshot.high:
        raise ValueError('invalid frontier bounds')
    window = PositionWindow(snapshot.spec.initial_sequence, snapshot.frontier)
    sets = []
    for collection in (snapshot.notifications, snapshot.gaps, snapshot.pending,
                       snapshot.candidates):
        positions = set()
        for row in collection:
            if (not 1 <= row.offset <= snapshot.high or
                    row.sequence != window.sequence_at(row.offset) or row.offset in positions):
                raise ValueError('invalid stored position')
            positions.add(row.offset)
        sets.append(positions)
    data, gaps, pending, candidates = sets
    if (data & gaps or data & pending or gaps & pending or
            data & candidates or gaps & candidates or pending & candidates):
        raise ValueError('data, gap, pending and candidate facts overlap')
    if len(data | gaps | pending | candidates) != snapshot.high:
        raise ValueError('known position coverage has a hole')
    if max(data | gaps | candidates, default=0) != snapshot.high:
        raise ValueError('known horizon lacks settled source evidence')
    blocked = min(pending | candidates, default=None)
    expected_frontier = snapshot.high if blocked is None else blocked - 1
    if snapshot.frontier != expected_frontier:
        raise ValueError('frontier is not the contiguous settled prefix')
    for gap in snapshot.gaps:
        if gap.source_id != snapshot.spec.source_id or gap.reason != 'MessageNotAvailable':
            raise ValueError('invalid gap evidence')
    version_ids = {}
    for config_id, items in snapshot.versions:
        if config_id in version_ids:
            raise ValueError('duplicate configuration version')
        version_ids[config_id] = items
    if INITIAL_CONFIG_ID not in version_ids:
        raise ValueError('initial configuration version is missing')
    if canonical_items(version_ids[INITIAL_CONFIG_ID]) != canonical_items(snapshot.spec.items):
        raise ValueError('initial configuration version is inconsistent')
    for notification in snapshot.notifications:
        if notification.config_id not in version_ids:
            raise ValueError('notification binds an unknown configuration version')
        registry = ItemRegistry(version_ids[notification.config_id])
        for observation in notification.observations:
            item = registry.by_id.get(observation.item_id)
            if item is None or (item.device, item.channel, item.unit) != (
                    observation.device, observation.channel, observation.unit):
                raise ValueError('observation is not interpreted by its bound version')
    for candidate in snapshot.candidates:
        if candidate.state not in ('waiting', 'invalid'):
            raise ValueError('invalid configuration candidate')
        if candidate.state == 'waiting' and candidate.config_id in version_ids:
            raise ValueError('waiting candidate already has a registered version')
    acks = {}
    for ack in snapshot.acknowledgments:
        if (ack.offset in acks or ack.sequence != window.sequence_at(ack.offset) or
                ack.attempts < 0 or ack.state not in ('pending', 'done')):
            raise ValueError('invalid acknowledgment')
        if ((ack.state == 'pending' and ack.receipt is not None) or
                (ack.state == 'done' and ack.receipt not in ('success', 'already'))):
            raise ValueError('acknowledgment receipt inconsistent')
        acks[ack.offset] = ack
    if set(acks) != data:
        raise ValueError('durable data and acknowledgment eligibility differ')
    if any(row.attempts < 0 for row in snapshot.pending):
        raise ValueError('negative repair attempts')
    return snapshot
