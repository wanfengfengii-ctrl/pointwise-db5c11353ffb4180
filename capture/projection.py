from dataclasses import dataclass
import json
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
        registry = ItemRegistry(spec.items)
        data = []
        for value in con.execute('SELECT * FROM notifications WHERE sub=? ORDER BY pos', (sub,)):
            observations = []
            for item in json.loads(value['payload']):
                decoded = decode_observation(
                    {key: item[key] for key in ('item_id', 'value', 'quality', 'timestamp')},
                    registry)
                if (decoded.device, decoded.channel, decoded.unit) != (
                        item['device'], item['channel'], item['unit']):
                    raise ValueError('stored observation metadata inconsistent')
                observations.append(decoded)
            data.append(StoredNotification(value['pos'], value['seq'], tuple(observations)))
        gaps = tuple(StoredGap(r['pos'], r['seq'], r['source'], r['reason'])
                     for r in con.execute('SELECT * FROM gaps WHERE sub=? ORDER BY pos', (sub,)))
        pending = tuple(PendingPosition(r['pos'], r['seq'], r['attempts'])
                        for r in con.execute('SELECT * FROM pending WHERE sub=? ORDER BY pos', (sub,)))
        acks = tuple(StoredAcknowledgment(r['pos'], r['seq'], r['state'], r['attempts'], r['receipt'])
                     for r in con.execute(
                         'SELECT * FROM acknowledgments WHERE sub=? ORDER BY pos', (sub,)))
        snapshot = CaptureSnapshot(spec, row['frontier'], row['high'], tuple(data), gaps, pending, acks)
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot):
    if not 0 <= snapshot.frontier <= snapshot.high:
        raise ValueError('invalid frontier bounds')
    window = PositionWindow(snapshot.spec.initial_sequence, snapshot.frontier)
    sets = []
    for collection in (snapshot.notifications, snapshot.gaps, snapshot.pending):
        positions = set()
        for row in collection:
            if (not 1 <= row.offset <= snapshot.high or
                    row.sequence != window.sequence_at(row.offset) or row.offset in positions):
                raise ValueError('invalid stored position')
            positions.add(row.offset)
        sets.append(positions)
    data, gaps, pending = sets
    if data & gaps or data & pending or gaps & pending:
        raise ValueError('data, gap and pending facts overlap')
    if len(data | gaps | pending) != snapshot.high:
        raise ValueError('known position coverage has a hole')
    if max(data | gaps, default=0) != snapshot.high:
        raise ValueError('known horizon lacks settled source evidence')
    expected_frontier = min(pending) - 1 if pending else snapshot.high
    if snapshot.frontier != expected_frontier:
        raise ValueError('frontier is not the contiguous settled prefix')
    for gap in snapshot.gaps:
        if gap.source_id != snapshot.spec.source_id or gap.reason != 'MessageNotAvailable':
            raise ValueError('invalid gap evidence')
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
