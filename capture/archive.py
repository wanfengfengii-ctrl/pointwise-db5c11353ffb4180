from dataclasses import dataclass
from capture.frontier import load_frontier, advance_frontier
from capture.identity import canonical_payload, same_content, NotificationConflict
from capture.notifications import DataNotification
from capture.pending import register_missing, resolve_pending
from capture.gaps import record_gap_evidence
from capture.acknowledgments import queue_data_ack
from capture.positions import validate_sequence
from capture.values import decode_observation


@dataclass(frozen=True)
class AdmissionResult:
    offset: int
    sequence: int
    duplicate: bool
    frontier: int


def accept_notification(handle, notification, expected=None):
    if not isinstance(notification, DataNotification):
        raise ValueError('only data notifications may be accepted')
    validate_sequence(notification.sequence)
    # Public typed admissions receive the same validation as wire admissions.
    for item in notification.observations:
        rebuilt = decode_observation(
            {'item_id': item.item_id, 'value': item.value, 'quality': item.quality,
             'timestamp': item.timestamp}, handle.registry)
        if rebuilt != item:
            raise ValueError('observation metadata differs from frozen configuration')
    payload = canonical_payload(notification)
    sub = handle.subscription_id
    with handle.db.transaction() as con:
        state = load_frontier(con, sub)
        position = state.window.resolve(notification.sequence)
        if expected is not None and (position.offset != expected.offset or
                                     position.sequence != expected.sequence):
            raise ValueError('Republish position mismatch')
        old = con.execute('SELECT payload FROM notifications WHERE sub=? AND pos=?',
                          (sub, position.offset)).fetchone()
        if old is not None:
            if not same_content(old['payload'], payload):
                raise NotificationConflict('same position has different notification content')
            return AdmissionResult(position.offset, position.sequence, True, state.frontier)
        if con.execute('SELECT 1 FROM gaps WHERE sub=? AND pos=?',
                       (sub, position.offset)).fetchone():
            raise NotificationConflict('position already has permanent gap evidence')
        if position.offset <= state.frontier:
            raise ValueError('settled position has no corresponding stored notification')
        register_missing(con, sub, state, position.offset)
        con.execute('INSERT INTO notifications(sub,pos,seq,payload) VALUES(?,?,?,?)',
                    (sub, position.offset, position.sequence, payload))
        resolve_pending(con, sub, position.offset)
        con.execute('UPDATE subscriptions SET high=MAX(high,?) WHERE id=?',
                    (position.offset, sub))
        queue_data_ack(con, sub, position)
        frontier = advance_frontier(con, sub)
        return AdmissionResult(position.offset, position.sequence, False, frontier.frontier)


def accept_gap(handle, evidence):
    if (evidence.source_id != handle.spec.source_id or
            evidence.subscription_id != handle.subscription_id or
            type(evidence.offset) is not int or evidence.offset <= 0):
        raise ValueError('gap identity mismatch')
    sub = handle.subscription_id
    with handle.db.transaction() as con:
        if con.execute('SELECT 1 FROM notifications WHERE sub=? AND pos=?',
                       (sub, evidence.offset)).fetchone():
            raise NotificationConflict('cannot replace data with a gap')
        old = con.execute('SELECT seq,source,reason FROM gaps WHERE sub=? AND pos=?',
                          (sub, evidence.offset)).fetchone()
        if old:
            if (old['seq'], old['source'], old['reason']) != (
                    evidence.sequence, evidence.source_id, evidence.reason):
                raise NotificationConflict('conflicting gap evidence')
            state = load_frontier(con, sub)
            return AdmissionResult(evidence.offset, evidence.sequence, True, state.frontier)
        record_gap_evidence(con, handle, evidence)
        resolve_pending(con, sub, evidence.offset)
        frontier = advance_frontier(con, sub)
        return AdmissionResult(evidence.offset, evidence.sequence, False, frontier.frontier)
