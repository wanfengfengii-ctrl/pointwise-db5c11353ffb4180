from dataclasses import dataclass
from capture.config_versions import load_version_items
from capture.frontier import load_frontier, advance_frontier
from capture.identity import (NotificationConflict, canonical_observations,
                              same_source_items)
from capture.notifications import DataNotification, SourceNotification
from capture.pending import register_missing, resolve_pending
from capture.gaps import record_gap_evidence
from capture.acknowledgments import queue_data_ack
from capture.positions import validate_sequence
from capture.values import (decode_observations, decode_source_items,
                            encode_source_items, source_from_observation)


@dataclass(frozen=True)
class AdmissionResult:
    offset: int
    sequence: int
    duplicate: bool
    frontier: int
    state: str = 'data'


def _incoming(notification):
    """Return (config_id, ordered validated source items, typed observations)."""
    if isinstance(notification, SourceNotification):
        validate_sequence(notification.sequence)
        if not isinstance(notification.config_id, str) or not notification.config_id:
            raise ValueError('config_id must be a nonempty string')
        return (notification.config_id,
                decode_source_items(notification.items), None)
    if isinstance(notification, DataNotification):
        validate_sequence(notification.sequence)
        if not isinstance(notification.config_id, str) or not notification.config_id:
            raise ValueError('config_id must be a nonempty string')
        raw = [source_from_observation(item) for item in notification.observations]
        return notification.config_id, decode_source_items(raw), notification.observations
    raise ValueError('only data notifications may be accepted')


def accept_notification(handle, notification, expected=None):
    """Original public typed admission; initial-version usage is unchanged."""
    if not isinstance(notification, DataNotification):
        raise ValueError('only data notifications may be accepted')
    return _admit(handle, notification, expected)


def accept_source(handle, notification, expected=None):
    if not isinstance(notification, SourceNotification):
        raise ValueError('expected a source notification')
    return _admit(handle, notification, expected)


def _admit(handle, notification, expected):
    config_id, source_items, typed = _incoming(notification)
    sub = handle.subscription_id
    with handle.db.transaction() as con:
        state = load_frontier(con, sub)
        position = state.window.resolve(notification.sequence)
        if expected is not None and (position.offset != expected.offset or
                                     position.sequence != expected.sequence):
            raise ValueError('Republish position mismatch')

        stored = con.execute(
            'SELECT payload,config_id FROM notifications WHERE sub=? AND pos=?',
            (sub, position.offset)).fetchone()
        candidate = con.execute(
            'SELECT payload,config_id,state FROM candidates WHERE sub=? AND pos=?',
            (sub, position.offset)).fetchone()
        if stored is not None and candidate is not None:
            raise ValueError('stored data and candidate facts overlap')
        if stored is not None or candidate is not None:
            row = stored if stored is not None else candidate
            if (row['config_id'] != config_id or
                    not same_source_items(row['payload'], source_items)):
                raise NotificationConflict('same position has different notification content')
            return AdmissionResult(position.offset, position.sequence, True,
                                   state.frontier,
                                   'data' if stored is not None else row['state'])
        if con.execute('SELECT 1 FROM gaps WHERE sub=? AND pos=?',
                       (sub, position.offset)).fetchone():
            raise NotificationConflict('position already has permanent gap evidence')
        if position.offset <= state.frontier:
            raise ValueError('settled position has no corresponding stored notification')

        version_items = load_version_items(con, sub, config_id)
        if version_items is None:
            con.execute('INSERT INTO candidates(sub,pos,seq,config_id,state,payload) '
                        'VALUES(?,?,?,?,?,?)',
                        (sub, position.offset, position.sequence, config_id,
                         'waiting', encode_source_items(source_items)))
            _settle_new_position(con, sub, state, position)
            frontier = advance_frontier(con, sub)
            return AdmissionResult(position.offset, position.sequence, False,
                                   frontier.frontier, 'waiting')

        observations = decode_observations(source_items, version_items)
        if typed is not None and tuple(typed) != observations:
            raise ValueError('observation metadata differs from frozen configuration')
        con.execute('INSERT INTO notifications(sub,pos,seq,payload,config_id) '
                    'VALUES(?,?,?,?,?)',
                    (sub, position.offset, position.sequence,
                     canonical_observations(observations), config_id))
        _settle_new_position(con, sub, state, position)
        queue_data_ack(con, sub, position)
        frontier = advance_frontier(con, sub)
        return AdmissionResult(position.offset, position.sequence, False,
                               frontier.frontier, 'data')


def _settle_new_position(con, sub, state, position):
    register_missing(con, sub, state, position.offset)
    resolve_pending(con, sub, position.offset)
    con.execute('UPDATE subscriptions SET high=MAX(high,?) WHERE id=?',
                (position.offset, sub))


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
        if con.execute('SELECT 1 FROM candidates WHERE sub=? AND pos=?',
                       (sub, evidence.offset)).fetchone():
            raise NotificationConflict('cannot replace a configuration candidate with a gap')
        old = con.execute('SELECT seq,source,reason FROM gaps WHERE sub=? AND pos=?',
                          (sub, evidence.offset)).fetchone()
        if old:
            if (old['seq'], old['source'], old['reason']) != (
                    evidence.sequence, evidence.source_id, evidence.reason):
                raise NotificationConflict('conflicting gap evidence')
            state = load_frontier(con, sub)
            return AdmissionResult(evidence.offset, evidence.sequence, True,
                                   state.frontier, 'gap')
        record_gap_evidence(con, handle, evidence)
        resolve_pending(con, sub, evidence.offset)
        frontier = advance_frontier(con, sub)
        return AdmissionResult(evidence.offset, evidence.sequence, False,
                               frontier.frontier, 'gap')
