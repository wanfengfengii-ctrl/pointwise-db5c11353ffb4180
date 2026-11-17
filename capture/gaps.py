from dataclasses import dataclass
from capture.positions import PositionWindow


@dataclass(frozen=True)
class GapEvidence:
    source_id: str
    subscription_id: str
    offset: int
    sequence: int
    reason: str = 'MessageNotAvailable'


def validate_unavailable_reply(handle, request, reply):
    if reply.status != 'unavailable' or reply.notification is not None:
        raise ValueError('explicit MessageNotAvailable required')
    if (reply.source_id != handle.spec.source_id or
            reply.subscription_id != handle.subscription_id or
            reply.sequence != request.sequence):
        raise ValueError('unavailable reply identity mismatch')
    return GapEvidence(reply.source_id, reply.subscription_id, request.offset, request.sequence)


def record_gap_evidence(con, handle, evidence):
    if (evidence.source_id != handle.spec.source_id or
            evidence.subscription_id != handle.subscription_id or
            evidence.reason != 'MessageNotAvailable'):
        raise ValueError('invalid gap evidence')
    sequence = PositionWindow(handle.spec.initial_sequence, 0).sequence_at(evidence.offset)
    if evidence.sequence != sequence:
        raise ValueError('gap sequence does not match position')
    row = con.execute('SELECT seq FROM pending WHERE sub=? AND pos=?',
                      (handle.subscription_id, evidence.offset)).fetchone()
    if row is None or row['seq'] != evidence.sequence:
        raise ValueError('unavailable reply is not for a pending request')
    con.execute('INSERT INTO gaps(sub,pos,seq,source,reason) VALUES(?,?,?,?,?)',
                (handle.subscription_id, evidence.offset, evidence.sequence,
                 evidence.source_id, evidence.reason))
