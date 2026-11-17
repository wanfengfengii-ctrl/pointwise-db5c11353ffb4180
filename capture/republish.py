from dataclasses import dataclass
from capture.pending import list_pending
from capture.availability import prioritize_pending
from capture.notifications import decode_republish
from capture.gaps import validate_unavailable_reply
from capture.archive import accept_source, accept_gap


@dataclass(frozen=True)
class RepairResult:
    attempted: int
    data: int
    gaps: int
    temporary: int


def apply_republish_reply(handle, request, reply):
    if (reply.source_id != handle.spec.source_id or
            reply.subscription_id != handle.subscription_id or
            reply.sequence != request.sequence):
        raise ValueError('Republish reply identity mismatch')
    if reply.status == 'data':
        notification = decode_republish(reply)
        accept_source(handle, notification, expected=request)
        return 'data'
    if reply.status == 'unavailable':
        evidence = validate_unavailable_reply(handle, request, reply)
        # A second admission can occur while the remote call is in progress.
        current = {work.offset: work for work in list_pending(handle)}
        if request.offset not in current:
            settled = handle.db.connection.execute(
                'SELECT 1 FROM notifications WHERE sub=? AND pos=?',
                (handle.subscription_id, request.offset)).fetchone()
            candidate = handle.db.connection.execute(
                'SELECT 1 FROM candidates WHERE sub=? AND pos=?',
                (handle.subscription_id, request.offset)).fetchone()
            if settled or candidate:
                raise ValueError('unavailable reply contradicts accepted source evidence')
        accept_gap(handle, evidence)
        return 'gaps'
    if reply.status == 'temporary' and reply.notification is None:
        return 'temporary'
    raise ValueError('invalid Republish status or body')


def repair_pending(handle, port, hint=None):
    handle.require_source(port)
    counts = {'data': 0, 'gaps': 0, 'temporary': 0}
    attempted = 0
    # One bounded pass: a temporary position cannot monopolize this call.
    for request in prioritize_pending(list_pending(handle), hint):
        with handle.db.transaction() as con:
            row = con.execute('SELECT seq FROM pending WHERE sub=? AND pos=?',
                              (handle.subscription_id, request.offset)).fetchone()
            if row is None:
                continue
            con.execute('UPDATE pending SET attempts=attempts+1 WHERE sub=? AND pos=?',
                        (handle.subscription_id, request.offset))
        reply = port.republish(request.sequence)
        counts[apply_republish_reply(handle, request, reply)] += 1
        attempted += 1
    return RepairResult(attempted, **counts)
