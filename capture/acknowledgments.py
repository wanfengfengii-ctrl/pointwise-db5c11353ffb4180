from dataclasses import dataclass
from capture.port import PortDisconnected


@dataclass(frozen=True)
class AcknowledgmentWork:
    offset: int
    sequence: int
    state: str
    attempts: int
    receipt: str | None


def queue_data_ack(con, subscription_id, position):
    row = con.execute('SELECT seq FROM notifications WHERE sub=? AND pos=?',
                      (subscription_id, position.offset)).fetchone()
    if row is None or row['seq'] != position.sequence:
        raise ValueError('acknowledgment requires stored data')
    con.execute('INSERT OR IGNORE INTO acknowledgments(sub,pos,seq,state) VALUES(?,?,?,?)',
                (subscription_id, position.offset, position.sequence, 'pending'))


def list_acknowledgments(handle, unfinished_only=False):
    query = 'SELECT pos,seq,state,attempts,receipt FROM acknowledgments WHERE sub=?'
    if unfinished_only:
        query += " AND state='pending'"
    query += ' ORDER BY pos'
    rows = handle.db.connection.execute(query, (handle.subscription_id,)).fetchall()
    return tuple(AcknowledgmentWork(row['pos'], row['seq'], row['state'],
                                    row['attempts'], row['receipt']) for row in rows)


def flush_acknowledgments(handle, port):
    handle.require_source(port)
    completed = 0
    for work in list_acknowledgments(handle, True):
        with handle.db.transaction() as con:
            con.execute('UPDATE acknowledgments SET attempts=attempts+1 WHERE sub=? AND pos=?',
                        (handle.subscription_id, work.offset))
        try:
            reply = port.acknowledge(work.sequence, position=work.offset)
        except PortDisconnected:
            raise
        if (reply.source_id != handle.spec.source_id or
                reply.subscription_id != handle.subscription_id or
                reply.sequence != work.sequence):
            raise ValueError('acknowledgment reply identity mismatch')
        if reply.status not in ('success', 'already', 'temporary'):
            raise ValueError('unknown acknowledgment status')
        if reply.status in ('success', 'already'):
            with handle.db.transaction() as con:
                con.execute("UPDATE acknowledgments SET state='done',receipt=? WHERE sub=? AND pos=?",
                            (reply.status, handle.subscription_id, work.offset))
            completed += 1
    return completed
