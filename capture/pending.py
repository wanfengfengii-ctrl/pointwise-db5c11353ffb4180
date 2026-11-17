from dataclasses import dataclass
from capture.positions import PositionWindow


@dataclass(frozen=True)
class PendingPosition:
    offset: int
    sequence: int
    attempts: int = 0


def list_pending(handle):
    rows = handle.db.connection.execute(
        'SELECT pos,seq,attempts FROM pending WHERE sub=? ORDER BY pos',
        (handle.subscription_id,)).fetchall()
    return tuple(PendingPosition(row['pos'], row['seq'], row['attempts']) for row in rows)


def register_missing(con, subscription_id, state, through):
    window = PositionWindow(state.initial, state.frontier)
    for position in range(state.high + 1, through):
        data = con.execute('SELECT 1 FROM notifications WHERE sub=? AND pos=?',
                           (subscription_id, position)).fetchone()
        gap = con.execute('SELECT 1 FROM gaps WHERE sub=? AND pos=?',
                          (subscription_id, position)).fetchone()
        candidate = con.execute('SELECT 1 FROM candidates WHERE sub=? AND pos=?',
                                (subscription_id, position)).fetchone()
        if not data and not gap and not candidate:
            con.execute('INSERT OR IGNORE INTO pending(sub,pos,seq) VALUES(?,?,?)',
                        (subscription_id, position, window.sequence_at(position)))


def resolve_pending(con, subscription_id, offset):
    con.execute('DELETE FROM pending WHERE sub=? AND pos=?', (subscription_id, offset))
