from dataclasses import dataclass
from capture.positions import PositionWindow


@dataclass(frozen=True)
class FrontierState:
    initial: int
    frontier: int
    high: int

    @property
    def window(self):
        return PositionWindow(self.initial, self.frontier)


def load_frontier(con, subscription_id):
    row = con.execute('SELECT initial,frontier,high FROM subscriptions WHERE id=?',
                      (subscription_id,)).fetchone()
    if row is None:
        raise ValueError('unknown subscription')
    return FrontierState(row['initial'], row['frontier'], row['high'])


def advance_frontier(con, subscription_id):
    """Advance only over admitted data or permanent gaps, never over candidates."""
    state = load_frontier(con, subscription_id)
    frontier = state.frontier
    while frontier < state.high:
        position = frontier + 1
        data = con.execute('SELECT 1 FROM notifications WHERE sub=? AND pos=?',
                           (subscription_id, position)).fetchone()
        gap = con.execute('SELECT 1 FROM gaps WHERE sub=? AND pos=?',
                          (subscription_id, position)).fetchone()
        candidate = con.execute('SELECT 1 FROM candidates WHERE sub=? AND pos=?',
                                (subscription_id, position)).fetchone()
        if (data is not None) + (gap is not None) + (candidate is not None) > 1:
            raise ValueError('data, gap and candidate facts conflict')
        if not data and not gap:
            break
        frontier = position
    con.execute('UPDATE subscriptions SET frontier=? WHERE id=?',
                (frontier, subscription_id))
    return FrontierState(state.initial, frontier, state.high)
