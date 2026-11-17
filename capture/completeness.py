from dataclasses import asdict, dataclass
from capture.projection import load_snapshot, validate_snapshot


@dataclass(frozen=True)
class CompletenessResult:
    state: str
    data_notifications: int
    visible_gaps: tuple
    pending: tuple
    blocked_data: tuple
    covered_positions: int
    frontier: int
    known_positions: int

    def to_dict(self):
        result = asdict(self)
        for name in ('visible_gaps', 'pending', 'blocked_data'):
            result[name] = list(result[name])
        return result


def summarize_coverage(snapshot):
    validate_snapshot(snapshot)
    count = sum(row.offset <= snapshot.frontier for row in snapshot.notifications)
    gaps = tuple(row.sequence for row in snapshot.gaps if row.offset <= snapshot.frontier)
    pending = tuple(row.sequence for row in snapshot.pending)
    blocked = tuple(row.sequence for row in snapshot.notifications if row.offset > snapshot.frontier)
    state = 'waiting' if pending else ('results' if snapshot.high else 'empty')
    return CompletenessResult(state, count, gaps, pending, blocked, count + len(gaps),
                              snapshot.frontier, snapshot.high)


def audit(handle):
    return summarize_coverage(load_snapshot(handle))
