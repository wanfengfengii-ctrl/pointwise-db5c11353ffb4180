from dataclasses import asdict, dataclass
from capture.projection import load_snapshot
from capture.items import ItemRegistry


@dataclass(frozen=True)
class ReadRecord:
    kind: str
    position: int
    sequence: int
    observations: tuple = ()
    reason: str | None = None

    def to_dict(self):
        result = {'kind': self.kind, 'position': self.position, 'sequence': self.sequence}
        if self.kind == 'data':
            result['observations'] = [asdict(item) for item in self.observations]
        else:
            result['reason'] = self.reason
        return result


def select_readings(snapshot, device=None):
    if device is not None:
        ItemRegistry(snapshot.spec.items).require_device(device)
    records = []
    for notification in snapshot.notifications:
        if notification.offset > snapshot.frontier:
            continue
        observations = tuple(item for item in notification.observations
                             if device is None or item.device == device)
        if device is None or observations:
            records.append(ReadRecord('data', notification.offset, notification.sequence,
                                      observations))
    for gap in snapshot.gaps:
        if gap.offset <= snapshot.frontier:
            records.append(ReadRecord('gap', gap.offset, gap.sequence, reason=gap.reason))
    return tuple(sorted(records, key=lambda record: record.position))


def read(handle, device=None):
    return select_readings(load_snapshot(handle), device)
