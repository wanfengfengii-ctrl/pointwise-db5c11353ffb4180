from dataclasses import dataclass
from capture.positions import validate_sequence


@dataclass(frozen=True)
class AvailabilityHint:
    sequences: tuple = ()


def decode_availability(event):
    values = tuple(validate_sequence(seq) for seq in event.available)
    if len(set(values)) != len(values):
        raise ValueError('duplicate available sequence')
    return AvailabilityHint(values)


def prioritize_pending(pending, hint=None):
    available = set(hint.sequences) if hint is not None else set()
    return tuple(sorted(pending, key=lambda work: (work.sequence not in available, work.offset)))
