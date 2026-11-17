from dataclasses import dataclass

MAX_SEQUENCE = 4294967295
HALF_WINDOW = MAX_SEQUENCE // 2


def validate_sequence(value):
    if type(value) is not int or not 1 <= value <= MAX_SEQUENCE:
        raise ValueError('sequence must be a nonzero uint32')
    return value


def forward_distance(a, b):
    return (validate_sequence(b) - validate_sequence(a)) % MAX_SEQUENCE


def next_sequence(value):
    return validate_sequence(value) % MAX_SEQUENCE + 1


@dataclass(frozen=True, order=True)
class SourcePosition:
    offset: int
    sequence: int


@dataclass(frozen=True)
class PositionWindow:
    initial: int
    frontier: int

    def sequence_at(self, offset):
        return (validate_sequence(self.initial) - 1 + offset) % MAX_SEQUENCE + 1

    def resolve(self, sequence):
        distance = forward_distance(self.sequence_at(self.frontier), sequence)
        delta = distance if distance <= HALF_WINDOW else distance - MAX_SEQUENCE
        offset = self.frontier + delta
        if offset <= 0:
            raise ValueError('position precedes this capture origin')
        return SourcePosition(offset, sequence)
