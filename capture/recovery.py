from dataclasses import dataclass
from capture.subscription import load_subscription
from capture.pending import list_pending
from capture.acknowledgments import list_acknowledgments, flush_acknowledgments
from capture.republish import repair_pending
from capture.projection import load_snapshot


@dataclass(frozen=True)
class RecoveryPlan:
    frontier: int
    pending: tuple
    blocked: tuple
    acknowledgments: tuple


def load_recovery_plan(handle):
    snapshot = load_snapshot(handle)
    return RecoveryPlan(snapshot.frontier, list_pending(handle),
                        tuple(row.sequence for row in snapshot.notifications
                              if row.offset > snapshot.frontier),
                        list_acknowledgments(handle, True))


def resume(db, subscription_id, port):
    handle = load_subscription(db, subscription_id)
    handle.require_source(port)
    load_recovery_plan(handle)
    repair_pending(handle, port)
    flush_acknowledgments(handle, port)
    return handle
