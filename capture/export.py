import json
from capture.projection import load_snapshot
from capture.readings import select_readings
from capture.completeness import summarize_coverage


def encode_report(snapshot, device=None):
    records = select_readings(snapshot, device)
    coverage = summarize_coverage(snapshot)
    report = {
        'subscription_id': snapshot.spec.subscription_id,
        'source_id': snapshot.spec.source_id,
        'device': device,
        'records': [record.to_dict() for record in records],
        'coverage': coverage.to_dict(),
    }
    return (json.dumps(report, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
            + '\n').encode('utf-8')


def export_snapshot(handle, device=None, snapshot=None):
    selected = load_snapshot(handle) if snapshot is None else snapshot
    if selected.spec != handle.spec:
        raise ValueError('snapshot belongs to another subscription')
    return encode_report(selected, device)
