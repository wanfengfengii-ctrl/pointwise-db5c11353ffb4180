import argparse
import json
from pathlib import Path
import tempfile
from capture.storage import CaptureDatabase
from capture.subscription import configure
from capture.port import DeterministicPort, PortDisconnected
from capture.receiver import pump
from capture.recovery import resume
from capture.republish import repair_pending
from capture.acknowledgments import flush_acknowledgments
from capture.export import export_snapshot


def run_scenario(scenario, db_path):
    port = DeterministicPort(
        scenario['spec']['source_id'], scenario['spec']['subscription_id'],
        scenario.get('events', []), scenario.get('republish'), scenario.get('acknowledgments'))
    db = CaptureDatabase(db_path)
    checkpoints = []
    try:
        handle = configure(db, scenario['spec'])
        for action in scenario.get('actions', []):
            operation = action['op']
            if operation == 'pump':
                try:
                    pump(handle, port, action['limit'])
                except PortDisconnected:
                    if not action.get('expect_disconnect', False):
                        raise
                else:
                    if action.get('expect_disconnect', False):
                        raise AssertionError('expected a real source disconnect')
            elif operation == 'repair':
                repair_pending(handle, port)
                flush_acknowledgments(handle, port)
            elif operation == 'reopen':
                db.close()
                db = CaptureDatabase(db_path)
                handle = resume(db, scenario['spec']['subscription_id'], port)
            elif operation == 'configure':
                handle = configure(db, scenario['spec'])
            else:
                raise ValueError('unknown scenario action')
            checkpoints.append(json.loads(export_snapshot(handle)))
        return {'report': json.loads(export_snapshot(handle)),
                'ack_calls': port.ack_calls, 'republish_calls': port.republish_calls,
                'checkpoints': checkpoints}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Deterministic vibration capture')
    parser.add_argument('scenarios', nargs='+')
    parser.add_argument('--expected', required=True)
    args = parser.parse_args(argv)
    expected = json.loads(Path(args.expected).read_text(encoding='utf-8'))
    for path in args.scenarios:
        scenario = json.loads(Path(path).read_text(encoding='utf-8'))
        with tempfile.TemporaryDirectory(prefix='capture-demo-') as directory:
            result = run_scenario(scenario, Path(directory) / 'capture.sqlite')
        report = result['report']
        actual = [[row['kind'], row['sequence']] for row in report['records']]
        downstream = {'data_notifications': report['coverage']['data_notifications'],
                      'visible_gaps': report['coverage']['visible_gaps'],
                      'pending': report['coverage']['pending'],
                      'state': report['coverage']['state']}
        oracle = expected[scenario['case_id']]
        if actual != oracle['records'] or downstream != oracle['coverage']:
            raise AssertionError({'case_id': scenario['case_id'], 'actual': result})
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': scenario['case_id'], 'input': scenario,
            'expected': oracle['records'], 'actual': actual,
            'downstream_expected': oracle['coverage'], 'downstream_actual': downstream,
            'code_paths': ['capture/receiver.py', 'capture/republish.py', 'capture/archive.py',
                           'capture/acknowledgments.py', 'capture/recovery.py', 'capture/export.py'],
            'oracle': 'Literal source-position ledger in samples/expected_results.json',
        }, ensure_ascii=False, separators=(',', ':')))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
