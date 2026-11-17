"""Version-binding source fixtures and literal-ledger runner; no V2 storage logic."""
from copy import deepcopy
from dataclasses import dataclass
import argparse
import json
from pathlib import Path
import tempfile
from capture.port import DeterministicPort, PublishEvent


@dataclass(frozen=True)
class VersionedPublishEvent(PublishEvent):
    config_id: str = 'initial'


class VersionedPort(DeterministicPort):
    @staticmethod
    def _event(value):
        if isinstance(value, VersionedPublishEvent):
            if value.kind != 'data' or not isinstance(value.config_id, str) or not value.config_id:
                raise ValueError('version binding requires data and a nonempty config_id')
            return deepcopy(value)
        if not isinstance(value, dict) or 'config_id' not in value:
            return DeterministicPort._event(value)
        raw = deepcopy(value)
        config_id = raw.pop('config_id')
        if raw.get('kind') != 'data' or not isinstance(config_id, str) or not config_id:
            raise ValueError('version binding requires data and a nonempty config_id')
        event = DeterministicPort._event(raw)
        return VersionedPublishEvent(event.kind, event.sequence, event.items,
                                     event.available, config_id)


def load_case(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def check_port():
    raw = {'kind': 'data', 'sequence': 11, 'config_id': 'B',
           'items': [{'item_id': 'a', 'value': '1.00', 'quality': 'Good'}]}
    port = VersionedPort('cape-source', 'vibration', [raw],
        {10: [{'status': 'data', 'notification': dict(raw, sequence=10)}]})
    event = port.publish()
    if (event.config_id, event.sequence, event.items[0]['value']) != ('B', 11, '1.00'):
        raise AssertionError('Publish binding or first text lost')
    reply = port.republish(10)
    if reply.notification.config_id != 'B' or reply.notification.sequence != 10:
        raise AssertionError('Republish binding lost')
    if port.republish(11).notification != event:
        raise AssertionError('source retransmission changed content')
    port.acknowledge(11, position=2)
    if port.republish(11).status != 'temporary':
        raise AssertionError('accepted ACK did not release source content')
    return {'publish_config': event.config_id, 'republish_config': reply.notification.config_id,
            'ack_calls': port.ack_calls}


def _contains(actual, expected, path='result'):
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            raise AssertionError(f'{path}: expected object')
        for key, value in expected.items():
            if key not in actual:
                raise AssertionError(f'{path}: missing {key}')
            _contains(actual[key], value, path + '.' + key)
    elif actual != expected:
        raise AssertionError(f'{path}: {actual!r} != {expected!r}')


def run_case(case, db_path):
    from capture.config_versions import register_config
    from capture.storage import CaptureDatabase
    from capture.subscription import configure
    from capture.receiver import pump
    from capture.recovery import resume
    from capture.acknowledgments import flush_acknowledgments
    from capture.projection import load_snapshot
    from capture.export import export_snapshot
    port = VersionedPort(case['spec']['source_id'], case['spec']['subscription_id'],
                         case['events'], case.get('republish'), case.get('acknowledgments'))
    db = CaptureDatabase(db_path)
    checkpoints = []
    saved = {}
    handle = configure(db, case['spec'])
    try:
        for action in case['actions']:
            operation = action['op']
            conversion = None
            if operation == 'pump':
                pump(handle, port, action['limit'])
            elif operation == 'reopen':
                db.close()
                db = CaptureDatabase(db_path)
                handle = resume(db, case['spec']['subscription_id'], port)
            elif operation == 'register':
                conversion = register_config(handle, action['config_id'], action['items'])
            elif operation == 'flush':
                flush_acknowledgments(handle, port)
            elif operation == 'snapshot':
                saved[action['name']] = load_snapshot(handle)
            else:
                raise ValueError('unknown version scenario action')
            report = json.loads(export_snapshot(handle))
            checkpoint = {'label': action['label'], 'report': report,
                          'ack_sequences': [call['sequence'] for call in port.ack_calls]}
            if conversion is not None:
                checkpoint['conversion'] = conversion
            if 'check_saved' in action:
                checkpoint['saved_report'] = json.loads(export_snapshot(
                    handle, snapshot=saved[action['check_saved']]))
            _contains(checkpoint, case['expected'][action['label']])
            checkpoints.append(checkpoint)
        return {'checkpoints': checkpoints, 'ack_calls': port.ack_calls,
                'republish_calls': port.republish_calls}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Version-binding source and business ledger')
    parser.add_argument('--check-port', action='store_true')
    parser.add_argument('--case', default='samples/config_versions.json')
    args = parser.parse_args(argv)
    if args.check_port:
        print(json.dumps(check_port(), ensure_ascii=False))
        return 0
    case = load_case(args.case)
    with tempfile.TemporaryDirectory(prefix='config-binding-') as directory:
        result = run_case(case, Path(directory) / 'capture.sqlite')
    print('CONFIG_BINDING_LEDGER ' + json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
