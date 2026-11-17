"""Late configuration binding business regressions."""
import json
from collections import deque
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from capture.storage import CaptureDatabase
from capture.subscription import configure, load_subscription
from capture.config_versions import register_config
from capture.receiver import pump
from capture.version_fixture import VersionedPort
from capture.republish import repair_pending, apply_republish_reply
from capture.acknowledgments import list_acknowledgments, flush_acknowledgments
from capture.readings import read
from capture.completeness import audit
from capture.export import export_snapshot, encode_report
from capture.projection import load_snapshot, validate_snapshot
from capture.pending import list_pending
from capture.identity import NotificationConflict
from capture.port import RepublishReply, PortDisconnected


SPEC = {
    'source_id': 'cape-source', 'subscription_id': 'vibration',
    'initial_sequence': 9,
    'items': [{'item_id': 'a', 'device': 'pump-1', 'channel': 'velocity',
               'unit': 'mm/s', 'quantity': 'velocity'}],
}
B_ITEMS = [{'item_id': 'a', 'device': 'centrifuge-4', 'channel': 'acceleration',
            'unit': 'm/s2', 'quantity': 'acceleration'}]


def versioned(sequence, config_id='B', value='1.00', item_id='a', quality='Good'):
    return {'kind': 'data', 'sequence': sequence, 'config_id': config_id,
            'items': [{'item_id': item_id, 'value': value, 'quality': quality,
                       'timestamp': None}]}


def initial_event(sequence, value='2.500', item_id='a'):
    return {'kind': 'data', 'sequence': sequence,
            'items': [{'item_id': item_id, 'value': value, 'quality': 'Good',
                       'timestamp': None}]}


def source_events(*values):
    return VersionedPort('cape-source', 'vibration', list(values))


class LateConfigBase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='late-config-')
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'capture.sqlite'
        self.db = CaptureDatabase(self.path)
        self.addCleanup(self._close)
        self.handle = configure(self.db, deepcopy(SPEC))

    def _close(self):
        try:
            self.db.close()
        except Exception:
            pass

    def reopen(self):
        self.db.close()
        self.db = CaptureDatabase(self.path)
        self.handle = load_subscription(self.db, 'vibration')


class TestLateBinding(LateConfigBase):
    def test_candidate_blocks_without_ack_and_interprets_after_registration(self):
        source = source_events(versioned(11), initial_event(10))
        pump(self.handle, source, 1)
        self.assertEqual(read(self.handle), ())
        waiting = audit(self.handle)
        self.assertEqual((waiting.state, waiting.pending, waiting.waiting_config,
                          waiting.blocked_data, waiting.frontier),
                         ('waiting', (10,), (11,), (), 0))
        self.assertEqual(source.ack_calls, [])
        self.assertNotIn(11, source.republish_calls)
        pump(self.handle, source, 1)
        self.assertEqual([c['sequence'] for c in source.ack_calls], [10])
        self.assertEqual(audit(self.handle).frontier, 1)
        self.assertEqual(audit(self.handle).waiting_config, (11,))
        conversion = register_config(self.handle, 'B', deepcopy(B_ITEMS))
        self.assertEqual(conversion, {'promoted': [11], 'invalid': []})
        records = read(self.handle)
        self.assertEqual([(r.kind, r.sequence, r.observations[0].device,
                           r.observations[0].unit, r.observations[0].value)
                          for r in records],
                         [('data', 10, 'pump-1', 'mm/s', '2.500'),
                          ('data', 11, 'centrifuge-4', 'm/s2', '1.00')])
        self.assertEqual(audit(self.handle).frontier, 2)
        self.assertEqual([w.state for w in list_acknowledgments(self.handle)],
                         ['done', 'pending'])
        flush_acknowledgments(self.handle, source)
        self.assertEqual([c['sequence'] for c in source.ack_calls], [10, 11])
        self.assertNotIn(11, source.retransmissions)

    def test_republish_data_for_unknown_binding_creates_candidate(self):
        pump(self.handle, source_events(versioned(11)), 1)
        self.assertEqual(audit(self.handle).pending, (10,))
        source = source_events()
        source.republish_scripts[10] = deque([
            {'status': 'data', 'notification': versioned(10)}])
        repair_pending(self.handle, source)
        self.assertEqual(source.republish_calls, [10])
        self.assertEqual(audit(self.handle).waiting_config, (10, 11))
        self.assertEqual(audit(self.handle).pending, ())
        self.assertEqual(source.ack_calls, [])
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [10, 11], 'invalid': []})
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        flush_acknowledgments(self.handle, source)
        self.assertEqual([c['sequence'] for c in source.ack_calls], [10, 11])

    def test_unrelated_fields_validated_before_waiting(self):
        bad = [
            dict(versioned(13), sequence=0), dict(versioned(13), sequence=True),
            dict(versioned(13), sequence=1.5), dict(versioned(13), sequence=4294967296),
            {'kind': 'data', 'sequence': 13, 'config_id': '',
             'items': [{'item_id': 'a', 'value': '1', 'quality': 'Good'}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 5,
             'items': [{'item_id': 'a', 'value': '1', 'quality': 'Good'}]},
            versioned(13, item_id=''),
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 3, 'value': '1', 'quality': 'Good'}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 'a', 'value': 'NaN', 'quality': 'Good'}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 'a', 'value': 'Infinity', 'quality': 'Bad'}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 'a', 'value': 1.5, 'quality': 'Good'}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 'a', 'value': '1', 'quality': 'Good',
                        'timestamp': '2026-11-17T08:00:00'}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 'a', 'value': '1', 'quality': 'Good', 'extra': 1}]},
            {'kind': 'data', 'sequence': 13, 'config_id': 'B',
             'items': [{'item_id': 'a', 'value': None, 'quality': 'Good'}]},
            {'kind': 'data', 'sequence': 13,
             'items': [{'item_id': 'unknown', 'value': '1', 'quality': 'Good'}]},
        ]
        before = export_snapshot(self.handle)
        for raw in bad:
            with self.subTest(raw=raw):
                source = source_events()
                source.events.append(raw)
                with self.assertRaises(ValueError):
                    pump(self.handle, source, 1)
                self.assertEqual(export_snapshot(self.handle), before)
                self.assertEqual(source.ack_calls, [])
                self.assertEqual(source.republish_calls, [])
        self.assertEqual(audit(self.handle).known_positions, 0)

    def test_unknown_binding_preserves_order_and_first_text(self):
        source = source_events(versioned(10), versioned(10))
        pump(self.handle, source, 2)
        snapshot = load_snapshot(self.handle)
        self.assertEqual([(c.config_id, c.state) for c in snapshot.candidates],
                         [('B', 'waiting')])
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [10], 'invalid': []})
        self.assertEqual(read(self.handle)[0].observations[0].value, '1.00')
        self.assertEqual(len(read(self.handle)), 1)
        flush_acknowledgments(self.handle, source)
        self.assertEqual([c['sequence'] for c in source.ack_calls], [10])
        self.assertEqual(audit(self.handle).state, 'results')

    def test_duplicate_candidate_numeric_and_first_text(self):
        pump(self.handle, source_events(versioned(10, value='1.00')), 1)
        pump(self.handle, source_events(versioned(10, value='1.0')), 1)
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        self.assertEqual(read(self.handle)[0].observations[0].value, '1.00')
        before = export_snapshot(self.handle)
        with self.assertRaises(NotificationConflict):
            pump(self.handle, source_events(versioned(10, value='2')), 1)
        self.assertEqual(export_snapshot(self.handle), before)

    def test_same_position_conflict_in_waiting_and_registered_states(self):
        pump(self.handle, source_events(versioned(11)), 1)
        before = export_snapshot(self.handle)
        with self.assertRaises(NotificationConflict):
            pump(self.handle, source_events(versioned(11, value='9')), 1)
        self.assertEqual(export_snapshot(self.handle), before)
        with self.assertRaises(NotificationConflict):
            pump(self.handle, source_events(versioned(11, config_id='C')), 1)
        self.assertEqual(export_snapshot(self.handle), before)
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        pump(self.handle, source_events(initial_event(10)), 1)
        before = export_snapshot(self.handle)
        with self.assertRaises(NotificationConflict):
            pump(self.handle, source_events(versioned(11, value='9')), 1)
        self.assertEqual(export_snapshot(self.handle), before)
        self.assertEqual(read(self.handle)[1].observations[0].value, '1.00')

    def test_invalid_candidate_replay_is_idempotent(self):
        pump(self.handle, source_events(versioned(11, item_id='missing')), 1)
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [], 'invalid': [11]})
        before = export_snapshot(self.handle)
        self.assertEqual(audit(self.handle).invalid, (11,))
        pump(self.handle, source_events(versioned(11, item_id='missing')), 1)
        self.assertEqual(export_snapshot(self.handle), before)
        with self.assertRaises(NotificationConflict):
            pump(self.handle, source_events(versioned(11, item_id='other')), 1)
        self.assertEqual(export_snapshot(self.handle), before)

    def test_known_version_illegal_notification_rejected_whole(self):
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        before = export_snapshot(self.handle)
        with self.assertRaises(ValueError):
            pump(self.handle, source_events(versioned(10, item_id='missing')), 1)
        self.assertEqual(export_snapshot(self.handle), before)
        self.assertEqual(audit(self.handle).known_positions, 0)


class TestInvalidCandidates(LateConfigBase):
    def test_invalid_blocks_but_later_initial_data_is_confirmable(self):
        source = source_events(versioned(11, config_id='LATE'),
                               versioned(12, config_id='LATE'))
        pump(self.handle, source, 1)
        pump(self.handle, source, 1)
        self.assertEqual(audit(self.handle).waiting_config, (11, 12))
        conversion = register_config(self.handle, 'LATE', [
            {'item_id': 'zzz', 'device': 'other', 'channel': 'x', 'unit': 'mm/s',
             'quantity': 'velocity'}])
        self.assertEqual(conversion, {'promoted': [], 'invalid': [11, 12]})
        result = audit(self.handle)
        self.assertEqual((result.invalid, result.waiting_config, result.frontier),
                         ((11, 12), (), 0))
        self.assertEqual(source.ack_calls, [])
        later = source_events(initial_event(13))
        pump(self.handle, later, 1)
        self.assertEqual(audit(self.handle).invalid, (11, 12))
        self.assertEqual(audit(self.handle).blocked_data, (13,))
        self.assertEqual([c['sequence'] for c in later.ack_calls], [13])
        self.assertEqual(audit(self.handle).frontier, 0)
        self.reopen()
        self.assertEqual(audit(self.handle).invalid, (11, 12))
        self.assertEqual([r.sequence for r in read(self.handle)], [])

    def test_mixed_candidates_judged_individually(self):
        source = source_events(versioned(11), versioned(12, item_id='missing'))
        pump(self.handle, source, 1)
        pump(self.handle, source, 1)
        self.assertEqual(audit(self.handle).pending, (10,))
        conversion = register_config(self.handle, 'B', deepcopy(B_ITEMS))
        self.assertEqual(conversion, {'promoted': [11], 'invalid': [12]})
        result = audit(self.handle)
        self.assertEqual((result.frontier, result.invalid, result.pending),
                         (0, (12,), (10,)))
        initial = source_events(initial_event(10))
        pump(self.handle, initial, 1)
        self.assertEqual(audit(self.handle).frontier, 2)
        self.assertEqual(audit(self.handle).invalid, (12,))
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        self.assertEqual([c['sequence'] for c in initial.ack_calls], [10, 11])

    def test_unavailable_reply_cannot_cover_candidate(self):
        pump(self.handle, source_events(versioned(11)), 1)
        request = list_pending(self.handle)[0]
        pump(self.handle, source_events(versioned(10)), 1)
        before = export_snapshot(self.handle)
        with self.assertRaises(ValueError):
            apply_republish_reply(
                self.handle, request,
                RepublishReply('unavailable', 10, 'cape-source', 'vibration'))
        self.assertEqual(export_snapshot(self.handle), before)
        self.assertEqual(audit(self.handle).waiting_config, (10, 11))


class TestRegistration(LateConfigBase):
    def test_idempotent_conflict_and_invalid_items(self):
        pump(self.handle, source_events(versioned(11)), 1)
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [11], 'invalid': []})
        before = export_snapshot(self.handle)
        work = list_acknowledgments(self.handle)
        self.assertEqual(register_config(self.handle, 'B', list(reversed(B_ITEMS))),
                         {'promoted': [], 'invalid': []})
        self.assertEqual(export_snapshot(self.handle), before)
        self.assertEqual(list_acknowledgments(self.handle), work)
        for bad in ([], None, 3, ''):
            with self.subTest(config_id=bad), self.assertRaises(ValueError):
                register_config(self.handle, bad, deepcopy(B_ITEMS))
        with self.assertRaises(ValueError):
            register_config(self.handle, 'B', [{'item_id': 'a', 'device': 'x',
                                                'channel': 'c', 'unit': 'rpm',
                                                'quantity': 'velocity'}])
        with self.assertRaises(ValueError):
            register_config(self.handle, 'B', [])
        self.assertEqual(export_snapshot(self.handle), before)

    def test_registration_does_not_call_source(self):
        pump(self.handle, source_events(versioned(11)), 1)
        source = source_events()
        self.reopen()
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        self.assertEqual((source.publish_calls, source.republish_calls,
                          source.ack_calls), (0, [], []))

    def test_commit_failure_rolls_back_registration(self):
        pump(self.handle, source_events(versioned(11)), 1)
        before = export_snapshot(self.handle)

        def fail():
            raise RuntimeError('controlled pre-commit failure')
        self.db.before_commit = fail
        with self.assertRaisesRegex(RuntimeError, 'pre-commit'):
            register_config(self.handle, 'B', deepcopy(B_ITEMS))
        self.db.before_commit = None
        self.assertEqual(export_snapshot(self.handle), before)
        self.reopen()
        self.assertEqual(audit(self.handle).waiting_config, (11,))
        self.assertEqual(read(self.handle), ())
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [11], 'invalid': []})

    def test_initial_version_is_immutable_and_empty_registration_ok(self):
        with self.assertRaises(ValueError):
            register_config(self.handle, 'initial', deepcopy(B_ITEMS))
        self.assertEqual(audit(self.handle).state, 'empty')
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [], 'invalid': []})
        self.assertEqual(audit(self.handle).state, 'empty')

    def test_transition_persists_across_reopen(self):
        pump(self.handle, source_events(versioned(11)), 1)
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        before = export_snapshot(self.handle)
        self.reopen()
        self.assertEqual(export_snapshot(self.handle), before)
        self.assertEqual(audit(self.handle).state, 'waiting')
        self.assertEqual(audit(self.handle).pending, (10,))
        source = source_events(initial_event(10))
        pump(self.handle, source, 1)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        flush_acknowledgments(self.handle, source)
        self.assertEqual([c['sequence'] for c in source.ack_calls], [10, 11])

    def test_lost_ack_after_promotion_is_retried_after_reopen(self):
        source = source_events(versioned(11), initial_event(10))
        source.ack_scripts[11] = deque(['lost'])
        pump(self.handle, source, 1)
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        with self.assertRaises(PortDisconnected):
            pump(self.handle, source, 1)
        self.assertTrue(source.ack_calls[-1]['accepted'])
        self.assertEqual([c['sequence'] for c in source.ack_calls], [10, 11])
        self.assertNotIn(11, source.retransmissions)
        committed = export_snapshot(self.handle)
        self.reopen()
        self.assertEqual(export_snapshot(self.handle), committed)
        self.assertEqual([w.state for w in list_acknowledgments(self.handle)],
                         ['done', 'pending'])
        from capture.recovery import resume
        self.handle = resume(self.db, 'vibration', source)
        self.assertEqual(export_snapshot(self.handle), committed)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        self.assertEqual([c['status'] for c in source.ack_calls], ['success', 'lost',
                                                                   'already'])
        self.assertEqual(list_acknowledgments(self.handle)[1].receipt, 'already')

    def test_resume_does_not_republish_candidate_positions(self):
        source = source_events(versioned(11))
        pump(self.handle, source, 1)
        self.assertEqual(audit(self.handle).waiting_config, (11,))
        from capture.recovery import resume
        self.reopen()
        self.handle = resume(self.db, 'vibration', source)
        self.assertNotIn(11, source.republish_calls)
        self.assertEqual(audit(self.handle).waiting_config, (11,))
        self.assertEqual([r.sequence for r in read(self.handle)], [])


class TestVersionedReadsAndExport(LateConfigBase):
    def test_device_filter_covers_registered_versions(self):
        pump(self.handle, source_events(versioned(11), initial_event(10)), 2)
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        self.assertEqual([r.sequence for r in read(self.handle, 'centrifuge-4')], [11])
        self.assertEqual([r.sequence for r in read(self.handle, 'pump-1')], [10])
        with self.assertRaises(ValueError):
            read(self.handle, 'unknown-device')
        report = json.loads(export_snapshot(self.handle, 'centrifuge-4'))
        self.assertEqual([(r['kind'], r['sequence'], r['observations'][0]['unit'])
                          for r in report['records']], [('data', 11, 'm/s2')])

    def test_saved_snapshot_stays_frozen(self):
        pump(self.handle, source_events(versioned(11)), 1)
        frozen = load_snapshot(self.handle)
        frozen_bytes = export_snapshot(self.handle, snapshot=frozen)
        register_config(self.handle, 'B', deepcopy(B_ITEMS))
        pump(self.handle, source_events(initial_event(10)), 1)
        self.assertEqual(export_snapshot(self.handle, snapshot=frozen), frozen_bytes)
        report = json.loads(frozen_bytes)
        self.assertEqual(report['coverage']['waiting_config'], [11])
        self.assertEqual(report['records'], [])
        self.assertEqual(encode_report(frozen), frozen_bytes)
        self.assertEqual(validate_snapshot(frozen), frozen)

    def test_corruption_detected_for_version_facts(self):
        corruptions = [
            "DELETE FROM config_versions WHERE sub='vibration' AND config_id='initial'",
            "UPDATE candidates SET config_id='initial' WHERE sub='vibration'",
            "UPDATE config_versions SET items='[]' WHERE sub='vibration' "
            "AND config_id='initial'",
            "INSERT INTO candidates(sub,pos,seq,config_id,state,payload) "
            "VALUES('vibration',1,10,'B','waiting','[]')",
        ]
        for sql in corruptions:
            with self.subTest(sql=sql):
                with CaptureDatabase(':memory:') as db:
                    handle = configure(db, deepcopy(SPEC))
                    pump(handle, source_events(versioned(11)), 1)
                    db.connection.execute(sql)
                    before = list(db.connection.iterdump())
                    with self.assertRaises(ValueError):
                        audit(handle)
                    self.assertEqual(list(db.connection.iterdump()), before)


class TestRestartPersistence(LateConfigBase):
    CHILD = '''
import json, os, sys
from capture.storage import CaptureDatabase
from capture.subscription import load_subscription
from capture.notifications import decode_versioned_publish
from capture.archive import accept_source
from capture.version_fixture import VersionedPort
db = CaptureDatabase(sys.argv[1])
handle = load_subscription(db, 'vibration')
source = VersionedPort('cape-source', 'vibration', [json.loads(sys.argv[3])])
if sys.argv[2] == 'before':
    db.before_commit = lambda: os._exit(19)
accept_source(handle, decode_versioned_publish(source.publish()))
os._exit(20)
'''

    def test_candidate_commit_boundary(self):
        import subprocess
        import sys
        root = Path(__file__).resolve().parents[1]
        raw = versioned(11)
        for boundary, expected_exit in [('before', 19), ('after', 20)]:
            completed = subprocess.run(
                [sys.executable, '-B', '-c', self.CHILD, str(self.path), boundary,
                 json.dumps(raw)], cwd=root, capture_output=True, text=True, timeout=20)
            self.assertEqual(completed.returncode, expected_exit, completed.stderr)
            self.reopen()
            if boundary == 'before':
                self.assertEqual(audit(self.handle).known_positions, 0)
                self.assertEqual(audit(self.handle).waiting_config, ())
            else:
                self.assertEqual(audit(self.handle).waiting_config, (11,))
                self.assertEqual(audit(self.handle).pending, (10,))
                register_config(self.handle, 'B', deepcopy(B_ITEMS))
                self.assertEqual(read(self.handle), ())
                self.assertEqual(audit(self.handle).blocked_data, (11,))
                self.assertEqual(audit(self.handle).frontier, 0)

    def test_helper_runs_while_remote_calls_happen(self):
        source = source_events(versioned(11), initial_event(10))
        operations = []

        def second_writer(operation, sequence):
            self.assertFalse(self.db.connection.in_transaction)
            operations.append(operation)
        source.before_remote = second_writer
        pump(self.handle, source, 1)
        self.assertEqual(register_config(self.handle, 'B', deepcopy(B_ITEMS)),
                         {'promoted': [11], 'invalid': []})
        pump(self.handle, source, 1)
        self.assertIn('republish', operations)
        self.assertIn('acknowledge', operations)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])


if __name__ == '__main__':
    unittest.main()
