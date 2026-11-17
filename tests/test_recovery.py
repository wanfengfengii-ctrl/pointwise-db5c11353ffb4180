import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from tests.test_capture import spec, event, port
from capture.storage import CaptureDatabase
from capture.subscription import configure, load_subscription
from capture.receiver import pump
from capture.recovery import resume, load_recovery_plan
from capture.republish import repair_pending
from capture.acknowledgments import list_acknowledgments, flush_acknowledgments
from capture.readings import read
from capture.completeness import audit
from capture.export import export_snapshot
from capture.port import PortDisconnected


class TestRecovery(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='capture-test-')
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'capture.sqlite'
        self.db = CaptureDatabase(self.path)
        self.addCleanup(lambda: self.db.close())
        self.handle = configure(self.db, spec())

    def reopen(self):
        self.db.close()
        self.db = CaptureDatabase(self.path)
        self.handle = load_subscription(self.db, 'vibration')

    def test_disconnect_with_pending(self):
        source = port([event(11), {'kind': 'disconnect'}], republish={
            10: ['temporary', {'status': 'data', 'notification': event(10)}]})
        with self.assertRaises(PortDisconnected):
            pump(self.handle, source, 2)
        before = load_recovery_plan(self.handle)
        self.assertEqual(before.pending[0].sequence, 10)
        self.assertEqual(before.blocked, (11,))
        self.reopen()
        self.assertEqual(load_recovery_plan(self.handle), before)
        self.handle = resume(self.db, 'vibration', source)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        self.assertEqual(audit(self.handle).pending, ())
        self.assertEqual([c['sequence'] for c in source.ack_calls], [11, 10])

    def test_source_mismatch(self):
        pump(self.handle, port([event(11)]), 1)
        self.reopen()
        before = export_snapshot(self.handle)
        wrong = port()
        wrong.source_id = 'other-source'
        with self.assertRaises(ValueError):
            resume(self.db, 'vibration', wrong)
        with self.assertRaises(ValueError):
            resume(self.db, 'unknown', port())
        self.assertEqual(export_snapshot(self.handle), before)

    def test_commit_and_lost_ack(self):
        source = port([event(10)])
        before = export_snapshot(self.handle)
        def fail_commit():
            raise RuntimeError('controlled pre-commit failure')
        self.db.before_commit = fail_commit
        with self.assertRaisesRegex(RuntimeError, 'pre-commit'):
            pump(self.handle, source, 1)
        self.db.before_commit = None
        self.assertEqual(export_snapshot(self.handle), before)
        self.assertEqual(source.ack_calls, [])
        self.assertEqual(list_acknowledgments(self.handle), ())
        self.reopen()
        source.events.append(event(10))
        source.ack_scripts[10] = __import__('collections').deque(['lost'])
        with self.assertRaises(PortDisconnected):
            pump(self.handle, source, 1)
        self.assertTrue(source.ack_calls[-1]['accepted'])
        self.assertNotIn(10, source.retransmissions)
        self.assertEqual(list_acknowledgments(self.handle)[0].state, 'pending')
        committed = export_snapshot(self.handle)
        self.reopen()
        self.handle = resume(self.db, 'vibration', source)
        self.assertEqual(export_snapshot(self.handle), committed)
        self.assertEqual(len(read(self.handle)), 1)
        self.assertEqual([c['status'] for c in source.ack_calls], ['lost', 'already'])
        self.assertEqual(list_acknowledgments(self.handle)[0].receipt, 'already')

    def test_no_remote_write_lock(self):
        source = port([event(11)], republish={
            10: [{'status': 'data', 'notification': event(10)}]})
        operations = []
        def second_writer(operation, sequence):
            self.assertFalse(self.db.connection.in_transaction)
            con = sqlite3.connect(self.path, isolation_level=None, timeout=0)
            try:
                con.execute('BEGIN IMMEDIATE')
                con.execute('UPDATE subscriptions SET high=high WHERE id=?', ('vibration',))
                con.commit()
            finally:
                con.close()
            operations.append(operation)
        source.before_remote = second_writer
        pump(self.handle, source, 1)
        self.assertIn('republish', operations)
        self.assertIn('acknowledge', operations)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])

    def test_each_public_event_boundary(self):
        source = port([event(12), {'kind': 'keepalive', 'sequence': 13}, event(10), event(11)])
        for index in range(4):
            pump(self.handle, source, 1)
            before = export_snapshot(self.handle)
            self.reopen()
            self.assertEqual(export_snapshot(self.handle), before)
            self.handle = resume(self.db, 'vibration', source)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11, 12])
        self.assertEqual(len(source.ack_calls), 3)
        self.assertEqual(audit(self.handle).pending, ())

    def test_process_commit_boundaries(self):
        child = '''
import os, sys
from capture.storage import CaptureDatabase
from capture.subscription import load_subscription
from capture.notifications import decode_publish
from capture.archive import accept_notification
from capture.port import DeterministicPort
import json
db = CaptureDatabase(sys.argv[1])
handle = load_subscription(db, 'vibration')
source = DeterministicPort('cape-source', 'vibration', [json.loads(sys.argv[3])])
notification = decode_publish(source.publish(), handle.registry)
if sys.argv[2] == 'before':
    db.before_commit = lambda: os._exit(19)
accept_notification(handle, notification)
os._exit(20)
'''
        root = Path(__file__).resolve().parents[1]
        source = port([event(10)])
        source.publish()
        for boundary, expected_exit in [('before', 19), ('after', 20)]:
            completed = subprocess.run(
                [sys.executable, '-B', '-c', child, str(self.path), boundary, json.dumps(event(10))],
                cwd=root, capture_output=True, text=True, timeout=20)
            self.assertEqual(completed.returncode, expected_exit, completed.stderr)
            self.reopen()
            if boundary == 'before':
                self.assertEqual(read(self.handle), ())
                self.assertEqual(list_acknowledgments(self.handle), ())
            else:
                self.assertEqual([r.sequence for r in read(self.handle)], [10])
                self.assertEqual(list_acknowledgments(self.handle)[0].state, 'pending')
                flush_acknowledgments(self.handle, source)
                self.assertEqual([c['sequence'] for c in source.ack_calls], [10])
                self.assertNotIn(10, source.retransmissions)

    def test_temporary_ack_is_not_completion(self):
        source = port([event(10)], acknowledgments={10: ['temporary', 'success']})
        pump(self.handle, source, 1)
        self.assertEqual(list_acknowledgments(self.handle)[0].state, 'pending')
        self.reopen()
        resume(self.db, 'vibration', source)
        self.assertEqual(list_acknowledgments(self.handle)[0].state, 'done')
        self.assertEqual(len(read(self.handle)), 1)
