import json
import unittest
from tests.test_capture import MemoryCapture, spec, event, port
from capture.storage import CaptureDatabase
from capture.subscription import configure
from capture.receiver import pump
from capture.republish import repair_pending
from capture.readings import read, select_readings
from capture.completeness import audit
from capture.projection import load_snapshot
from capture.export import export_snapshot, encode_report


class TestConsumption(MemoryCapture):
    def test_audit_and_export(self):
        self.assertEqual(audit(self.handle).state, 'empty')
        self.assertEqual(read(self.handle, 'idle-3'), ())
        self.accept(event(11))
        waiting = audit(self.handle)
        self.assertEqual((waiting.state, waiting.data_notifications, waiting.pending,
                          waiting.blocked_data), ('waiting', 0, (10,), (11,)))
        self.assertEqual(read(self.handle), ())
        repair_pending(self.handle, port(republish={10: ['unavailable']}))
        result = audit(self.handle)
        self.assertEqual((result.state, result.data_notifications, result.visible_gaps,
                          result.pending, result.covered_positions),
                         ('results', 1, (10,), (), 2))
        report = json.loads(export_snapshot(self.handle, 'pump-1'))
        self.assertEqual([(r['kind'], r['sequence']) for r in report['records']],
                         [('gap', 10), ('data', 11)])
        self.assertEqual([o['item_id'] for o in report['records'][1]['observations']], ['a'])
        self.assertEqual(report['coverage'], result.to_dict())
        self.assertEqual([r.kind for r in read(self.handle, 'idle-3')], ['gap'])
        with self.assertRaises(ValueError):
            read(self.handle, 'unknown-device')

    def test_snapshot_export(self):
        self.accept(event(10))
        snapshot = load_snapshot(self.handle)
        expected = encode_report(snapshot)
        self.accept(event(11, '9.000000000000000000000001'))
        self.assertEqual(export_snapshot(self.handle, snapshot=snapshot), expected)
        self.assertEqual(encode_report(snapshot), expected)
        self.assertEqual([r.sequence for r in select_readings(snapshot)], [10])
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        report = json.loads(expected)
        self.assertEqual(report['records'][0]['observations'][1]['timestamp'],
                         '2026-11-17T08:00:00.123456+08:00')
        self.assertEqual(report['coverage']['covered_positions'], 1)
        self.assertEqual(report['records'][0]['observations'][0]['value'], None)

    def test_gaps_and_waiting_can_coexist(self):
        self.accept(event(11))
        repair_pending(self.handle, port(republish={10: ['unavailable']}))
        self.accept(event(13))
        result = audit(self.handle)
        self.assertEqual((result.state, result.visible_gaps, result.pending,
                          result.blocked_data), ('waiting', (10,), (12,), (13,)))
        self.assertEqual(result.covered_positions, 2)

    def test_corruption_detected(self):
        corruptions = [
            ("INSERT INTO gaps(sub,pos,seq,source,reason) VALUES('vibration',1,10,"
             "'cape-source','MessageNotAvailable')"),
            "UPDATE subscriptions SET frontier=0 WHERE id='vibration'",
            "DELETE FROM acknowledgments WHERE sub='vibration'",
            "UPDATE notifications SET seq=99 WHERE sub='vibration'",
            "UPDATE notifications SET pos=2 WHERE sub='vibration'"]
        for sql in corruptions:
            with self.subTest(sql=sql):
                with CaptureDatabase(':memory:') as db:
                    handle = configure(db, spec())
                    pump(handle, port([event(10)]), 1)
                    if sql.startswith('UPDATE notifications SET pos=2'):
                        db.connection.execute('PRAGMA foreign_keys=OFF')
                    db.connection.execute(sql)
                    before = list(db.connection.iterdump())
                    with self.assertRaises(ValueError):
                        audit(handle)
                    self.assertEqual(list(db.connection.iterdump()), before)
