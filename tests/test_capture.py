from copy import deepcopy
import unittest
from capture.storage import CaptureDatabase
from capture.subscription import configure
from capture.port import DeterministicPort
from capture.receiver import pump
from capture.readings import read
from capture.completeness import audit
from capture.export import export_snapshot
from capture.acknowledgments import list_acknowledgments
from capture.notifications import decode_publish
from capture.archive import accept_notification
from capture.positions import PositionWindow, MAX_SEQUENCE, next_sequence, forward_distance


def spec(initial=9):
    return {'source_id': 'cape-source', 'subscription_id': 'vibration',
            'initial_sequence': initial, 'items': [
                {'item_id': 'a', 'device': 'pump-1', 'channel': 'velocity',
                 'unit': 'mm/s', 'quantity': 'velocity'},
                {'item_id': 'b', 'device': 'bench-2', 'channel': 'acceleration',
                 'unit': 'm/s2', 'quantity': 'acceleration'},
                {'item_id': 'c', 'device': 'idle-3', 'channel': 'velocity',
                 'unit': 'mm/s', 'quantity': 'velocity'}]}


def event(sequence, value='1.2345678901234567890123456789'):
    return {'kind': 'data', 'sequence': sequence, 'items': [
        {'item_id': 'b', 'value': None, 'quality': 'Bad', 'timestamp': None},
        {'item_id': 'a', 'value': value, 'quality': 'Good',
         'timestamp': '2026-11-17T08:00:00.123456+08:00'}]}


def port(events=(), republish=None, acknowledgments=None):
    return DeterministicPort('cape-source', 'vibration', events, republish, acknowledgments)


class MemoryCapture(unittest.TestCase):
    def setUp(self):
        self.db = CaptureDatabase(':memory:')
        self.addCleanup(self.db.close)
        self.handle = configure(self.db, spec())

    def accept(self, raw):
        source = port([raw])
        notification = decode_publish(source.publish(), self.handle.registry)
        return accept_notification(self.handle, notification)


class TestDailyCapture(MemoryCapture):
    def test_data_and_keepalive(self):
        source = port([event(10), {'kind': 'keepalive', 'sequence': 11}])
        result = pump(self.handle, source, 2)
        self.assertEqual(result.processed, 2)
        self.assertEqual(result.last_keepalive, 11)
        records = read(self.handle)
        self.assertEqual([(r.kind, r.sequence) for r in records], [('data', 10)])
        values = records[0].observations
        self.assertEqual([v.item_id for v in values], ['b', 'a'])
        self.assertEqual([v.unit for v in values], ['m/s2', 'mm/s'])
        self.assertEqual(values[1].value, '1.2345678901234567890123456789')
        self.assertEqual(values[1].timestamp, '2026-11-17T08:00:00.123456+08:00')
        self.assertEqual((values[0].value, values[0].quality, values[0].timestamp),
                         (None, 'Bad', None))
        self.assertEqual([call['sequence'] for call in source.ack_calls], [10])
        self.assertEqual(audit(self.handle).covered_positions, 1)

    def test_keepalive_only(self):
        source = port([{'kind': 'keepalive', 'sequence': 400}])
        pump(self.handle, source, 1)
        self.assertEqual(read(self.handle), ())
        self.assertEqual(audit(self.handle).state, 'empty')
        self.assertEqual(audit(self.handle).pending, ())
        self.assertEqual(source.ack_calls, [])
        self.assertEqual(source.republish_calls, [])

    def test_zero_limit(self):
        source = port([event(10)])
        before = export_snapshot(self.handle)
        self.assertEqual(pump(self.handle, source, 0).processed, 0)
        self.assertEqual(source.publish_calls, 0)
        self.assertEqual(len(source.events), 1)
        self.assertEqual(export_snapshot(self.handle), before)
        for invalid in (-1, True, 1.5, None):
            with self.assertRaises(ValueError):
                pump(self.handle, source, invalid)
        self.assertEqual(source.publish_calls, 0)

    def test_invalid_item_atomic(self):
        bad_cases = [
            {'item_id': 'unknown'}, {'value': 'NaN'}, {'value': 'Infinity'},
            {'value': 0.25}, {'value': None}, {'quality': 'Unknown'},
            {'timestamp': '2026-11-17T08:00:00'}, {'timestamp': 'not-a-time'}]
        for changes in bad_cases:
            with self.subTest(changes=changes):
                raw = event(10)
                raw['items'][1].update(changes)
                source = port([raw])
                before = export_snapshot(self.handle)
                with self.assertRaises(ValueError):
                    pump(self.handle, source, 1)
                self.assertEqual(export_snapshot(self.handle), before)
                self.assertEqual(source.ack_calls, [])
                self.assertEqual(list_acknowledgments(self.handle), ())

    def test_uncertain_and_empty_notification(self):
        raw = event(10)
        raw['items'][1].update(value=None, quality='Uncertain', timestamp=None)
        source = port([raw, {'kind': 'data', 'sequence': 11, 'items': []}])
        pump(self.handle, source, 2)
        self.assertEqual(read(self.handle)[0].observations[1].quality, 'Uncertain')
        self.assertEqual(read(self.handle)[1].observations, ())
        self.assertEqual(audit(self.handle).data_notifications, 2)

    def test_position_arithmetic(self):
        self.assertEqual(next_sequence(MAX_SEQUENCE), 1)
        self.assertEqual(forward_distance(MAX_SEQUENCE - 2, 1), 3)
        window = PositionWindow(MAX_SEQUENCE - 2, 3)
        self.assertEqual(window.resolve(1).offset, 3)
        self.assertEqual(window.resolve(MAX_SEQUENCE - 1).offset, 1)
        with self.assertRaises(ValueError):
            window.resolve(MAX_SEQUENCE - 2)


class TestConfiguration(MemoryCapture):
    def test_reinitialize(self):
        self.accept(event(11))
        before = export_snapshot(self.handle)
        work = list_acknowledgments(self.handle)
        again = configure(self.db, spec())
        self.assertEqual(again.subscription_id, self.handle.subscription_id)
        self.assertEqual(export_snapshot(again), before)
        self.assertEqual(list_acknowledgments(again), work)
        conflict = spec()
        conflict['items'][0]['device'] = 'other-pump'
        with self.assertRaises(ValueError):
            configure(self.db, conflict)
        self.assertEqual(export_snapshot(self.handle), before)
        reordered = spec()
        reordered['items'].reverse()
        self.assertEqual(export_snapshot(configure(self.db, reordered)), before)

    def test_reject_atomic(self):
        before = export_snapshot(self.handle)
        candidates = []
        unknown = spec()
        unknown['items'][0]['unit'] = 'rpm'
        candidates.append(unknown)
        duplicate = spec()
        duplicate['items'].append(deepcopy(duplicate['items'][0]))
        candidates.append(duplicate)
        mismatch = spec()
        mismatch['items'][0]['quantity'] = 'acceleration'
        candidates.append(mismatch)
        candidates.extend([dict(spec(), initial_sequence=0),
                           dict(spec(), initial_sequence=True), dict(spec(), version=2)])
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                with self.assertRaises(ValueError):
                    configure(self.db, candidate)
                self.assertEqual(export_snapshot(self.handle), before)
