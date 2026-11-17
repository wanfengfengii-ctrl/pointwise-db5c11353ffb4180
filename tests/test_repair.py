from copy import deepcopy
import unittest
from tests.test_capture import MemoryCapture, event, port, spec
from capture.subscription import configure
from capture.receiver import pump
from capture.republish import repair_pending, apply_republish_reply
from capture.readings import read
from capture.completeness import audit
from capture.acknowledgments import list_acknowledgments, flush_acknowledgments
from capture.identity import NotificationConflict
from capture.export import export_snapshot
from capture.port import RepublishReply, PublishEvent
from capture.pending import list_pending
from capture.positions import MAX_SEQUENCE


class TestRepublish(MemoryCapture):
    def test_late_success(self):
        source = port([event(11), event(12)], republish={
            10: ['temporary', 'temporary', {'status': 'data', 'notification': event(10)}]})
        pump(self.handle, source, 1)
        self.assertEqual(read(self.handle), ())
        first = audit(self.handle)
        self.assertEqual((first.pending, first.blocked_data, first.visible_gaps), ((10,), (11,), ()))
        self.assertEqual([c['sequence'] for c in source.ack_calls], [11])
        pump(self.handle, source, 1)
        self.assertEqual(read(self.handle), ())
        self.assertEqual(audit(self.handle).blocked_data, (11, 12))
        repair_pending(self.handle, source)
        flush_acknowledgments(self.handle, source)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11, 12])
        self.assertEqual(audit(self.handle).visible_gaps, ())
        self.assertEqual(audit(self.handle).pending, ())
        self.assertEqual(audit(self.handle).covered_positions, 3)

    def test_wrap_and_unavailable(self):
        self.handle = configure(self.db, dict(spec(MAX_SEQUENCE - 2), subscription_id='wrap'))
        from capture.port import DeterministicPort
        source = DeterministicPort('cape-source', 'wrap',
            [event(1), {'kind': 'keepalive', 'sequence': 2}, event(MAX_SEQUENCE - 1)],
            {MAX_SEQUENCE: ['temporary', 'temporary', 'unavailable']})
        pump(self.handle, source, 1)
        self.assertEqual(audit(self.handle).pending, (MAX_SEQUENCE - 1, MAX_SEQUENCE))
        self.assertEqual(read(self.handle), ())
        pump(self.handle, source, 1)
        self.assertEqual(audit(self.handle).pending, (MAX_SEQUENCE - 1, MAX_SEQUENCE))
        pump(self.handle, source, 1)
        actual = [(r.kind, r.sequence) for r in read(self.handle)]
        self.assertEqual(actual, [('data', MAX_SEQUENCE - 1), ('gap', MAX_SEQUENCE), ('data', 1)])
        self.assertEqual(audit(self.handle).covered_positions, 3)
        self.assertEqual(audit(self.handle).pending, ())
        self.assertEqual([c['sequence'] for c in source.ack_calls], [1, MAX_SEQUENCE - 1])
        self.assertNotIn(2, source.republish_calls)
        self.assertEqual([r.position for r in read(self.handle)], [1, 2, 3])

    def test_reply_identity_and_evidence(self):
        self.accept(event(11))
        request = list_pending(self.handle)[0]
        before = export_snapshot(self.handle)
        for reply in (
                RepublishReply('unavailable', 10, 'wrong', 'vibration'),
                RepublishReply('unavailable', 12, 'cape-source', 'vibration'),
                RepublishReply('data', 10, 'cape-source', 'vibration',
                               PublishEvent('keepalive', 10)),
                RepublishReply('unknown', 10, 'cape-source', 'vibration')):
            with self.assertRaises(ValueError):
                apply_republish_reply(self.handle, request, reply)
            self.assertEqual(export_snapshot(self.handle), before)

    def test_remote_result_rechecked(self):
        self.accept(event(11))
        request = list_pending(self.handle)[0]
        self.accept(event(10))
        before = export_snapshot(self.handle)
        with self.assertRaises(ValueError):
            apply_republish_reply(self.handle, request,
                                  RepublishReply('unavailable', 10, 'cape-source', 'vibration'))
        self.assertEqual(export_snapshot(self.handle), before)

    def test_gap_cannot_be_replaced(self):
        self.accept(event(11))
        repair_pending(self.handle, port(republish={10: ['unavailable']}))
        before = export_snapshot(self.handle)
        with self.assertRaises(NotificationConflict):
            self.accept(event(10))
        self.assertEqual(export_snapshot(self.handle), before)


class TestNotificationIdentity(MemoryCapture):
    def test_duplicate_and_conflict(self):
        self.accept(event(10))
        before = export_snapshot(self.handle)
        work = list_acknowledgments(self.handle)
        self.assertTrue(self.accept(event(10)).duplicate)
        for change in ('order', 'quality', 'timestamp', 'value'):
            raw = event(10)
            if change == 'order':
                raw['items'].reverse()
            elif change == 'quality':
                raw['items'][0]['quality'] = 'Uncertain'
            elif change == 'timestamp':
                raw['items'][1]['timestamp'] = '2026-11-17T00:00:00.123456Z'
            else:
                raw['items'][1]['value'] = '99'
            with self.subTest(change=change):
                with self.assertRaises(NotificationConflict):
                    self.accept(raw)
                self.assertEqual(export_snapshot(self.handle), before)
                self.assertEqual(list_acknowledgments(self.handle), work)

    def test_duplicate_ack(self):
        source = port([event(10), event(10)])
        pump(self.handle, source, 2)
        self.assertEqual(len(read(self.handle)), 1)
        self.assertEqual(len(source.ack_calls), 1)
        self.assertEqual(len(list_acknowledgments(self.handle)), 1)
        self.assertEqual(list_acknowledgments(self.handle)[0].state, 'done')

    def test_republish_duplicate_uses_same_admission(self):
        self.accept(event(11))
        request = list_pending(self.handle)[0]
        raw = event(10)
        source = port(republish={10: [{'status': 'data', 'notification': raw}]})
        reply = source.republish(10)
        apply_republish_reply(self.handle, request, reply)
        apply_republish_reply(self.handle, request, reply)
        self.assertEqual([r.sequence for r in read(self.handle)], [10, 11])
        self.assertEqual(len(list_acknowledgments(self.handle)), 2)

    def test_numeric_equivalence_preserves_first_text(self):
        self.accept(event(10, '1.00'))
        self.assertTrue(self.accept(event(10, '1.0')).duplicate)
        self.assertEqual(read(self.handle)[0].observations[1].value, '1.00')
