from copy import deepcopy
import unittest
from capture.port import PublishEvent, PortDisconnected
from capture.version_fixture import VersionedPort, VersionedPublishEvent, check_port


class TestVersionSource(unittest.TestCase):
    def test_port_contract(self):
        result = check_port()
        self.assertEqual(result['publish_config'], 'B')
        self.assertEqual(result['republish_config'], 'B')
        self.assertEqual(result['ack_calls'][0]['position'], 2)

    def test_legacy_and_keepalive(self):
        port = VersionedPort('s', 'n', [
            {'kind': 'data', 'sequence': 10, 'items': []},
            {'kind': 'keepalive', 'sequence': 11}])
        self.assertEqual(port.publish(), PublishEvent('data', 10))
        self.assertEqual(port.publish(), PublishEvent('keepalive', 11))
        with self.assertRaises(ValueError):
            VersionedPort._event({'kind': 'keepalive', 'sequence': 11, 'config_id': 'B'})

    def test_binding_is_copied_and_republished(self):
        raw = {'kind': 'data', 'sequence': 10, 'config_id': 'B',
               'items': [{'item_id': 'a', 'value': '1.00', 'quality': 'Good'}]}
        original = deepcopy(raw)
        port = VersionedPort('s', 'n', [raw], acknowledgments={10: ['lost']})
        raw['items'][0]['value'] = '99'
        event = port.publish()
        self.assertIsInstance(event, VersionedPublishEvent)
        event.items[0]['value'] = '88'
        replay = port.republish(10).notification
        self.assertEqual(replay.config_id, 'B')
        self.assertEqual(replay.items[0], original['items'][0])
        with self.assertRaises(PortDisconnected):
            port.acknowledge(10, position=1)
        self.assertEqual(port.republish(10).status, 'temporary')
        self.assertEqual(port.acknowledge(10, position=1).status, 'already')

    def test_bad_binding_and_unknown_field(self):
        for config_id in ('', None, 3):
            with self.subTest(config_id=config_id), self.assertRaises(ValueError):
                VersionedPort._event({'kind': 'data', 'sequence': 10, 'config_id': config_id})
        with self.assertRaises(ValueError):
            VersionedPort._event({'kind': 'data', 'sequence': 10, 'config_id': 'B', 'extra': 1})


if __name__ == '__main__':
    unittest.main()
