"""只在服务器执行的定向测试，不联网、不启动真实代理。"""
import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import manager
import nodes


class ParsingTests(unittest.TestCase):
    def test_base64_and_transport_parameters(self):
        link = 'vless://00000000-0000-0000-0000-000000000001@example.com:443?security=tls&type=ws&path=%2Fproxy%3Fed%3D2048&host=example.com'
        parsed = nodes.parse_subscription(base64.b64encode(link.encode()).decode())
        self.assertEqual(parsed[0]['transport']['path'], '/proxy?ed=2048')
        self.assertEqual(parsed[0]['tls']['server_name'], 'example.com')

    def test_credentials_are_part_of_identity(self):
        a = nodes.parse_link('trojan://one@example.com:443')
        b = nodes.parse_link('trojan://two@example.com:443')
        self.assertNotEqual(nodes.node_id(a), nodes.node_id(b))
        self.assertTrue(a['tls']['enabled'])

    def test_reality_configuration(self):
        parsed = nodes.parse_link('vless://uuid@example.com:443?security=reality&pbk=abc&sid=01&flow=xtls-rprx-vision')
        self.assertTrue(parsed['tls']['utls']['enabled'])
        self.assertEqual(parsed['tls']['reality']['short_id'], '01')
        self.assertEqual(parsed['flow'], 'xtls-rprx-vision')

    def test_no_direct_fallback(self):
        config = nodes.build_config([nodes.parse_link('trojan://one@example.com:443')], 24000)
        self.assertEqual(config['route']['final'], 'block')
        self.assertEqual(config['route']['rules'][0]['outbound'], 'out-0')


class FakeCore:
    all = []
    fail_production = False

    def __init__(self, config, nodes, port):
        self.nodes, self.port, self.stopped = nodes, port, False
        self.all.append(self)

    def start(self):
        if self.fail_production and self.port in (24000, 25000):
            raise RuntimeError('candidate production failed')

    def stop(self):
        self.stopped = True


class PublicationTests(unittest.TestCase):
    def setUp(self):
        FakeCore.all, FakeCore.fail_production = [], False
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.node = nodes.parse_link('trojan://one@example.com:443')
        self.config = {'state_dir': self.temp.name, 'proxy_file': self.temp.name + '/proxies.txt',
                       'candidate_port': 23000, 'production_ports': [24000, 25000], 'max_candidates': 200,
                       'max_active_nodes': 40, 'check_workers': 2, 'drain_seconds': 125}
        self.file = Path(self.config['proxy_file'])
        self.file.write_text('http://127.0.0.1:20000\n')

    def test_new_pool_starts_before_old_pool_stops(self):
        with patch.object(manager, 'Core', FakeCore), patch.object(manager, 'validate_nodes', side_effect=lambda ns, *args: ns), patch.object(manager, 'fetch_nodes', return_value=[self.node]), patch.object(manager, 'rank_nodes', return_value=[self.node]), patch.object(manager, 'reload_gateway') as reload:
            m = manager.Manager(self.config)
            old = FakeCore(self.config, [self.node], 25000)
            m.active = old
            self.assertTrue(m.cycle())
            self.assertEqual(m.active.port, 24000)
            self.assertFalse(old.stopped)
            self.assertEqual(self.file.read_text(), 'http://127.0.0.1:24000\n')
            reload.assert_called_once()
            self.assertEqual(len(m.retired), 1)
            m.stop()

    def test_reload_failure_rolls_back_file_and_keeps_old_pool(self):
        with patch.object(manager, 'Core', FakeCore), patch.object(manager, 'validate_nodes', side_effect=lambda ns, *args: ns), patch.object(manager, 'fetch_nodes', return_value=[self.node]), patch.object(manager, 'rank_nodes', return_value=[self.node]), patch.object(manager, 'reload_gateway', side_effect=OSError('reload failed')):
            m = manager.Manager(self.config)
            old = FakeCore(self.config, [self.node], 25000)
            m.active = old
            with self.assertRaises(OSError):
                m.cycle()
            self.assertIs(m.active, old)
            self.assertFalse(old.stopped)
            self.assertEqual(self.file.read_text(), 'http://127.0.0.1:20000\n')

    def test_single_working_node_is_not_discarded(self):
        with patch.object(manager, 'check_proxy', return_value=100.0):
            fake = FakeCore(self.config, [self.node], 23000)
            self.assertEqual(manager.rank_nodes([self.node], fake, self.config, {}), [self.node])

    def test_ranking_keeps_fastest_and_records_failed_node(self):
        candidates = [nodes.parse_link(f'trojan://{i}@example.com:443') for i in range(3)]
        config = {**self.config, 'max_active_nodes': 1}
        delays = {23000: 500, 23001: 100, 23002: None}
        state = {}
        with patch.object(manager, 'check_proxy', side_effect=lambda p, c: delays[p]):
            result = manager.rank_nodes(candidates, FakeCore(config, candidates, 23000), config, state)
        self.assertEqual(result, [candidates[1]])
        self.assertFalse(state[nodes.node_id(candidates[2])]['healthy'])


if __name__ == '__main__':
    unittest.main()
