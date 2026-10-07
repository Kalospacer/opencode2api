#!/usr/bin/env python3
"""服务器端订阅代理池：发现、测速、择优、无中断切换与热重载。"""
import argparse
import concurrent.futures
import copy
import http.cookiejar
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
from urllib.request import Request, build_opener, ProxyHandler, HTTPCookieProcessor

from nodes import build_config, node_id
from sources import fetch_nodes

log = logging.getLogger('proxy-pool')


def atomic_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            out.write(text)
        os.chmod(temp, 0o600)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def check_proxy(port, config):
    # curl 显式指定代理并清空 no_proxy，避免环境变量让测速绕过节点。
    command = ['curl', '--silent', '--show-error', '--noproxy', '', '--proxy', f'http://127.0.0.1:{port}',
               '--max-time', str(config['check_timeout_seconds']), '--output', os.devnull,
               '--write-out', '%{http_code} %{time_starttransfer}',
               '--header', 'Authorization: Bearer public', config['check_url']]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=config['check_timeout_seconds'] + 2)
        code, delay = result.stdout.strip().split()
        if result.returncode == 0 and code == '200':
            return float(delay) * 1000
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    return None


class Core:
    def __init__(self, config, nodes, port):
        self.config = config
        self.nodes = nodes
        self.port = port
        self.process = None
        self.path = Path(config['state_dir']) / f'core-{port}.json'
        self.logfile = None

    def start(self):
        atomic_write(self.path, json.dumps(build_config(self.nodes, self.port)))
        # 先校验候选配置，绝不先停旧进程。
        result = subprocess.run([self.config['sing_box_bin'], 'check', '-c', str(self.path)], capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            raise RuntimeError('sing-box config check failed: ' + result.stderr[-500:])
        self.logfile = open(Path(self.config['state_dir']) / f'core-{self.port}.log', 'a')
        self.process = subprocess.Popen([self.config['sing_box_bin'], 'run', '-c', str(self.path)],
                                        stdin=subprocess.DEVNULL, stdout=self.logfile, stderr=self.logfile)
        time.sleep(1)
        if self.process.poll() is not None:
            raise RuntimeError('sing-box failed to start; check core log')

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.logfile:
            self.logfile.close()
            self.logfile = None


def reload_gateway(config):
    # 本地管理接口直连；Token 来自登录 JSON，不是 Cookie。
    opener = build_opener(ProxyHandler({}), HTTPCookieProcessor(http.cookiejar.CookieJar()))
    base = config['admin_url'].rstrip('/')
    credentials = {'username': config['admin_username'], 'password': config['admin_password']}
    request = Request(base + '/api/auth/login', data=json.dumps(credentials).encode(), headers={'Content-Type': 'application/json'})
    with opener.open(request, timeout=10) as response:
        login = json.load(response)
    headers = {'Content-Type': 'application/json', 'X-CSRF-Token': login['csrf_token']}
    try:
        with opener.open(Request(base + '/api/config/reload', data=b'{}', headers=headers), timeout=30) as response:
            result = json.load(response)
            log.info('gateway config reloaded')
            return result
    finally:
        try:
            with opener.open(Request(base + '/api/auth/logout', data=b'{}', headers=headers), timeout=5):
                pass
        except OSError:
            log.warning('admin logout failed after reload')


def validate_nodes(nodes, config, cache):
    # 缓存的是完整配置指纹；参数或凭证变化时必须重新校验。
    valid = cache.setdefault('config_valid', {})
    path = Path(config['state_dir']) / 'validate.json'
    result = []
    for node in nodes:
        identity = node_id(node)
        if identity not in valid:
            atomic_write(path, json.dumps(build_config([node], config['candidate_port'])))
            try:
                checked = subprocess.run([config['sing_box_bin'], 'check', '-c', str(path)], capture_output=True, timeout=5)
                valid[identity] = checked.returncode == 0
            except subprocess.TimeoutExpired:
                valid[identity] = False
        if valid[identity]:
            result.append(node)
    log.info('configuration validation: %d accepted, %d unsupported', len(result), len(nodes) - len(result))
    return result


def rank_nodes(nodes, core, config, state):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=config['check_workers']) as executor:
        futures = {executor.submit(check_proxy, core.port + i, config): i for i in range(len(nodes))}
        for future in concurrent.futures.as_completed(futures):
            i = futures[future]
            latency = future.result()
            identity = node_id(nodes[i])
            previous = state.get(identity, {})
            if latency is None:
                state[identity] = {'healthy': False, 'failures': previous.get('failures', 0) + 1, 'checked_at': time.time()}
            else:
                # 平滑偶发抖动；同一节点下轮变慢会降低排名。
                score = .7 * latency + .3 * previous.get('latency_ms', latency)
                state[identity] = {'healthy': True, 'latency_ms': score, 'checked_at': time.time(), 'failures': 0}
                results.append((score, nodes[i]))
    results.sort(key=lambda row: row[0])
    selected = results[:config['max_active_nodes']]
    log.info('ranked %d candidates: %d reachable, %d selected; top ms=%s', len(nodes), len(results), len(selected), [round(x[0]) for x in selected[:5]])
    return [node for _, node in selected]


class Manager:
    def __init__(self, config):
        self.config = config
        self.active = None
        self.retired = []
        self.cache = {}
        self.health = {}
        self.discovery_offset = 0
        self.stop_event = threading.Event()

    def cycle(self):
        self.cleanup_retired()
        discovered = fetch_nodes(self.config, self.cache)
        # 活动节点必须参加复测，不能被随机抽样丢掉；其余节点按轮次覆盖。
        nodes = list(self.active.nodes) if self.active else []
        known = {node_id(node) for node in nodes}
        extra = [node for node in discovered if node_id(node) not in known]
        room = max(0, self.config['max_candidates'] - len(nodes))
        if extra and room:
            start = self.discovery_offset % len(extra)
            chosen = (extra[start:] + extra[:start])[:room]
            self.discovery_offset += len(chosen)
            nodes.extend(chosen)
        if not nodes:
            log.warning('no candidate nodes; existing pool retained')
            return False
        nodes = validate_nodes(nodes, self.config, self.cache)
        if not nodes:
            log.warning('no supported candidates; existing pool retained')
            return False
        port = self.config['candidate_port']
        candidate = Core(self.config, nodes, port)
        try:
            candidate.start()
            selected = rank_nodes(nodes, candidate, self.config, self.health)
        finally:
            candidate.stop()
        atomic_write(Path(self.config['state_dir']) / 'health.json', json.dumps(self.health))
        if not selected:
            # 空白名单不会写给网关，避免其默认回退为 direct。
            log.warning('no healthy candidates; not publishing an empty proxy file')
            return False
        production_ports = self.config['production_ports']
        occupied = {core.port for core, _ in self.retired}
        if self.active:
            occupied.add(self.active.port)
        available = [p for p in production_ports if p not in occupied]
        if not available:
            log.info('previous pool still draining; postpone publication')
            return False
        new = Core(self.config, selected, available[0])
        proxy_file = Path(self.config['proxy_file'])
        previous_file = proxy_file.read_text() if proxy_file.exists() else ''
        try:
            new.start()
            atomic_write(proxy_file, ''.join(f'http://127.0.0.1:{new.port + i}\n' for i in range(len(selected))))
            reload_gateway(self.config)
        except Exception:
            atomic_write(proxy_file, previous_file)
            # reload 成功与否不确定时，尽力重新加载旧文件；旧进程始终在线。
            try:
                reload_gateway(self.config)
            except OSError:
                log.error('gateway rollback reload failed; inspect admin API')
            new.stop()
            raise
        old = self.active
        self.active = new
        if old:
            self.retired.append((old, time.monotonic() + self.config['drain_seconds']))
        atomic_write(Path(self.config['state_dir']) / 'active.json', json.dumps({'port': new.port, 'nodes': selected, 'published_at': time.time()}))
        atomic_write(Path(self.config['state_dir']) / 'sources.json', json.dumps(self.cache))
        log.info('published %d latency-ranked proxies at port %d; old pool retained for in-flight requests', len(selected), new.port)
        return True

    def cleanup_retired(self):
        remaining = []
        for core, until in self.retired:
            if time.monotonic() >= until:
                core.stop()
            else:
                remaining.append((core, until))
        self.retired = remaining

    def stop(self):
        if self.active:
            self.active.stop()
        for core, _ in self.retired:
            core.stop()


def load_config(path):
    config = json.loads(Path(path).read_text())
    defaults = {'sing_box_bin': '/usr/local/bin/sing-box', 'state_dir': '/home/ubuntu/opencode2api-proxy-state',
                'proxy_file': '/home/ubuntu/proxies_working.txt', 'admin_url': 'http://127.0.0.1:8081',
                'admin_username': 'admin', 'refresh_seconds': 600, 'check_timeout_seconds': 8,
                'check_workers': 20, 'max_candidates': 200, 'max_active_nodes': 40,
                'candidate_port': 23000, 'production_ports': [24000, 25000], 'drain_seconds': 125,
                'check_url': 'https://opencode.ai/zen/v1/models', 'github_discovery': True}
    defaults.update(config)
    for field in ('refresh_seconds', 'check_timeout_seconds', 'check_workers', 'max_candidates', 'max_active_nodes'):
        if defaults[field] < 1:
            raise ValueError(field + ' must be positive')
    if defaults['max_active_nodes'] > defaults['max_candidates']:
        raise ValueError('max_active_nodes cannot exceed max_candidates')
    if 'admin_password' not in defaults:
        raise ValueError('admin_password is required')
    return defaults


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    config = load_config(args.config)
    Path(config['state_dir']).mkdir(parents=True, exist_ok=True)
    manager = Manager(config)
    for name, attr in (('health.json', 'health'), ('sources.json', 'cache')):
        path = Path(config['state_dir']) / name
        if path.exists():
            setattr(manager, attr, json.loads(path.read_text()))
    saved = Path(config['state_dir']) / 'active.json'
    if saved.exists():
        active = json.loads(saved.read_text())
        manager.active = Core(config, active['nodes'], active['port'])
        manager.active.start()
        # 网关在机器重启后可能已将本地未启动端口标记失败，恢复后重新加载。
        try:
            reload_gateway(config)
        except OSError:
            log.warning('initial gateway reload unavailable; next cycle will retry')
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: manager.stop_event.set())
    try:
        while not manager.stop_event.is_set():
            try:
                manager.cycle()
            except Exception:
                log.exception('refresh failed; existing pool retained')
            manager.stop_event.wait(config['refresh_seconds'])
    finally:
        manager.stop()


if __name__ == '__main__':
    main()
