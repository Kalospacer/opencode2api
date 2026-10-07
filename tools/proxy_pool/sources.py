"""发现公开订阅来源，下载有大小上限并验证 HTTPS 证书。"""
import json
import logging
import time
from urllib.parse import quote
from urllib.request import Request, urlopen
from nodes import parse_subscription, node_id

log = logging.getLogger('proxy-pool')
DEFAULT_SOURCES = [
    'https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub',
    'https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt',
]


def fetch(url, timeout=15):
    with urlopen(Request(url, headers={'User-Agent': 'opencode2api-proxy-pool/1.0'}), timeout=timeout) as response:
        body = response.read((2 << 20) + 1)
        if len(body) > 2 << 20:
            raise ValueError('subscription response exceeds 2 MiB')
        return body.decode('utf-8-sig')


def discover(cache, enabled=True):
    if not enabled:
        return []
    if time.time() - cache.get('discovered_at', 0) < 3600:
        return cache.get('discovered_sources', [])
    result = []
    try:
        search = json.loads(fetch('https://api.github.com/search/repositories?q=' + quote('free v2ray nodes') + '&sort=updated&per_page=5'))
        for repo in search.get('items', []):
            if repo.get('archived') or repo.get('disabled'):
                continue
            name, branch = repo['full_name'], repo['default_branch']
            try:
                tree = json.loads(fetch(f'https://api.github.com/repos/{name}/git/trees/{quote(branch, safe="")}?recursive=1'))
                paths = [item['path'] for item in tree.get('tree', []) if item.get('type') == 'blob' and item.get('size', 0) < 2 << 20]
                paths = [path for path in paths if path.lower().endswith('.txt') and any(word in path.lower() for word in ('v2ray', 'base64', 'sub', 'nodes', 'list'))]
                for path in sorted(paths, key=lambda p: (p.count('/'), len(p)))[:2]:
                    result.append(f'https://raw.githubusercontent.com/{name}/{quote(branch, safe="")}/{quote(path, safe="/")}')
            except (OSError, ValueError, KeyError) as err:
                log.warning('repository discovery skipped %s (%s)', name, type(err).__name__)
        cache['discovered_at'] = time.time()
        cache['discovered_sources'] = result
    except (OSError, ValueError, KeyError) as err:
        log.warning('GitHub discovery failed; keeping known sources (%s)', type(err).__name__)
        # GitHub 限流时不每轮重新访问 API，仍保留上轮来源。
        cache['discovered_at'] = time.time()
        result = cache.get('discovered_sources', [])
    return result


def fetch_nodes(config, cache):
    urls = list(dict.fromkeys(config.get('subscription_urls', DEFAULT_SOURCES) + discover(cache, config.get('github_discovery', True))))
    nodes = {}
    for url in urls:
        try:
            parsed = parse_subscription(fetch(url))
            for node in parsed:
                nodes[node_id(node)] = node
            log.info('subscription parsed: %d nodes from %s', len(parsed), url)
        except (OSError, ValueError, KeyError) as err:
            log.warning('subscription unavailable: %s (%s)', url, type(err).__name__)
    log.info('discovered %d distinct node configurations', len(nodes))
    return list(nodes.values())
