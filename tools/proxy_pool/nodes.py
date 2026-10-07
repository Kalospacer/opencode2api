"""订阅节点解析；仅生成 sing-box 配置，不执行订阅中的任何内容。"""
import base64
import json
from urllib.parse import urlsplit, parse_qs, unquote


def decode64(value):
    return base64.urlsafe_b64decode(value.strip() + '=' * (-len(value.strip()) % 4)).decode()


def transport(kind, path='', host='', service=''):
    if kind in ('', 'tcp', 'none'):
        return {}
    if kind == 'ws':
        value = {'type': 'ws', 'path': path or '/'}
        if host:
            value['headers'] = {'Host': host}
        return {'transport': value}
    if kind == 'grpc':
        return {'transport': {'type': 'grpc', 'service_name': service}}
    raise ValueError('unsupported transport')


def parse_link(link):
    if link.startswith('vmess://'):
        value = json.loads(decode64(link[8:]))
        out = {'type': 'vmess', 'server': value['add'], 'server_port': int(value['port']),
               'uuid': value['id'], 'security': 'auto', 'alter_id': int(value.get('aid', 0))}
        if value.get('tls') == 'tls':
            out['tls'] = {'enabled': True, 'server_name': value.get('sni') or value.get('host') or value['add']}
        out.update(transport(value.get('net', 'tcp'), value.get('path'), value.get('host')))
        return out
    if link.startswith('ss://'):
        raw = link[5:].split('#', 1)[0]
        if '?' in raw:
            raise ValueError('Shadowsocks plugins are not supported')
        if '@' not in raw:
            raw = decode64(raw)
        auth, endpoint = raw.rsplit('@', 1)
        if ':' not in auth:
            auth = decode64(auth)
        method, password = unquote(auth).split(':', 1)
        address = urlsplit('ss://' + endpoint.rstrip('/'))
        return {'type': 'shadowsocks', 'server': address.hostname, 'server_port': address.port,
                'method': method, 'password': password}
    value = urlsplit(link)
    scheme = {'hy2': 'hysteria2'}.get(value.scheme, value.scheme)
    if scheme not in ('vless', 'trojan', 'hysteria2'):
        raise ValueError('unsupported protocol')
    params = {k: v[-1] for k, v in parse_qs(value.query).items()}
    out = {'type': scheme, 'server': value.hostname, 'server_port': value.port or (443 if scheme == 'hysteria2' else None)}
    credential = unquote(value.username or '')
    if value.password:
        credential += ':' + unquote(value.password)
    if scheme == 'vless':
        out['uuid'] = credential
        if params.get('flow'):
            out['flow'] = params['flow']
    else:
        out['password'] = credential
    security = params.get('security', 'tls' if scheme != 'vless' else '')
    if security in ('tls', 'reality') or scheme == 'hysteria2':
        tls = {'enabled': True, 'server_name': params.get('sni') or params.get('peer') or value.hostname}
        if params.get('alpn'):
            tls['alpn'] = params['alpn'].split(',')
        if params.get('insecure') == '1' or params.get('allowInsecure') == '1':
            tls['insecure'] = True
        if security == 'reality':
            tls['utls'] = {'enabled': True, 'fingerprint': params.get('fp') or 'chrome'}
            tls['reality'] = {'enabled': True, 'public_key': params['pbk'], 'short_id': params.get('sid', '')}
        out['tls'] = tls
    if scheme != 'hysteria2':
        out.update(transport(params.get('type', 'tcp'), params.get('path'), params.get('host'), params.get('serviceName', '')))
    elif params.get('obfs') == 'salamander':
        out['obfs'] = {'type': 'salamander', 'password': params.get('obfs-password', '')}
    if not out['server'] or not out['server_port'] or not credential:
        raise ValueError('missing endpoint or credentials')
    return out


def parse_subscription(content):
    if '://' not in content:
        try:
            content = decode64(content)
        except (ValueError, UnicodeError):
            return []
    result = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        try:
            result.append(parse_link(line))
        except (ValueError, KeyError, TypeError, UnicodeError):
            continue
    return result


def node_id(node):
    import hashlib
    # 凭证、协议及传输配置不同的节点不能只按 server:port 合并。
    return hashlib.sha256(json.dumps(node, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def build_config(nodes, start_port):
    return {
        'log': {'level': 'error'},
        'inbounds': [{'type': 'http', 'tag': f'in-{i}', 'listen': '127.0.0.1', 'listen_port': start_port + i} for i in range(len(nodes))],
        'outbounds': [{**node, 'tag': f'out-{i}'} for i, node in enumerate(nodes)] + [{'type': 'block', 'tag': 'block'}],
        'route': {'rules': [{'inbound': [f'in-{i}'], 'outbound': f'out-{i}'} for i in range(len(nodes))], 'final': 'block'},
    }
