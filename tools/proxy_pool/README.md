# 服务器端自动代理池

此工具仅在部署服务器运行，不需要在客户端机器安装或运行节点程序。它与 opencode2api 独立，由 Python 3、curl 和 sing-box 组成；不执行订阅中的脚本。

## 工作流程

1. 从配置的公开订阅拉取节点，并可通过 GitHub API 发现新的公开节点仓库。
2. 按完整节点配置去重，保留协议、认证和传输参数不同的节点。
3. 活动节点参加每轮复测，剩余候选按轮次覆盖；逐个校验 sing-box 配置。
4. 在独立测试端口启动候选节点，对 `https://opencode.ai/zen/v1/models` 测量首个响应字节时间。
5. 连接失败的节点记录到 `health.json`，不发布到新活动池；下轮复测成功可以恢复。成功节点按平滑后的延迟排序，保留配置数量内最快的一组。
6. 新活动池在另一组端口启动后原子写入代理文件，再通过管理 API 登录并携带 JSON 返回的 CSRF Token 重载网关。
7. 重载失败恢复旧代理文件，旧进程继续服务；成功后旧池保留到排空期限结束，保护已经开始的请求。

测速仅说明模型目录的连接性能，不代表推理吞吐、住宅 IP 属性或某个模型的可用性。503、429 仍可能由上游返回；本工具不伪装或绕过服务端模型权限。

## 前置依赖与运行

本次部署验证使用 Python 3、curl、sing-box 1.10.3。sing-box 的配置格式跨版本可能变化，更换版本前应重新运行定向测试和配置校验。

```bash
python3 tools/proxy_pool/manager.py --config /path/to/proxy-pool-config.json
```

配置示例（管理员密码请自行设置，配置文件权限应为 `0600`）：

```json
{
  "admin_url": "http://127.0.0.1:8081",
  "admin_username": "admin",
  "admin_password": "REPLACE_WITH_ADMIN_PASSWORD",
  "proxy_file": "/home/ubuntu/proxies_working.txt",
  "state_dir": "/home/ubuntu/opencode2api-proxy-state",
  "refresh_seconds": 600,
  "check_workers": 20,
  "check_timeout_seconds": 8,
  "max_candidates": 200,
  "max_active_nodes": 40,
  "candidate_port": 23000,
  "production_ports": [24000, 25000],
  "drain_seconds": 125,
  "github_discovery": true
}
```

- `drain_seconds` 应大于等于网关请求总超时加少量余量。
- 三组端口及各自连续节点端口范围不得重叠，也不得占用其他服务端口。
- 节点只监听 `127.0.0.1`，不会开放公网代理。
- 可用池为空时不发布空文件，保留旧后备池；网关默认直连节点与后备文件独立。
- `subscription_urls` 可替换默认来源；仅支持 URI 列表及 Base64 URI 订阅，不支持 Clash YAML。
- 不支持的协议、传输和插件会被跳过，不降级成直连。
- 活动节点、测速结果和来源缓存保存在状态目录；其中包含节点凭据，应保持目录权限 `0700`。

网关配置需指向上述代理文件，并启用顺序优先：

```json
{
  "proxies": [],
  "proxyfile": "/home/ubuntu/proxies_working.txt",
  "performance": { "proxy_selection": "ordered" }
}
```

网关默认直连节点固定为优先级 0，直连正常时不使用订阅代理。直连收到 429 冷却或发生真实连接故障后才进入后备池；冷却到期自动恢复直连优先。`ordered` 模式在匿名和认证后备选择中优先使用文件前面的最快可用代理，冷却或不健康节点会被跳过。默认空值或 `affinity` 对后备池保留亲和性行为。自动热重载不会重置直连冷却。

## 本次服务器部署位置

- 管理进程：`opencode2api-proxy-pool.service`
- 网关：`opencode2api.service`
- 配置：`/home/ubuntu/opencode2api-proxy-config.json`
- 源码：`/home/ubuntu/opencode2api-deploy-v3/opencode2api/tools/proxy_pool/`

```bash
systemctl status opencode2api-proxy-pool.service --no-pager
journalctl -u opencode2api-proxy-pool.service -n 50 --no-pager
```

## 定向测试（在服务器执行）

```bash
cd tools/proxy_pool
python3 -m unittest -v test_pool
```

测试覆盖订阅解析、节点身份去重、最快排序、失败节点状态、单节点保留、无直连回退、成功发布与热重载失败回滚。网关侧还有 Go 回归测试验证顺序优先、429 冷却换节点及默认亲和性兼容。
