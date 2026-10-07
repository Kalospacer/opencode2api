package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"time"
)

// directState 在同一进程的热重载实例之间共享，避免代理更新重置直连限流。
type directState struct {
	healthy       atomic.Bool
	failures      atomic.Uint32
	cooldownUntil atomic.Int64
}

func (p *proxyTransport) isHealthy() bool {
	if p.direct != nil {
		return p.direct.healthy.Load()
	}
	return p.healthy.Load()
}

func (p *proxyTransport) swapHealthy(value bool) bool {
	if p.direct != nil {
		return p.direct.healthy.Swap(value)
	}
	return p.healthy.Swap(value)
}

func (p *proxyTransport) available() bool {
	now := time.Now().UnixNano()
	return p.isHealthy() && p.rateLimitUntil.Load() <= now && (p.direct == nil || p.direct.cooldownUntil.Load() <= now)
}

func (p *transportPool) directNode() *proxyTransport {
	for _, proxy := range p.items {
		if proxy.name == "direct" {
			return proxy
		}
	}
	return nil
}

func (g *Gateway) inheritDirectState(previous *Gateway) {
	old, next := previous.transports.directNode(), g.transports.directNode()
	if old != nil && next != nil && old.direct != nil {
		next.direct = old.direct
	}
}

func (g *Gateway) observeDirectResult(ctx context.Context, proxy *proxyTransport, resp *http.Response, err error) {
	if isDiagnosticRequest(ctx) || proxy.direct == nil {
		return
	}
	if directConnectionFailure(err) && !isProxyFailure(err) {
		g.rebindFailedProxy(proxy)
		g.verifyProxyAfterError(ctx, proxy, upstreamStatus(resp))
	} else {
		g.syncProxyResult(ctx, proxy, upstreamStatus(resp), err)
	}
	if err != nil || resp == nil {
		return
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		state := proxy.direct
		failures := state.failures.Add(1)
		delay := time.Duration(g.cfg.Performance.FailureCooldownSeconds) * time.Second * time.Duration(1<<min(failures-1, 3))
		if after := parseRetryAfter(resp.Header.Get("Retry-After")); after > delay {
			delay = after
		}
		until := time.Now().Add(delay).UnixNano()
		// 并发请求中短冷却不能覆盖已经收到的更长 Retry-After。
		for {
			previous := state.cooldownUntil.Load()
			if previous >= until || state.cooldownUntil.CompareAndSwap(previous, until) {
				break
			}
		}
		g.logger.Warn("direct route rate limited; using proxy fallback", "component", "proxy", "event", "direct_rate_limited",
			"cooldown_until", time.Unix(0, state.cooldownUntil.Load()).UTC(), "retry_after_seconds", delay.Seconds())
	} else if resp.StatusCode/100 == 2 && proxy.direct.cooldownUntil.Load() <= time.Now().UnixNano() {
		// 已在途的成功请求不能提前清掉另一个请求产生的 429 冷却。
		proxy.direct.failures.Store(0)
	}
}

// directResponseEndsLane 区分直连 IP 限流、真实连接失败与模型或参数错误。
func directResponseEndsLane(proxy *proxyTransport, resp *http.Response, err error) bool {
	if proxy.direct == nil {
		return false
	}
	if err != nil {
		return !directConnectionFailure(err)
	}
	return resp == nil || resp.StatusCode != http.StatusTooManyRequests
}

// coolFallbackProxy 对认证通道的代理 429 冷却出口，不把它误判为 Key 失效。
func (g *Gateway) coolFallbackProxy(proxy *proxyTransport, resp *http.Response) {
	delay := time.Duration(g.cfg.Performance.FailureCooldownSeconds) * time.Second
	if after := parseRetryAfter(resp.Header.Get("Retry-After")); after > delay {
		delay = after
	}
	until := time.Now().Add(delay).UnixNano()
	for {
		previous := proxy.rateLimitUntil.Load()
		if previous >= until || proxy.rateLimitUntil.CompareAndSwap(previous, until) {
			break
		}
	}
	proxy.swapHealthy(true)
}

// directConnectionFailure 只识别连接类故障，不把证书或请求协议错误当作换 IP 理由。
func directConnectionFailure(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if isProxyFailure(err) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return true
	}
	var network *net.OpError
	return errors.As(err, &network) && (network.Op == "dial" || network.Op == "read" || network.Op == "write")
}
