package gateway

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

func directTestGateway(t *testing.T, authenticated bool) *Gateway {
	t.Helper()
	cfg := config.Config{
		Anonymous: !authenticated, Proxies: []string{"http://backup-one.invalid:8080", "direct", "http://backup-two.invalid:8080"}, Prefer: config.TierZen,
		Upstream: config.UpstreamConfig{Zen: "http://upstream.invalid", Go: "http://upstream.invalid"},
		Retry:    config.RetryConfig{MaxAttempts: 3, TimeoutSeconds: 3}, Models: config.ModelsConfig{RefreshSeconds: 300},
		Performance: config.PerformanceConfig{MaxIdleConns: 4, MaxIdleConnsPerHost: 4, ConnectTimeoutSeconds: 1, IdleConnTimeoutSeconds: 1, FailureCooldownSeconds: 60, ProxySelection: "ordered"},
	}
	if authenticated {
		cfg.ZenKeys = []string{"upstream-test-key"}
	}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), telemetry.NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func directTestResponse(status int) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"message":"hello"}`))}
}

func directTestCall(g *Gateway, authenticated bool) (*http.Response, error) {
	route := models.Route{ID: "space-bunny-free", Tier: config.TierZen, Protocol: wire.Chat, Anonymous: !authenticated}
	if authenticated {
		route.KeyTiers = []config.Tier{config.TierZen}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, _, err := g.doUpstream(ctx, route, map[config.Tier][]byte{config.TierZen: []byte(`{"model":"space-bunny-free","stream":true}`)},
		identity.RequestIDs{Session: identity.CanonicalSessionID("direct-priority-test"), Request: "req_direct_priority", Project: "prj_test"})
	return resp, err
}

func directAssertStatus(t *testing.T, g *Gateway, authenticated bool, status int) {
	t.Helper()
	resp, err := directTestCall(g, authenticated)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil || resp == nil || resp.StatusCode != status {
		t.Fatalf("response=%v err=%v want=%d", resp, err, status)
	}
}

func TestDirectAlwaysWinsBothSelectionModes(t *testing.T) {
	for _, mode := range []string{"ordered", "affinity"} {
		g := directTestGateway(t, false)
		g.anonymous.ordered = mode == "ordered"
		if g.transports.items[0].name != "direct" {
			t.Fatal("direct was not assigned index zero")
		}
		for _, session := range []string{"a", "b", "c", ""} {
			cursor := g.anonymous.CursorFor(session)
			if node := cursor.Next(); node == nil || node.proxy.name != "direct" {
				t.Fatalf("%s mode selected backup before direct", mode)
			}
		}
	}
}

func TestDirect429UsesBackupUntilExpiry(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		g := directTestGateway(t, authenticated)
		var exits []string
		limitDirect := true
		for _, proxy := range g.transports.items {
			p := proxy
			p.client.Transport = backendRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet {
					return directTestResponse(200), nil
				}
				exits = append(exits, p.name)
				if p.direct != nil && limitDirect {
					response := directTestResponse(429)
					response.Header.Set("Retry-After", "120")
					return response, nil
				}
				return directTestResponse(200), nil
			})
		}
		directAssertStatus(t, g, authenticated, 200)
		if want := []string{"direct", "http://backup-one.invalid:8080"}; !reflect.DeepEqual(exits, want) {
			t.Fatalf("authenticated=%v exits=%v", authenticated, exits)
		}
		primary := g.transports.directNode()
		if time.Until(time.Unix(0, primary.direct.cooldownUntil.Load())) < 119*time.Second {
			t.Fatal("Retry-After was not honored")
		}
		if authenticated && g.zenNodes.nodes[0].failures.Load() != 0 {
			t.Fatal("direct IP limit penalized a working key")
		}
		exits = nil
		directAssertStatus(t, g, authenticated, 200)
		if len(exits) != 1 || exits[0] != "http://backup-one.invalid:8080" {
			t.Fatalf("cooling direct was retried: %v", exits)
		}
		limitDirect = false
		primary.direct.cooldownUntil.Store(time.Now().Add(-time.Second).UnixNano())
		exits = nil
		directAssertStatus(t, g, authenticated, 200)
		if len(exits) != 1 || exits[0] != "direct" {
			t.Fatalf("expired cooldown did not restore primary: %v", exits)
		}
	}
}

func TestDirectHTTPFailuresDoNotUseProxy(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		for _, status := range []int{400, 401, 403, 500, 503} {
			g := directTestGateway(t, authenticated)
			var exits []string
			for _, proxy := range g.transports.items {
				p := proxy
				p.client.Transport = backendRoundTripper(func(r *http.Request) (*http.Response, error) {
					exits = append(exits, p.name)
					return directTestResponse(status), nil
				})
			}
			directAssertStatus(t, g, authenticated, status)
			if !reflect.DeepEqual(exits, []string{"direct"}) {
				t.Fatalf("status=%d authenticated=%v used proxies: %v", status, authenticated, exits)
			}
			if !g.transports.directNode().available() {
				t.Fatalf("HTTP status=%d disabled primary", status)
			}
		}
	}
}

func TestDirectConnectionFailureUsesBackup(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		g := directTestGateway(t, authenticated)
		var exits []string
		for _, proxy := range g.transports.items {
			p := proxy
			p.client.Transport = backendRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost {
					exits = append(exits, p.name)
				}
				if p.direct != nil {
					return nil, syscall.ECONNREFUSED
				}
				return directTestResponse(200), nil
			})
		}
		directAssertStatus(t, g, authenticated, 200)
		if want := []string{"direct", "http://backup-one.invalid:8080"}; !reflect.DeepEqual(exits, want) {
			t.Fatalf("connection failure did not switch exits: %v", exits)
		}
	}
}

func TestFallback429RotatesWithoutInvalidatingKey(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		g := directTestGateway(t, authenticated)
		var exits []string
		for _, proxy := range g.transports.items {
			p := proxy
			p.client.Transport = backendRoundTripper(func(r *http.Request) (*http.Response, error) {
				exits = append(exits, p.name)
				if p.index < 2 {
					return directTestResponse(429), nil
				}
				return directTestResponse(200), nil
			})
		}
		directAssertStatus(t, g, authenticated, 200)
		if want := []string{"direct", "http://backup-one.invalid:8080", "http://backup-two.invalid:8080"}; !reflect.DeepEqual(exits, want) {
			t.Fatalf("fallback rate limit did not rotate: %v", exits)
		}
		if authenticated && g.zenNodes.nodes[0].failures.Load() != 0 {
			t.Fatal("exit rate limiting invalidated a key")
		}
		if len(exits) > g.cfg.Retry.MaxAttempts {
			t.Fatal("request attempt budget exceeded")
		}
	}
}

func TestDirectStateSurvivesReloadAndInflightSuccess(t *testing.T) {
	old := directTestGateway(t, false)
	response := directTestResponse(429)
	response.Header.Set("Retry-After", "120")
	defer response.Body.Close()
	old.observeDirectResult(context.Background(), old.transports.directNode(), response, nil)
	next := directTestGateway(t, false)
	next.inheritDirectState(old)
	primary := next.transports.directNode()
	if primary.direct != old.transports.directNode().direct || primary.available() {
		t.Fatal("reload did not share pending primary cooldown")
	}
	until := primary.direct.cooldownUntil.Load()
	success := directTestResponse(200)
	defer success.Body.Close()
	old.observeDirectResult(context.Background(), old.transports.directNode(), success, nil)
	if primary.direct.cooldownUntil.Load() != until {
		t.Fatal("older in-flight success cleared a later 429 cooldown")
	}
	cursor := next.anonymous.CursorFor("new-session")
	if node := cursor.Next(); node == nil || node.proxy.direct != nil {
		t.Fatal("reload retried cooling direct")
	}
	old.transports.directNode().swapHealthy(false)
	if primary.isHealthy() {
		t.Fatal("old in-flight route failure was not visible after reload")
	}
}

func TestShorterRateLimitDoesNotShortenPendingCooldown(t *testing.T) {
	g := directTestGateway(t, false)
	g.cfg.Performance.FailureCooldownSeconds = 1
	long := directTestResponse(429)
	long.Header.Set("Retry-After", "120")
	defer long.Body.Close()
	primary := g.transports.directNode()
	g.observeDirectResult(context.Background(), primary, long, nil)
	until := primary.direct.cooldownUntil.Load()
	short := directTestResponse(429)
	defer short.Body.Close()
	g.observeDirectResult(context.Background(), primary, short, nil)
	if primary.direct.cooldownUntil.Load() < until {
		t.Fatal("shorter response shortened an existing Retry-After")
	}
}

func TestAuthenticatedOrderedFallbackUsesFastestProxy(t *testing.T) {
	g := directTestGateway(t, true)
	primary := g.transports.directNode()
	primary.direct.cooldownUntil.Store(time.Now().Add(time.Minute).UnixNano())
	g.zenNodes.nodes[0].proxyIndex.Store(2)
	if got := g.zenNodes.Proxy(g.zenNodes.nodes[0]); got == nil || got.index != 1 {
		t.Fatal("authenticated ordered fallback ignored fastest available proxy")
	}
}

func TestDirectConnectionErrorClassification(t *testing.T) {
	for _, err := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, io.EOF, &net.DNSError{Name: "upstream.invalid", Err: "no such host"}} {
		if !directConnectionFailure(err) {
			t.Fatalf("real connection failure not recognized: %v", err)
		}
	}
	if directConnectionFailure(context.Canceled) {
		t.Fatal("client cancellation misclassified as direct failure")
	}
}
