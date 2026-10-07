package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

func TestPeekFirstSSEDataID(t *testing.T) {
	cases := []struct {
		name, stream, id string
		wantError        bool
	}{
		{"normal", "data: {\"id\":\"msg_good\"}\n\ndata: [DONE]\n\n", "msg_good", false},
		{"no id first", "data: {\"choices\":[]}\n\ndata: {\"id\":\"msg_good\"}\n\n", "msg_good", false},
		{"heartbeat CRLF", ": alive\r\n\r\nevent: chunk\r\ndata: {\"id\":\"resp_good\"}\r\n\r\n", "resp_good", false},
		{"multiline", "data: {\n data: ignored\ndata: \"id\":\"msg_good\"}\n\n", "msg_good", false},
		{"done without id", "data: [DONE]\n\n", "", true},
		{"truncated", "data: {\"id\":\"msg_good\"}\n", "", true},
		{"unknown prefix", "data: {\"id\":\"unknown_good\"}\n\n", "unknown_good", false},
		{"stream error", "data: {\"error\":{\"message\":\"failed\"}}\n\n", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, id, err := peekFirstSSEDataID(&http.Response{Body: io.NopCloser(strings.NewReader(tc.stream))})
			defer body.Close()
			if (err != nil) != tc.wantError || id != tc.id {
				t.Fatalf("id=%q err=%v", id, err)
			}
			all, readErr := io.ReadAll(body)
			if readErr != nil || string(all) != tc.stream {
				t.Fatalf("stream bytes changed: %v", readErr)
			}
		})
	}
}

type backendRoundTripper func(*http.Request) (*http.Response, error)

func (f backendRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// trackedBackendBody 用来确认错后端关闭前没有继续读取剩余生成内容。
type trackedBackendBody struct {
	first    string
	closed   bool
	tailRead bool
}

func (b *trackedBackendBody) Read(p []byte) (int, error) {
	if b.first != "" {
		n := copy(p, b.first)
		b.first = b.first[n:]
		return n, nil
	}
	b.tailRead = true
	return 0, io.EOF
}
func (b *trackedBackendBody) Close() error { b.closed = true; return nil }

func backendTestGateway(t *testing.T, count, maxAttempts int, rt http.RoundTripper) *Gateway {
	t.Helper()
	cfg := config.Config{
		Anonymous: true, Proxies: []string{"direct"},
		Retry:  config.RetryConfig{MaxAttempts: maxAttempts, TimeoutSeconds: 5},
		Models: config.ModelsConfig{RefreshSeconds: 300}, Upstream: config.UpstreamConfig{Zen: "http://upstream.invalid", Go: "http://upstream.invalid"},
		Performance: config.PerformanceConfig{ConnectTimeoutSeconds: 1, MaxIdleConns: 4, MaxIdleConnsPerHost: 4, IdleConnTimeoutSeconds: 1, FailureCooldownSeconds: 1},
	}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), telemetry.NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	g.transports.items = nil
	for i := 0; i < count; i++ {
		p := &proxyTransport{index: i, name: "http://test-proxy.invalid", client: &http.Client{Transport: rt}}
		p.healthy.Store(true)
		g.transports.items = append(g.transports.items, p)
	}
	g.anonymous = newAnonymousPool(true, g.transports, time.Second)
	return g
}

func backendTestRoute(prefix string) models.Route {
	return models.Route{ID: "exo-free-claude", UpstreamModel: "exo-free", BackendSelector: prefix, Tier: config.TierZen,
		Protocol: wire.Chat, Anonymous: true, Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Chat}}
}

func backendTestCall(g *Gateway, route models.Route) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(WithDiagnosticRequest(context.Background()), 3*time.Second)
	defer cancel()
	resp, _, err := g.doUpstream(ctx, route, map[config.Tier][]byte{config.TierZen: []byte(`{"model":"exo-free","stream":true}`)},
		identity.RequestIDs{Session: identity.CanonicalSessionID("backend-test"), Request: "req_local_trace", Project: "prj_test"})
	return resp, err
}

func TestBackendMismatchClosesImmediatelyAndRerolls(t *testing.T) {
	wrong := &trackedBackendBody{first: "data: {\"id\":\"resp_wrong\"}\n\n"}
	var requests, sessions []string
	calls := 0
	g := backendTestGateway(t, 1, 3, backendRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		requests = append(requests, r.Header.Get("x-opencode-request"))
		sessions = append(sessions, r.Header.Get("x-opencode-session"))
		if calls == 1 {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: wrong}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"id\":\"msg_good\"}\n\ndata: [DONE]\n\n"))}, nil
	}))
	resp, err := backendTestCall(g, backendTestRoute("msg_"))
	if err != nil || resp == nil {
		t.Fatalf("response=%v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if calls != 2 || !wrong.closed || wrong.tailRead {
		t.Fatalf("calls=%d closed=%v tailRead=%v", calls, wrong.closed, wrong.tailRead)
	}
	if requests[0] == requests[1] || sessions[0] == sessions[1] {
		t.Fatal("upstream request/session not renewed")
	}
	rows := g.monitor.Snapshot().Upstream.Recent
	if len(rows) != 2 || rows[0].RequestID != "req_local_trace" || rows[1].RequestID != "req_local_trace" || rows[1].Attempt != 2 {
		t.Fatalf("trace/attempt numbering changed: %+v", rows)
	}
}

func TestBackendSelectionSharesActualAttemptBudget(t *testing.T) {
	calls := 0
	g := backendTestGateway(t, 8, 3, backendRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 || calls == 3 {
			return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"unavailable"}`))}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"id\":\"resp_wrong\"}\n\n"))}, nil
	}))
	resp, _ := backendTestCall(g, backendTestRoute("msg_"))
	if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			t.Fatal("wrong backend escaped")
		}
	}
	if calls != 3 {
		t.Fatalf("configured 3 attempts but sent %d", calls)
	}
}

func TestBackendSelectionRejectsUnverifiedResponses(t *testing.T) {
	for _, contentType := range []string{"application/json", "text/event-stream"} {
		t.Run(contentType, func(t *testing.T) {
			calls := 0
			g := backendTestGateway(t, 1, 3, backendRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				data := `{"id":"resp_wrong"}`
				if contentType == "text/event-stream" {
					data = "data: {\"id\":\"resp_wrong\"}\n\n"
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(data))}, nil
			}))
			resp, err := backendTestCall(g, backendTestRoute("msg_"))
			if resp != nil || !errors.Is(err, errBackendSelection) || calls != 3 {
				t.Fatalf("calls=%d response=%v err=%v", calls, resp, err)
			}
		})
	}
}

func TestAnonymousAttemptLimitAnd429Rotation(t *testing.T) {
	var nodes []int
	calls := 0
	g := backendTestGateway(t, 8, 3, nil)
	for i, p := range g.transports.items {
		idx := i
		p.client.Transport = backendRoundTripper(func(r *http.Request) (*http.Response, error) {
			calls++
			nodes = append(nodes, idx)
			return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"30"}}, Body: io.NopCloser(strings.NewReader(`{"error":"rate limited"}`))}, nil
		})
	}
	resp, err := backendTestCall(g, backendTestRoute(""))
	if err != nil || resp == nil || resp.StatusCode != 429 || calls != 3 {
		t.Fatalf("calls=%d response=%v err=%v", calls, resp, err)
	}
	defer resp.Body.Close()
	if nodes[0] == nodes[1] || nodes[1] == nodes[2] {
		t.Fatalf("429 did not rotate nodes: %v", nodes)
	}
}

func TestPrepareVirtualModelBody(t *testing.T) {
	g := backendTestGateway(t, 1, 3, nil)
	input := map[string]any{"model": "exo-free-claude", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "stream": true}
	bodies, err := g.prepareRouteBodies(wire.Chat, backendTestRoute("msg_"), input)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(bodies[config.TierZen], &body); err != nil {
		t.Fatal(err)
	}
	if body["model"] != "exo-free" || input["model"] != "exo-free-claude" {
		t.Fatalf("upstream/client model changed incorrectly: %v / %v", body["model"], input["model"])
	}
}

func TestSelectedKeyBackendCheckDoesNotRetry(t *testing.T) {
	calls := 0
	g := backendTestGateway(t, 1, 5, backendRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"id\":\"resp_wrong\"}\n\n"))}, nil
	}))
	var err error
	g.zenNodes, err = newNodePool([]string{"selected-test-key"}, g.transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	route := backendTestRoute("msg_")
	route.Anonymous = false
	route.KeyTiers = []config.Tier{config.TierZen}
	ctx := WithDebugKeyOverride(context.Background(), DebugKeyOverride{Tier: config.TierZen, KeyID: config.Fingerprint("selected-test-key")})
	resp, _, err := g.doUpstream(ctx, route, map[config.Tier][]byte{config.TierZen: []byte(`{"model":"exo-free","stream":true}`)}, identity.RequestIDs{Session: identity.CanonicalSessionID("selected-test"), Request: "req_selected", Project: "prj_test"})
	if resp != nil || !errors.Is(err, errBackendSelection) || calls != 1 {
		t.Fatalf("selected key sent %d attempts, response=%v err=%v", calls, resp, err)
	}
	if g.zenNodes.nodes[0].failures.Load() != 0 || !g.transports.items[0].healthy.Load() {
		t.Fatal("diagnostic changed production pool health")
	}
}

func TestCanceledBackendSelectionDoesNotSend(t *testing.T) {
	calls := 0
	g := backendTestGateway(t, 1, 3, backendRoundTripper(func(r *http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not send") }))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := g.doUpstream(ctx, backendTestRoute("msg_"), nil, identity.RequestIDs{})
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("canceled call sent %d attempts: %v", calls, err)
	}
}
