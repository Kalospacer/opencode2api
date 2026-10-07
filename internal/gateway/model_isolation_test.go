package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

func TestAnonymousModel503DoesNotDisableOtherModels(t *testing.T) {
	calls := 0
	g := backendTestGateway(t, 1, 1, backendRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("route reachable"))}, nil
		}
		calls++
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["model"] == "exo-free" {
			return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"Endpoint is unavailable"}`))}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"message":"hello"}`))}, nil
	}))
	ids := identity.RequestIDs{Session: identity.CanonicalSessionID("model-isolation"), Request: "req_model_isolation", Project: "prj_test"}
	for _, model := range []string{"exo-free", "space-bunny-free"} {
		route := models.Route{ID: model, Tier: config.TierZen, Protocol: wire.Chat, Anonymous: true}
		body := []byte(`{"model":"` + model + `","stream":true}`)
		resp, _, err := g.doUpstream(context.Background(), route, map[config.Tier][]byte{config.TierZen: body}, ids)
		if err != nil || resp == nil {
			t.Fatalf("model %s blocked after another model failed: %v", model, err)
		}
		resp.Body.Close()
		want := 200
		if model == "exo-free" {
			want = 503
		}
		if resp.StatusCode != want {
			t.Fatalf("model %s status=%d want=%d", model, resp.StatusCode, want)
		}
	}
	if calls != 2 {
		t.Fatalf("independent model request did not reach upstream: calls=%d", calls)
	}
}

func TestAnonymousCooldownSeparatesServerErrorAndProxyFailure(t *testing.T) {
	pool := &anonymousPool{cooldown: time.Second}
	for _, status := range []int{500, 502, 503, 504} {
		node := &anonymousNode{}
		pool.MarkFailure(node, &http.Response{StatusCode: status, Header: http.Header{}}, nil)
		if node.cooldownUntil.Load() != 0 || node.failures.Load() != 0 {
			t.Fatalf("upstream status %d globally cooled the proxy", status)
		}
	}
	for _, status := range []int{401, 403, 429} {
		node := &anonymousNode{}
		pool.MarkFailure(node, &http.Response{StatusCode: status, Header: http.Header{}}, nil)
		if node.cooldownUntil.Load() <= time.Now().UnixNano() {
			t.Fatalf("status %d did not preserve cooldown", status)
		}
	}
	node := &anonymousNode{}
	pool.MarkFailure(node, nil, errors.New("transport failure"))
	if node.cooldownUntil.Load() <= time.Now().UnixNano() {
		t.Fatal("transport failure did not preserve cooldown")
	}
}
