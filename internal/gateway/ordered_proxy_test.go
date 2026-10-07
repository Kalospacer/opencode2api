package gateway

import (
	"net/http"
	"testing"
	"time"
)

func TestOrderedAnonymousProxySelectionAndCooldown(t *testing.T) {
	transports := &transportPool{ordered: true}
	for i := 0; i < 3; i++ {
		proxy := &proxyTransport{index: i}
		proxy.healthy.Store(true)
		transports.items = append(transports.items, proxy)
	}
	pool := newAnonymousPool(true, transports, time.Second)
	for _, session := range []string{"session-a", "session-b", ""} {
		cursor := pool.CursorFor(session)
		if node := cursor.Next(); node == nil || node.proxy.index != 0 {
			t.Fatal("ordered mode did not prioritize fastest node")
		}
	}
	pool.MarkFailure(pool.nodes[0], &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"30"}}}, nil)
	cursor := pool.CursorFor("session-a")
	if node := cursor.Next(); node == nil || node.proxy.index != 1 {
		t.Fatal("ordered mode reused the rate-limited fastest node")
	}
	transports.items[1].healthy.Store(false)
	cursor = pool.CursorFor("session-b")
	if node := cursor.Next(); node == nil || node.proxy.index != 2 {
		t.Fatal("ordered mode used unhealthy node")
	}
	pool.nodes[0].cooldownUntil.Store(0)
	cursor = pool.CursorFor("session-a")
	if node := cursor.Next(); node == nil || node.proxy.index != 0 {
		t.Fatal("recovered fastest node was not preferred")
	}
}

func TestDefaultAnonymousProxySelectionStillUsesAffinity(t *testing.T) {
	transports := &transportPool{}
	for i := 0; i < 10; i++ {
		proxy := &proxyTransport{index: i}
		proxy.healthy.Store(true)
		transports.items = append(transports.items, proxy)
	}
	pool := newAnonymousPool(true, transports, time.Second)
	a, b := pool.CursorFor("existing-session"), pool.CursorFor("existing-session")
	if a.Next().proxy.index != b.Next().proxy.index {
		t.Fatal("default stable session affinity changed")
	}
}
