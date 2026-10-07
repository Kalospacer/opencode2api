package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDirectFirstWithProxyFileAndReload(t *testing.T) {
	dir := t.TempDir()
	proxyFile := filepath.Join(dir, "proxies.txt")
	if err := os.WriteFile(proxyFile, []byte("http://first.invalid:8080\ndirect\nhttp://second.invalid:8080\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	body := `{"server_keys":["local-test-key"],"anonymous":true,"proxies":["http://second.invalid:8080","direct"],"proxyfile":"proxies.txt"}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"direct", "http://second.invalid:8080", "http://first.invalid:8080"}
	if got := cfg.RuntimeProxies(); !reflect.DeepEqual(got, want) {
		t.Fatalf("proxy order=%v want=%v", got, want)
	}
	if err := os.WriteFile(proxyFile, []byte("http://new.invalid:8080\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Normalize(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"direct", "http://second.invalid:8080", "http://new.invalid:8080"}
	if got := cfg.RuntimeProxies(); !reflect.DeepEqual(got, want) {
		t.Fatalf("reload dropped primary or reordered proxies: %v", got)
	}
}

func TestDirectIsDefaultEvenWithoutNormalization(t *testing.T) {
	for _, cfg := range []Config{{}, {Proxies: []string{"http://backup.invalid:8080"}}} {
		if got := cfg.RuntimeProxies(); len(got) == 0 || got[0] != "direct" {
			t.Fatalf("missing default direct node: %v", got)
		}
	}
}
