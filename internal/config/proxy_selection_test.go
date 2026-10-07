package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProxySelectionConfiguration(t *testing.T) {
	for _, strategy := range []string{"", "affinity", "ordered", "invalid"} {
		t.Run(strategy, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			body := `{"server_keys":["test-local-key"],"anonymous":true,"performance":{"proxy_selection":"` + strategy + `"}}`
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if strategy == "invalid" {
				if err == nil {
					t.Fatal("invalid proxy strategy accepted")
				}
				return
			}
			if err != nil || cfg.Performance.ProxySelection != strategy {
				t.Fatalf("strategy=%q err=%v", cfg.Performance.ProxySelection, err)
			}
		})
	}
}
