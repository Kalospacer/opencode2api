package models

import (
	"testing"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
)

func TestVirtualExoModelsInheritRouteAndMetadata(t *testing.T) {
	c := NewCatalog(config.TierZen, nil)
	c.ReplaceWithCapabilities([]string{"exo-free", "other"}, nil,
		map[config.Tier]map[string]wire.Protocol{config.TierZen: {"exo-free": wire.Chat, "other": wire.Chat}}, nil,
		map[config.Tier]map[string]Metadata{config.TierZen: {"exo-free": {ContextWindow: 1000, ToolCall: true}}})
	routes, _ := c.AvailableModels(false, false, true)
	seen := map[string]bool{}
	for _, r := range routes {
		seen[r.ID] = true
	}
	for model, prefix := range map[string]string{"exo-free-claude": "msg_", "exo-free-gpt": "resp_"} {
		if !seen[model] || !c.Supported(model) || !c.IsFreeModel(model) {
			t.Fatalf("virtual model missing: %s", model)
		}
		r, err := c.Route(model, false, false, true)
		if err != nil || r.UpstreamModel != "exo-free" || r.BackendSelector != prefix || r.Protocol != wire.Chat {
			t.Fatalf("virtual route invalid: %+v %v", r, err)
		}
		md := c.MetadataForTier(model, config.TierZen)
		if md.ContextWindow != 1000 || !md.ToolCall {
			t.Fatalf("base metadata missing: %+v", md)
		}
		pinned, err := c.RouteForTier(model, config.TierZen, true, false)
		if err != nil || pinned.BackendSelector != prefix || pinned.UpstreamModel != "exo-free" {
			t.Fatalf("pinned route invalid: %+v %v", pinned, err)
		}
	}
	r, err := c.Route("exo-free", false, false, true)
	if err != nil || r.BackendSelector != "" || r.UpstreamModel != "" {
		t.Fatalf("base model behavior changed: %+v %v", r, err)
	}
}

func TestVirtualExoModelsDisappearWithBaseModel(t *testing.T) {
	c := NewCatalog(config.TierZen, nil)
	c.ReplaceWithCapabilities([]string{"other"}, nil,
		map[config.Tier]map[string]wire.Protocol{config.TierZen: {"other": wire.Chat}}, nil, nil)
	for _, model := range []string{"exo-free-claude", "exo-free-gpt"} {
		if c.Supported(model) {
			t.Fatalf("virtual model advertised without base: %s", model)
		}
		if _, err := c.Route(model, false, false, true); err == nil {
			t.Fatalf("virtual route accepted without base: %s", model)
		}
	}
}
