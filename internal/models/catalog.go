// Package models discovers model capabilities, pricing, and available routes.
package models

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
)

type Route struct {
	ID       string
	Tier     config.Tier
	Protocol wire.Protocol
	// Protocols is the native protocol for each possible upstream tier. Zen
	// and Go intentionally do not share one global protocol: OpenCode's
	// catalog currently exposes, for example, MiniMax through Chat on Zen and
	// Messages on Go. The request must therefore be re-encoded when a retry
	// crosses tiers.
	Protocols map[config.Tier]wire.Protocol
	Anonymous bool
	// KeyTiers is the ordered authenticated fallback plan. Anonymous requests
	// always start on Zen, then enter this list when the public credential does
	// not succeed.
	KeyTiers []config.Tier
	// BackendSelector 非空时要求上游响应的第一个 SSE data 块 id
	// 以此前缀开头，否则断开重试。仅对 exo-free 的虚拟子模型生效。
	BackendSelector string
	// UpstreamModel 是发往上游的实际模型名。虚拟子模型时与 ID 不同。
	UpstreamModel string
}

type RouteDiagnostic struct {
	Model                string                        `json:"model"`
	RequestedProtocol    wire.Protocol                 `json:"requested_protocol,omitempty"`
	NativeProtocol       wire.Protocol                 `json:"native_protocol"`
	NativeProtocols      map[config.Tier]wire.Protocol `json:"native_protocols,omitempty"`
	ProtocolSource       string                        `json:"protocol_source"`
	AvailableZen         bool                          `json:"available_zen"`
	AvailableGo          bool                          `json:"available_go"`
	Tier                 config.Tier                   `json:"tier,omitempty"`
	Anonymous            bool                          `json:"anonymous"`
	KeyID                string                        `json:"key_id,omitempty"`
	Channel              string                        `json:"channel,omitempty"`
	Attempts             int                           `json:"attempts,omitempty"`
	KeyTiers             []config.Tier                 `json:"key_tiers,omitempty"`
	AnonymousEligibility AnonymousDecision             `json:"anonymous_eligibility"`
	RouteError           string                        `json:"route_error,omitempty"`
}

type Catalog struct {
	mu        sync.RWMutex
	zen       map[string]bool
	goModels  map[string]bool
	protocols map[string]wire.Protocol
	// nativeProtocols is populated from OpenCode's public model capability
	// catalog. protocols remains the user-configured override map.
	nativeProtocols map[config.Tier]map[string]wire.Protocol
	unsupported     map[config.Tier]map[string]bool
	modelMeta       map[config.Tier]map[string]Metadata
	updatedAt       time.Time
	prefer          config.Tier
	pricing         *PricingStore
	cachePath       string
	cacheSource     string
	stale           bool
	refreshAfter    time.Duration
}

type CatalogSnapshot struct {
	Zen         int       `json:"zen"`
	Go          int       `json:"go"`
	Total       int       `json:"total"`
	Exposed     int       `json:"exposed"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
	CacheSource string    `json:"cache_source,omitempty"`
	Stale       bool      `json:"stale"`
}

func NewCatalog(prefer config.Tier, overrides map[string]string) *Catalog {
	protocols := make(map[string]wire.Protocol, len(overrides))
	for model, protocol := range overrides {
		protocols[model] = wire.Protocol(protocol)
	}
	return &Catalog{
		zen: map[string]bool{}, goModels: map[string]bool{}, protocols: protocols,
		nativeProtocols: map[config.Tier]map[string]wire.Protocol{config.TierZen: {}, config.TierGo: {}},
		unsupported:     map[config.Tier]map[string]bool{config.TierZen: {}, config.TierGo: {}}, prefer: prefer,
		cacheSource: "none",
	}
}

// SetPricingStore connects routing decisions to the shared pricing metadata.
func (c *Catalog) SetPricingStore(store *PricingStore) {
	c.mu.Lock()
	c.pricing = store
	c.mu.Unlock()
}

// MetadataSnapshot reports pricing freshness without exposing the store.
func (c *Catalog) MetadataSnapshot() MetadataSnapshot {
	c.mu.RLock()
	store := c.pricing
	c.mu.RUnlock()
	if store == nil {
		return MetadataSnapshot{}
	}
	return store.Snapshot()
}

func (c *Catalog) SetCachePath(path string) {
	c.mu.Lock()
	c.cachePath = path
	c.mu.Unlock()
}

func (c *Catalog) SetRefreshInterval(interval time.Duration) {
	c.mu.Lock()
	c.refreshAfter = interval
	c.mu.Unlock()
}

func (c *Catalog) Replace(zen, goModels []string) {
	c.ReplaceWithCapabilities(zen, goModels, nil, nil, nil)
}

func (c *Catalog) ReplaceWithCapabilities(zen, goModels []string, native map[config.Tier]map[string]wire.Protocol, unsupported map[config.Tier]map[string]bool, metadata map[config.Tier]map[string]Metadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if zen != nil {
		c.zen = toSet(zen)
	}
	if goModels != nil {
		c.goModels = toSet(goModels)
	}
	if native != nil {
		for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
			if protocols, ok := native[tier]; ok {
				c.nativeProtocols[tier] = cloneProtocols(protocols)
			}
		}
	}
	if unsupported != nil {
		for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
			if models, ok := unsupported[tier]; ok {
				c.unsupported[tier] = cloneBools(models)
			}
		}
	}
	if metadata != nil {
		c.modelMeta = cloneModelMeta(metadata)
	}
	c.updatedAt = time.Now().UTC()
	c.cacheSource = "live"
	c.stale = false
}

func (c *Catalog) CopyState(source *Catalog) {
	if source == nil {
		return
	}
	source.mu.RLock()
	zen := make(map[string]bool, len(source.zen))
	goModels := make(map[string]bool, len(source.goModels))
	for model, available := range source.zen {
		zen[model] = available
	}
	for model, available := range source.goModels {
		goModels[model] = available
	}
	native := map[config.Tier]map[string]wire.Protocol{config.TierZen: {}, config.TierGo: {}}
	unsupported := map[config.Tier]map[string]bool{config.TierZen: {}, config.TierGo: {}}
	for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
		for model, protocol := range source.nativeProtocols[tier] {
			native[tier][model] = protocol
		}
		for model, value := range source.unsupported[tier] {
			unsupported[tier][model] = value
		}
	}
	meta := cloneModelMeta(source.modelMeta)
	updatedAt := source.updatedAt
	cacheSource := source.cacheSource
	stale := source.stale
	source.mu.RUnlock()
	c.mu.Lock()
	c.zen, c.goModels, c.nativeProtocols, c.unsupported, c.updatedAt = zen, goModels, native, unsupported, updatedAt
	c.modelMeta = meta
	c.cacheSource, c.stale = cacheSource, stale
	c.mu.Unlock()
}

func (c *Catalog) Route(model string, hasZenKeys, hasGoKeys, hasAnonymous bool) (Route, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routeLocked(model, hasZenKeys, hasGoKeys, hasAnonymous)
}

func (c *Catalog) routeLocked(model string, hasZenKeys, hasGoKeys, hasAnonymous bool) (Route, error) {
	base, selector := splitVirtualModel(model)
	keyTiers := c.keyTierOrderLocked(base, hasZenKeys, hasGoKeys)
	// OpenCode's public credential is a Zen-only lane. Every free model starts
	// there, even if the current catalog only advertises it on Go: an upstream
	// rejection will move the request into the authenticated fallback plan.
	decision := c.anonymousDecision(base)
	if hasAnonymous && decision.Allowed && (c.protocols[base] != "" || !c.unsupported[config.TierZen][base]) &&
		(len(c.zen) == 0 && len(c.goModels) == 0 || c.zen[base] || c.goModels[base]) {
		protocols := c.protocolsForLocked(base, keyTiers, true)
		return Route{
			ID: model, Tier: config.TierZen, Protocol: protocols[config.TierZen],
			Protocols: protocols, Anonymous: true, KeyTiers: keyTiers,
			BackendSelector: selector, UpstreamModel: upstreamModelFor(model, base),
		}, nil
	}
	if len(keyTiers) > 0 {
		protocols := c.protocolsForLocked(base, keyTiers, false)
		return Route{
			ID: model, Tier: keyTiers[0], Protocol: protocols[keyTiers[0]],
			Protocols: protocols, KeyTiers: keyTiers,
			BackendSelector: selector, UpstreamModel: upstreamModelFor(model, base),
		}, nil
	}
	return Route{}, fmt.Errorf("model %q is not available in the configured Zen or Go pools", model)
}

// upstreamModelFor 返回发往上游的实际模型名。虚拟子模型返回基础模型名。
func upstreamModelFor(virtual, base string) string {
	if virtual != base {
		return base
	}
	return ""
}

func (r Route) ProtocolFor(tier config.Tier) wire.Protocol {
	if protocol := r.Protocols[tier]; protocol != "" {
		return protocol
	}
	return r.Protocol
}

func (c *Catalog) protocolsForLocked(model string, keyTiers []config.Tier, includeZen bool) map[config.Tier]wire.Protocol {
	protocols := make(map[config.Tier]wire.Protocol, len(keyTiers)+1)
	if includeZen {
		protocols[config.TierZen] = c.protocolForLocked(model, config.TierZen)
	}
	for _, tier := range keyTiers {
		protocols[tier] = c.protocolForLocked(model, tier)
	}
	return protocols
}

func (c *Catalog) protocolForLocked(model string, tier config.Tier) wire.Protocol {
	base, _ := splitVirtualModel(model)
	if protocol := c.protocols[base]; protocol != "" {
		return protocol
	}
	if protocol := c.nativeProtocols[tier][base]; protocol != "" {
		return protocol
	}
	// The OpenCode capability catalog is authoritative when available. Chat is
	// the only safe protocol-neutral fallback for an ID that has just appeared
	// in /v1/models but is not present in the capability snapshot yet.
	return wire.Chat
}

// keyTierOrderLocked builds an authenticated route in prefer order. A tier is
// included only when it has a key and advertises the model. Before the first
// successful catalog refresh, configured key pools remain usable so temporary
// discovery failures do not take the gateway offline.
func (c *Catalog) keyTierOrderLocked(model string, hasZenKeys, hasGoKeys bool) []config.Tier {
	base, _ := splitVirtualModel(model)
	catalogPending := len(c.zen) == 0 && len(c.goModels) == 0
	available := func(tier config.Tier) bool {
		switch tier {
		case config.TierZen:
			return hasZenKeys && (catalogPending || c.zen[base]) && c.tierSupportedLocked(model, config.TierZen)
		case config.TierGo:
			return hasGoKeys && (catalogPending || c.goModels[base]) && c.tierSupportedLocked(model, config.TierGo)
		default:
			return false
		}
	}
	order := []config.Tier{config.TierZen, config.TierGo}
	if c.prefer == config.TierGo {
		order[0], order[1] = order[1], order[0]
	}
	result := make([]config.Tier, 0, len(order))
	for _, tier := range order {
		if available(tier) {
			result = append(result, tier)
		}
	}
	return result
}

// RouteForTier builds a route pinned to one tier, used by the Playground's
// per-key diagnostics. It never selects the anonymous lane and never adds a
// fallback tier: the operator asked to exercise one configured key, so the
// result describes that key and nothing else.
func (c *Catalog) RouteForTier(model string, tier config.Tier, hasZenKeys, hasGoKeys bool) (Route, error) {
	if tier != config.TierZen && tier != config.TierGo {
		return Route{}, errors.New("selected key tier must be zen or go")
	}
	hasKeys := hasZenKeys
	if tier == config.TierGo {
		hasKeys = hasGoKeys
	}
	if !hasKeys {
		return Route{}, fmt.Errorf("no %s key is configured", tier)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	base, selector := splitVirtualModel(model)
	catalogPending := len(c.zen) == 0 && len(c.goModels) == 0
	advertised := c.zen[base]
	if tier == config.TierGo {
		advertised = c.goModels[base]
	}
	if !catalogPending && !advertised {
		return Route{}, fmt.Errorf("model %q is not available in the selected %s key tier", model, tier)
	}
	if !c.tierSupportedLocked(model, tier) {
		return Route{}, fmt.Errorf("model %q uses an upstream protocol that is not available on the selected %s key tier", model, tier)
	}
	protocol := c.protocolForLocked(model, tier)
	return Route{
		ID: model, Tier: tier, Protocol: protocol,
		Protocols:       map[config.Tier]wire.Protocol{tier: protocol},
		KeyTiers:        []config.Tier{tier},
		BackendSelector: selector, UpstreamModel: upstreamModelFor(model, base),
	}, nil
}

func (c *Catalog) anonymousDecision(model string) AnonymousDecision {
	base, _ := splitVirtualModel(model)
	if c.pricing != nil {
		return c.pricing.Decide(base)
	}
	return AnonymousDecision{Allowed: isFreeModel(model), Source: "name_fallback_metadata_pending"}
}

// IsFreeModel reports whether the catalog considers the model free-tier
// (pricing metadata, falling back to the -free name convention). Key-tier
// shaping uses it so free models without "free" in the name are covered.
func (c *Catalog) IsFreeModel(model string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.anonymousDecision(model).Allowed
}

func (c *Catalog) Diagnostic(model string, requested wire.Protocol, hasZenKeys, hasGoKeys, hasAnonymous bool) RouteDiagnostic {
	c.mu.RLock()
	base, _ := splitVirtualModel(model)
	configured, explicit := c.protocols[base]
	zen, goModel := c.zen[base], c.goModels[base]
	nativeProtocols := map[config.Tier]wire.Protocol{
		config.TierZen: c.protocolForLocked(model, config.TierZen),
		config.TierGo:  c.protocolForLocked(model, config.TierGo),
	}
	_, zenKnown := c.nativeProtocols[config.TierZen][base]
	_, goKnown := c.nativeProtocols[config.TierGo][base]
	c.mu.RUnlock()
	source := "configured"
	if !explicit {
		source = "default"
		if zenKnown || goKnown {
			source = "upstream"
		}
	}
	protocol := configured
	if protocol == "" {
		// Route() below selects the preferred available tier. This is only the
		// fallback shown when no route can currently be built.
		protocol = nativeProtocols[config.TierZen]
		if c.prefer == config.TierGo {
			protocol = nativeProtocols[config.TierGo]
		}
	}
	diagnostic := RouteDiagnostic{
		Model: model, RequestedProtocol: requested, NativeProtocol: protocol, NativeProtocols: nativeProtocols, ProtocolSource: source,
		AvailableZen: zen, AvailableGo: goModel, AnonymousEligibility: c.anonymousDecision(model),
	}
	route, err := c.Route(model, hasZenKeys, hasGoKeys, hasAnonymous)
	if err != nil {
		diagnostic.RouteError = err.Error()
		return diagnostic
	}
	diagnostic.NativeProtocol = route.Protocol
	diagnostic.NativeProtocols = route.Protocols
	diagnostic.Tier, diagnostic.Anonymous = route.Tier, route.Anonymous
	diagnostic.KeyTiers = append([]config.Tier(nil), route.KeyTiers...)
	return diagnostic
}

// splitVirtualModel 将 "exo-free-claude" / "exo-free-gpt" 拆分为基础模型和
// 期望后端前缀。非虚拟模型返回原始 ID 和空选择器。
func splitVirtualModel(model string) (base string, selector string) {
	switch model {
	case "exo-free-claude":
		return "exo-free", "msg_"
	case "exo-free-gpt":
		return "exo-free", "resp_"
	}
	return model, ""
}

// virtualModelSuffixes 列出所有虚拟子模型的后缀，用于在模型列表中展开。
var virtualModelSuffixes = []string{"-claude", "-gpt"}

func isFreeModel(model string) bool {
	base, _ := splitVirtualModel(model)
	return strings.Contains(strings.ToLower(base), "free")
}

func (c *Catalog) List() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.modelIDsLocked()
	models := ids[:0]
	for _, model := range ids {
		if c.supportedLocked(model) {
			models = append(models, model)
		}
	}
	return models
}

func (c *Catalog) modelIDsLocked() []string {
	seen := make(map[string]bool, len(c.zen)+len(c.goModels))
	for model := range c.zen {
		seen[model] = true
	}
	for model := range c.goModels {
		seen[model] = true
	}
	// 为 exo-free 的每个后端提供可选择的虚拟子模型。
	if seen["exo-free"] {
		for _, suffix := range virtualModelSuffixes {
			seen["exo-free"+suffix] = true
		}
	}
	return sortedSetKeys(seen)
}

func (c *Catalog) Snapshot() CatalogSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked(c.modelIDsLocked())
}

// AvailableModels provides discovery and readiness with the same route
// filtering, including configured key tiers and anonymous eligibility.
func (c *Catalog) AvailableModels(hasZenKeys, hasGoKeys, hasAnonymous bool) ([]Route, CatalogSnapshot) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.modelIDsLocked()
	routes := make([]Route, 0, len(ids))
	for _, model := range ids {
		if !c.supportedLocked(model) {
			continue
		}
		if route, err := c.routeLocked(model, hasZenKeys, hasGoKeys, hasAnonymous); err == nil {
			routes = append(routes, route)
		}
	}
	snapshot := c.snapshotLocked(ids)
	snapshot.Exposed = len(routes)
	return routes, snapshot
}

func (c *Catalog) snapshotLocked(ids []string) CatalogSnapshot {
	exposed := 0
	for _, model := range ids {
		if c.supportedLocked(model) {
			exposed++
		}
	}
	stale := c.stale
	if !c.updatedAt.IsZero() && c.refreshAfter > 0 {
		stale = stale || time.Since(c.updatedAt) > max(2*c.refreshAfter, time.Minute)
	}
	return CatalogSnapshot{
		Zen: len(c.zen), Go: len(c.goModels), Total: len(ids), Exposed: exposed,
		UpdatedAt: c.updatedAt, CacheSource: c.cacheSource, Stale: stale,
	}
}

func (c *Catalog) Supported(model string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.supportedLocked(model)
}

// MetadataForTier returns the rich per-model metadata (context window,
// reasoning, tool call, modalities) captured from the opencode catalog for
// the tier that will actually serve the request. Same-named models can carry
// different limits per tier, so callers must pass route.Tier — never a
// tier-blind lookup. Anonymous routes always resolve to TierZen, which keeps
// the keyless path on Zen metadata. The zero value is returned for models
// the catalog does not describe (or before the first capability refresh).
func (c *Catalog) MetadataForTier(model string, tier config.Tier) Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	base, _ := splitVirtualModel(model)
	return c.modelMeta[tier][base]
}

func (c *Catalog) supportedLocked(model string) bool {
	base, _ := splitVirtualModel(model)
	if len(c.zen) == 0 && len(c.goModels) == 0 {
		return true
	}
	if c.zen[base] && c.tierSupportedLocked(model, config.TierZen) {
		return true
	}
	if c.goModels[base] && c.tierSupportedLocked(model, config.TierGo) {
		return true
	}
	return false
}

func (c *Catalog) tierSupportedLocked(model string, tier config.Tier) bool {
	base, _ := splitVirtualModel(model)
	if c.protocols[base] != "" {
		return true
	}
	if c.unsupported[tier][base] {
		return false
	}
	if c.nativeProtocols[tier][base] != "" {
		return true
	}
	// A pending catalog has no upstream capability snapshot to contradict a
	// configured key, so retain the pre-refresh compatibility behavior.
	return len(c.zen) == 0 && len(c.goModels) == 0
}

func toSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[item] = true
	}
	return out
}

func cloneProtocols(source map[string]wire.Protocol) map[string]wire.Protocol {
	result := make(map[string]wire.Protocol, len(source))
	for model, protocol := range source {
		result[model] = protocol
	}
	return result
}

func cloneBools(source map[string]bool) map[string]bool {
	result := make(map[string]bool, len(source))
	for model, value := range source {
		result[model] = value
	}
	return result
}

func cloneModelMeta(source map[config.Tier]map[string]Metadata) map[config.Tier]map[string]Metadata {
	result := map[config.Tier]map[string]Metadata{config.TierZen: {}, config.TierGo: {}}
	for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
		for id, md := range source[tier] {
			result[tier][id] = md
		}
	}
	return result
}

func sortedSetKeys(source map[string]bool) []string {
	result := make([]string, 0, len(source))
	for model, available := range source {
		if available {
			result = append(result, model)
		}
	}
	sort.Strings(result)
	return result
}

func cloneTierProtocols(source map[config.Tier]map[string]wire.Protocol) map[config.Tier]map[string]wire.Protocol {
	result := map[config.Tier]map[string]wire.Protocol{config.TierZen: {}, config.TierGo: {}}
	for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
		if protocols, ok := source[tier]; ok {
			result[tier] = cloneProtocols(protocols)
		}
	}
	return result
}

func cloneTierBools(source map[config.Tier]map[string]bool) map[config.Tier]map[string]bool {
	result := map[config.Tier]map[string]bool{config.TierZen: {}, config.TierGo: {}}
	for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
		if models, ok := source[tier]; ok {
			result[tier] = cloneBools(models)
		}
	}
	return result
}
