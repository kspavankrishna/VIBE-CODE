package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const plannerVersion = "2026-04-hedge-1"

// CapabilitySet describes what a provider/model pair can safely serve.
type CapabilitySet struct {
	SupportsJSON      bool `json:"supports_json"`
	SupportsTools     bool `json:"supports_tools"`
	SupportsReasoning bool `json:"supports_reasoning"`
	SupportsVision    bool `json:"supports_vision"`
	SupportsStreaming bool `json:"supports_streaming"`
	MaxContextTokens  int  `json:"max_context_tokens"`
	MaxOutputTokens   int  `json:"max_output_tokens"`
}

// CostModel stores token pricing in USD.
type CostModel struct {
	InputPer1KUSD       float64 `json:"input_per_1k_usd"`
	CachedInputPer1KUSD float64 `json:"cached_input_per_1k_usd,omitempty"`
	OutputPer1KUSD      float64 `json:"output_per_1k_usd"`
}

// LatencyProfile is the planner's live or predicted latency view for a provider.
type LatencyProfile struct {
	P50Ms    float64 `json:"p50_ms"`
	P95Ms    float64 `json:"p95_ms"`
	JitterMs float64 `json:"jitter_ms,omitempty"`
}

// HealthWindow captures recent reliability.
type HealthWindow struct {
	ErrorRate1m    float64 `json:"error_rate_1m"`
	ThrottleRate1m float64 `json:"throttle_rate_1m"`
	Warm           bool    `json:"warm"`
}

// CapacityWindow captures live concurrency pressure.
type CapacityWindow struct {
	InFlight         int `json:"in_flight"`
	ConcurrencyLimit int `json:"concurrency_limit"`
}

// ProviderSnapshot is the input contract for a provider or model deployment.
type ProviderSnapshot struct {
	Name          string         `json:"name"`
	Vendor        string         `json:"vendor"`
	Region        string         `json:"region"`
	Model         string         `json:"model"`
	Capabilities  CapabilitySet  `json:"capabilities"`
	Cost          CostModel      `json:"cost"`
	Latency       LatencyProfile `json:"latency"`
	Health        HealthWindow   `json:"health"`
	Capacity      CapacityWindow `json:"capacity"`
	DisabledUntil time.Time      `json:"disabled_until,omitempty"`
	RetryAfterMs  int            `json:"retry_after_ms,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
}

// RequestShape describes a single inference request.
type RequestShape struct {
	ID                     string   `json:"id,omitempty"`
	InputTokens            int      `json:"input_tokens"`
	CachedInputTokens      int      `json:"cached_input_tokens,omitempty"`
	ExpectedOutputTokens   int      `json:"expected_output_tokens"`
	NeedsJSON              bool     `json:"needs_json,omitempty"`
	NeedsTools             bool     `json:"needs_tools,omitempty"`
	NeedsReasoning         bool     `json:"needs_reasoning,omitempty"`
	NeedsVision            bool     `json:"needs_vision,omitempty"`
	WantsStreaming         bool     `json:"wants_streaming,omitempty"`
	LatencySLOMs           int      `json:"latency_slo_ms,omitempty"`
	MaxCostUSD             float64  `json:"max_cost_usd,omitempty"`
	HedgeAllowed           bool     `json:"hedge_allowed,omitempty"`
	MaxParallel            int      `json:"max_parallel,omitempty"`
	PreferredVendors       []string `json:"preferred_vendors,omitempty"`
	PreferredRegions       []string `json:"preferred_regions,omitempty"`
	DeniedVendors          []string `json:"denied_vendors,omitempty"`
	DeniedRegions          []string `json:"denied_regions,omitempty"`
	RequireVendorDiversity bool     `json:"require_vendor_diversity,omitempty"`
	RequireRegionDiversity bool     `json:"require_region_diversity,omitempty"`
	MaxThrottleRate        float64  `json:"max_throttle_rate,omitempty"`
}

// PlannerConfig controls scoring, hard rejections, and hedge timing.
type PlannerConfig struct {
	DefaultLatencySLOMs     int     `json:"default_latency_slo_ms"`
	LatencyWeight           float64 `json:"latency_weight"`
	CostWeight              float64 `json:"cost_weight"`
	ErrorWeight             float64 `json:"error_weight"`
	ThrottleWeight          float64 `json:"throttle_weight"`
	SaturationWeight        float64 `json:"saturation_weight"`
	TailWeight              float64 `json:"tail_weight"`
	WarmPenalty             float64 `json:"warm_penalty"`
	PreferenceBonus         float64 `json:"preference_bonus"`
	VendorDiversityPenalty  float64 `json:"vendor_diversity_penalty"`
	RegionDiversityPenalty  float64 `json:"region_diversity_penalty"`
	HardErrorLimit          float64 `json:"hard_error_limit"`
	HardThrottleLimit       float64 `json:"hard_throttle_limit"`
	HardSaturationLimit     float64 `json:"hard_saturation_limit"`
	MinHealthyHeadroom      int     `json:"min_healthy_headroom"`
	HedgeP95TriggerFraction float64 `json:"hedge_p95_trigger_fraction"`
	HedgeSaturationTrigger  float64 `json:"hedge_saturation_trigger"`
	HedgeThrottleTrigger    float64 `json:"hedge_throttle_trigger"`
	HedgeErrorTrigger       float64 `json:"hedge_error_trigger"`
	MinHedgeDelayMs         int     `json:"min_hedge_delay_ms"`
	MaxHedgeDelayMs         int     `json:"max_hedge_delay_ms"`
}

// PlanInput is the JSON shape accepted by the CLI and library.
type PlanInput struct {
	Request   RequestShape       `json:"request"`
	Providers []ProviderSnapshot `json:"providers"`
	Config    *PlannerConfig     `json:"config,omitempty"`
}

// SelectedProvider is a planner decision record that is safe to emit to users.
type SelectedProvider struct {
	Name             string   `json:"name"`
	Vendor           string   `json:"vendor"`
	Region           string   `json:"region"`
	Model            string   `json:"model"`
	Score            float64  `json:"score"`
	EstimatedCostUSD float64  `json:"estimated_cost_usd"`
	PredictedP50Ms   int      `json:"predicted_p50_ms"`
	PredictedP95Ms   int      `json:"predicted_p95_ms"`
	Saturation       float64  `json:"saturation"`
	Reasons          []string `json:"reasons"`
}

// RejectedProvider captures why a provider was filtered out.
type RejectedProvider struct {
	Name    string   `json:"name"`
	Reasons []string `json:"reasons"`
}

// RoutingPlan is the planner's final result.
type RoutingPlan struct {
	PlannerVersion        string             `json:"planner_version"`
	RequestID             string             `json:"request_id,omitempty"`
	Primary               *SelectedProvider  `json:"primary,omitempty"`
	Hedge                 *SelectedProvider  `json:"hedge,omitempty"`
	HedgeAfterMs          int                `json:"hedge_after_ms,omitempty"`
	EstimatedPrimaryCost  float64            `json:"estimated_primary_cost_usd"`
	EstimatedWorstCaseUSD float64            `json:"estimated_worst_case_cost_usd"`
	PredictedPrimaryP95Ms int                `json:"predicted_primary_p95_ms"`
	PredictedPlanP95Ms    int                `json:"predicted_plan_p95_ms"`
	Reasons               []string           `json:"reasons"`
	Rejected              []RejectedProvider `json:"rejected,omitempty"`
	GeneratedAt           time.Time          `json:"generated_at"`
}

type candidate struct {
	provider   ProviderSnapshot
	score      float64
	cost       float64
	p50        float64
	p95        float64
	saturation float64
	reasons    []string
	rejected   []string
}

type requestFilters struct {
	preferredVendors map[string]struct{}
	preferredRegions map[string]struct{}
	deniedVendors    map[string]struct{}
	deniedRegions    map[string]struct{}
}

// Observation represents one completed provider call. It powers live snapshots.
type Observation struct {
	Latency    time.Duration
	HTTPStatus int
	Err        error
	Throttled  bool
}

// ObservationStore keeps a small live window of latency and failure data.
type ObservationStore struct {
	mu        sync.Mutex
	halfLife  time.Duration
	windowCap int
	now       func() time.Time
	providers map[string]*liveStats
}

type liveStats struct {
	latencies     LatencyWindow
	inflight      int
	errorEWMA     float64
	throttleEWMA  float64
	lastUpdatedAt time.Time
	warm          bool
}

// LatencyWindow stores a rolling sample window for percentile calculations.
type LatencyWindow struct {
	values []float64
	count  int
	next   int
}

func DefaultPlannerConfig() PlannerConfig {
	return PlannerConfig{
		DefaultLatencySLOMs:     2200,
		LatencyWeight:           1.00,
		CostWeight:              0.55,
		ErrorWeight:             1.10,
		ThrottleWeight:          1.15,
		SaturationWeight:        0.85,
		TailWeight:              0.45,
		WarmPenalty:             0.20,
		PreferenceBonus:         0.18,
		VendorDiversityPenalty:  0.30,
		RegionDiversityPenalty:  0.12,
		HardErrorLimit:          0.35,
		HardThrottleLimit:       0.25,
		HardSaturationLimit:     0.98,
		MinHealthyHeadroom:      1,
		HedgeP95TriggerFraction: 0.90,
		HedgeSaturationTrigger:  0.78,
		HedgeThrottleTrigger:    0.12,
		HedgeErrorTrigger:       0.10,
		MinHedgeDelayMs:         120,
		MaxHedgeDelayMs:         1800,
	}
}

func (c PlannerConfig) Normalize() PlannerConfig {
	d := DefaultPlannerConfig()
	if c.DefaultLatencySLOMs <= 0 {
		c.DefaultLatencySLOMs = d.DefaultLatencySLOMs
	}
	c.LatencyWeight = defaultFloat(c.LatencyWeight, d.LatencyWeight)
	c.CostWeight = defaultFloat(c.CostWeight, d.CostWeight)
	c.ErrorWeight = defaultFloat(c.ErrorWeight, d.ErrorWeight)
	c.ThrottleWeight = defaultFloat(c.ThrottleWeight, d.ThrottleWeight)
	c.SaturationWeight = defaultFloat(c.SaturationWeight, d.SaturationWeight)
	c.TailWeight = defaultFloat(c.TailWeight, d.TailWeight)
	c.WarmPenalty = defaultFloat(c.WarmPenalty, d.WarmPenalty)
	c.PreferenceBonus = defaultFloat(c.PreferenceBonus, d.PreferenceBonus)
	c.VendorDiversityPenalty = defaultFloat(c.VendorDiversityPenalty, d.VendorDiversityPenalty)
	c.RegionDiversityPenalty = defaultFloat(c.RegionDiversityPenalty, d.RegionDiversityPenalty)
	c.HardErrorLimit = clamp(defaultFloat(c.HardErrorLimit, d.HardErrorLimit), 0.01, 1.0)
	c.HardThrottleLimit = clamp(defaultFloat(c.HardThrottleLimit, d.HardThrottleLimit), 0.01, 1.0)
	c.HardSaturationLimit = clamp(defaultFloat(c.HardSaturationLimit, d.HardSaturationLimit), 0.05, 1.0)
	if c.MinHealthyHeadroom <= 0 {
		c.MinHealthyHeadroom = d.MinHealthyHeadroom
	}
	c.HedgeP95TriggerFraction = clamp(defaultFloat(c.HedgeP95TriggerFraction, d.HedgeP95TriggerFraction), 0.10, 2.0)
	c.HedgeSaturationTrigger = clamp(defaultFloat(c.HedgeSaturationTrigger, d.HedgeSaturationTrigger), 0.05, 1.0)
	c.HedgeThrottleTrigger = clamp(defaultFloat(c.HedgeThrottleTrigger, d.HedgeThrottleTrigger), 0.0, 1.0)
	c.HedgeErrorTrigger = clamp(defaultFloat(c.HedgeErrorTrigger, d.HedgeErrorTrigger), 0.0, 1.0)
	if c.MinHedgeDelayMs <= 0 {
		c.MinHedgeDelayMs = d.MinHedgeDelayMs
	}
	if c.MaxHedgeDelayMs <= 0 {
		c.MaxHedgeDelayMs = d.MaxHedgeDelayMs
	}
	if c.MinHedgeDelayMs > c.MaxHedgeDelayMs {
		c.MinHedgeDelayMs, c.MaxHedgeDelayMs = c.MaxHedgeDelayMs, c.MinHedgeDelayMs
	}
	return c
}

// PlanRouting picks a primary provider and, when justified, an adaptive hedge.
func PlanRouting(input PlanInput) (RoutingPlan, error) {
	cfg := DefaultPlannerConfig()
	if input.Config != nil {
		cfg = input.Config.Normalize()
	} else {
		cfg = cfg.Normalize()
	}
	if err := validateRequest(input.Request); err != nil {
		return RoutingPlan{}, err
	}
	if len(input.Providers) == 0 {
		return RoutingPlan{}, fmt.Errorf("no providers supplied")
	}

	req := input.Request
	if req.MaxParallel <= 0 {
		if req.HedgeAllowed {
			req.MaxParallel = 2
		} else {
			req.MaxParallel = 1
		}
	}
	filters := newRequestFilters(req)
	now := time.Now().UTC()

	accepted := make([]candidate, 0, len(input.Providers))
	rejected := make([]RejectedProvider, 0, len(input.Providers))
	for _, provider := range input.Providers {
		cand := evaluateCandidate(req, filters, provider, cfg, now)
		if len(cand.rejected) > 0 {
			rejected = append(rejected, RejectedProvider{
				Name:    providerLabel(provider),
				Reasons: append([]string(nil), cand.rejected...),
			})
			continue
		}
		accepted = append(accepted, cand)
	}
	if len(accepted) == 0 {
		return RoutingPlan{}, fmt.Errorf("no eligible providers after filtering")
	}

	sortCandidates(accepted)
	primary := accepted[0]
	primarySelected := primary.selectedProvider()

	plan := RoutingPlan{
		PlannerVersion:        plannerVersion,
		RequestID:             req.ID,
		Primary:               &primarySelected,
		EstimatedPrimaryCost:  roundUSD(primary.cost),
		EstimatedWorstCaseUSD: roundUSD(primary.cost),
		PredictedPrimaryP95Ms: int(math.Round(primary.p95)),
		PredictedPlanP95Ms:    int(math.Round(primary.p95)),
		Rejected:              rejected,
		Reasons: []string{
			fmt.Sprintf("picked %s as primary because its weighted score %.4f beat %d other eligible providers", primary.provider.Name, roundFloat(primary.score, 4), len(accepted)-1),
			fmt.Sprintf("primary tail prediction is %.0f ms at an estimated %.4f USD", primary.p95, roundUSD(primary.cost)),
		},
		GeneratedAt: now,
	}

	headge, delay, hedgeReasons := chooseHedge(primary, accepted, req, cfg)
	if hedge != nil {
		headgeSelected := hedge.selectedProvider()
		plan.Hedge = &hedgeSelected
		plan.HedgeAfterMs = delay
		plan.EstimatedWorstCaseUSD = roundUSD(primary.cost + hedge.cost)
		plan.PredictedPlanP95Ms = predictHedgedP95(primary, *hedge, delay)
		plan.Reasons = append(plan.Reasons, hedgeReasons...)
	} else {
		plan.Reasons = append(plan.Reasons, hedgeReasons...)
	}

	return plan, nil
}

func validateRequest(req RequestShape) error {
	if req.InputTokens < 0 {
		return fmt.Errorf("input_tokens must be >= 0")
	}
	if req.CachedInputTokens < 0 {
		return fmt.Errorf("cached_input_tokens must be >= 0")
	}
	if req.ExpectedOutputTokens < 0 {
		return fmt.Errorf("expected_output_tokens must be >= 0")
	}
	if req.MaxCostUSD < 0 {
		return fmt.Errorf("max_cost_usd must be >= 0")
	}
	if req.MaxThrottleRate < 0 || req.MaxThrottleRate > 1 {
		return fmt.Errorf("max_throttle_rate must be between 0 and 1")
	}
	return nil
}

func newRequestFilters(req RequestShape) requestFilters {
	return requestFilters{
		preferredVendors: makeLookup(req.PreferredVendors),
		preferredRegions: makeLookup(req.PreferredRegions),
		deniedVendors:    makeLookup(req.DeniedVendors),
		deniedRegions:    makeLookup(req.DeniedRegions),
	}
}

func (f requestFilters) vendorPreferred(v string) bool {
	_, ok := f.preferredVendors[normalizeKey(v)]
	return ok
}

func (f requestFilters) regionPreferred(v string) bool {
	_, ok := f.preferredRegions[normalizeKey(v)]
	return ok
}

func (f requestFilters) vendorDenied(v string) bool {
	_, ok := f.deniedVendors[normalizeKey(v)]
	return ok
}

func (f requestFilters) regionDenied(v string) bool {
	_, ok := f.deniedRegions[normalizeKey(v)]
	return ok
}

func makeLookup(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		key := normalizeKey(value)
		if key == "" {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

func evaluateCandidate(req RequestShape, filters requestFilters, provider ProviderSnapshot, cfg PlannerConfig, now time.Time) candidate {
	cand := candidate{provider: provider}
	reject := func(reason string) {
		cand.rejected = append(cand.rejected, reason)
	}

	if strings.TrimSpace(provider.Name) == "" {
		reject("missing provider name")
	}
	if strings.TrimSpace(provider.Model) == "" {
		reject("missing model name")
	}
	if filters.vendorDenied(provider.Vendor) {
		reject(fmt.Sprintf("vendor %q is denied by request policy", provider.Vendor))
	}
	if filters.regionDenied(provider.Region) {
		reject(fmt.Sprintf("region %q is denied by request policy", provider.Region))
	}
	if !provider.DisabledUntil.IsZero() && provider.DisabledUntil.After(now) {
		reject(fmt.Sprintf("provider disabled until %s", provider.DisabledUntil.Format(time.RFC3339)))
	}
	if req.NeedsJSON && !provider.Capabilities.SupportsJSON {
		reject("provider does not support structured JSON mode")
	}
	if req.NeedsTools && !provider.Capabilities.SupportsTools {
		reject("provider does not support tool calling")
	}
	if req.NeedsReasoning && !provider.Capabilities.SupportsReasoning {
		reject("provider does not support reasoning mode")
	}
	if req.NeedsVision && !provider.Capabilities.SupportsVision {
		reject("provider does not support vision input")
	}
	if req.WantsStreaming && !provider.Capabilities.SupportsStreaming {
		reject("provider does not support streaming")
	}

	totalContext := req.InputTokens + req.CachedInputTokens + req.ExpectedOutputTokens
	if provider.Capabilities.MaxContextTokens > 0 && totalContext > provider.Capabilities.MaxContextTokens {
		reject(fmt.Sprintf("context window %d is too small for %d total tokens", provider.Capabilities.MaxContextTokens, totalContext))
	}
	if provider.Capabilities.MaxOutputTokens > 0 && req.ExpectedOutputTokens > provider.Capabilities.MaxOutputTokens {
		reject(fmt.Sprintf("max output %d is too small for %d expected output tokens", provider.Capabilities.MaxOutputTokens, req.ExpectedOutputTokens))
	}
	if req.MaxThrottleRate > 0 && provider.Health.ThrottleRate1m > req.MaxThrottleRate {
		reject(fmt.Sprintf("throttle rate %.2f exceeds request cap %.2f", provider.Health.ThrottleRate1m, req.MaxThrottleRate))
	}

	saturation := computeSaturation(provider.Capacity)
	cand.saturation = saturation
	if provider.Capacity.ConcurrencyLimit > 0 {
		headroom := provider.Capacity.ConcurrencyLimit - provider.Capacity.InFlight
		if headroom < cfg.MinHealthyHeadroom {
			reject(fmt.Sprintf("healthy headroom %d is below required minimum %d", headroom, cfg.MinHealthyHeadroom))
		}
	}
	if saturation >= cfg.HardSaturationLimit {
		reject(fmt.Sprintf("saturation %.2f exceeds hard limit %.2f", saturation, cfg.HardSaturationLimit))
	}
	if provider.Health.ErrorRate1m >= cfg.HardErrorLimit {
		reject(fmt.Sprintf("recent error rate %.2f exceeds hard limit %.2f", provider.Health.ErrorRate1m, cfg.HardErrorLimit))
	}
	if provider.Health.ThrottleRate1m >= cfg.HardThrottleLimit {
		reject(fmt.Sprintf("recent throttle rate %.2f exceeds hard limit %.2f", provider.Health.ThrottleRate1m, cfg.HardThrottleLimit))
	}

	cost := EstimateCostUSD(req, provider)
	cand.cost = cost
	if req.MaxCostUSD > 0 && cost > req.MaxCostUSD {
		reject(fmt.Sprintf("estimated primary cost %.4f USD exceeds request budget %.4f USD", roundUSD(cost), req.MaxCostUSD))
	}
	if len(cand.rejected) > 0 {
		return cand
	}

	p50 := provider.Latency.P50Ms
	p95 := provider.Latency.P95Ms
	if p50 <= 0 && p95 > 0 {
		p50 = p95 * 0.65
	}
	if p95 <= 0 && p50 > 0 {
		p95 = p50 * 1.55
	}
	if p50 <= 0 {
		p50 = 450
	}
	if p95 < p50 {
		p95 = p50
	}
	cand.p50 = p50
	cand.p95 = p95

	latencyBaseline := float64(req.LatencySLOMs)
	if latencyBaseline <= 0 {
		latencyBaseline = float64(cfg.DefaultLatencySLOMs)
	}
	costBaseline := req.MaxCostUSD
	if costBaseline <= 0 {
		costBaseline = math.Max(cost, 0.03)
	}

	latencyScore := p95 / latencyBaseline
	costScore := cost / costBaseline
	tailScore := 0.0
	if p50 > 0 {
		tailScore = (p95 - p50) / p50
	}
	warmPenalty := 0.0
	if !provider.Health.Warm {
		warmPenalty = cfg.WarmPenalty
	}
	throttlePenalty := 0.0
	if provider.RetryAfterMs > 0 {
		throttlePenalty = math.Min(1.0, float64(provider.RetryAfterMs)/latencyBaseline)
	}

	score :=
		cfg.LatencyWeight*latencyScore +
		cfg.CostWeight*costScore +
		cfg.ErrorWeight*provider.Health.ErrorRate1m +
		cfg.ThrottleWeight*provider.Health.ThrottleRate1m +
		cfg.SaturationWeight*math.Pow(math.Max(saturation, 0), 1.35) +
		cfg.TailWeight*tailScore +
		warmPenalty +
		throttlePenalty*0.15

	if filters.vendorPreferred(provider.Vendor) {
		score -= cfg.PreferenceBonus
		cand.reasons = append(cand.reasons, "preferred vendor matched")
	}
	if filters.regionPreferred(provider.Region) {
		score -= cfg.PreferenceBonus * 0.5
		cand.reasons = append(cand.reasons, "preferred region matched")
	}
	if provider.Health.Warm {
		cand.reasons = append(cand.reasons, "warm recent traffic")
	} else {
		cand.reasons = append(cand.reasons, "cold recent traffic penalty applied")
	}
	if provider.RetryAfterMs > 0 {
		cand.reasons = append(cand.reasons, fmt.Sprintf("retry-after penalty from %d ms backoff", provider.RetryAfterMs))
	}
	cand.reasons = append(cand.reasons,
		fmt.Sprintf("predicted p95 %.0f ms", p95),
		fmt.Sprintf("estimated primary cost %.4f USD", roundUSD(cost)),
		fmt.Sprintf("saturation %.2f", saturation),
	)
	cand.score = score
	return cand
}

func EstimateCostUSD(req RequestShape, provider ProviderSnapshot) float64 {
	uncached := math.Max(float64(req.InputTokens), 0)
	cached := math.Max(float64(req.CachedInputTokens), 0)
	output := math.Max(float64(req.ExpectedOutputTokens), 0)

	cachedPrice := provider.Cost.CachedInputPer1KUSD
	if cachedPrice <= 0 {
		cachedPrice = provider.Cost.InputPer1KUSD
	}

	cost := 0.0
	cost += (uncached / 1000.0) * provider.Cost.InputPer1KUSD
	cost += (cached / 1000.0) * cachedPrice
	cost += (output / 1000.0) * provider.Cost.OutputPer1KUSD
	return cost
}

func computeSaturation(capacity CapacityWindow) float64 {
	if capacity.ConcurrencyLimit <= 0 {
		if capacity.InFlight <= 0 {
			return 0
		}
		return 0.25
	}
	return clamp(float64(capacity.InFlight)/float64(capacity.ConcurrencyLimit), 0, 2.0)
}

func sortCandidates(cands []candidate) {
	sort.SliceStable(cands, func(i, j int) bool {
		if nearlyEqual(cands[i].score, cands[j].score) {
			if nearlyEqual(cands[i].cost, cands[j].cost) {
				if nearlyEqual(cands[i].p95, cands[j].p95) {
					return providerLabel(cands[i].provider) < providerLabel(cands[j].provider)
				}
				return cands[i].p95 < cands[j].p95
			}
			return cands[i].cost < cands[j].cost
		}
		return cands[i].score < cands[j].score
	})
}

func chooseHedge(primary candidate, accepted []candidate, req RequestShape, cfg PlannerConfig) (*candidate, int, []string) {
	if !req.HedgeAllowed {
		return nil, 0, []string{"hedging disabled for this request"}
	}
	if req.MaxParallel < 2 {
		return nil, 0, []string{"max_parallel prevents a hedge path"}
	}
	if len(accepted) < 2 {
		return nil, 0, []string{"no alternate provider available for hedging"}
	}
	if !shouldHedge(primary, req, cfg) {
		return nil, 0, []string{"primary provider already satisfies the current latency and risk envelope without a hedge"}
	}

	var (
		best      *candidate
		bestScore = math.MaxFloat64
	)
	for i := range accepted {
		cand := &accepted[i]
		if cand.provider.Name == primary.provider.Name {
			continue
		}
		if req.RequireVendorDiversity && normalizeKey(cand.provider.Vendor) == normalizeKey(primary.provider.Vendor) {
			continue
		}
		if req.RequireRegionDiversity && normalizeKey(cand.provider.Region) == normalizeKey(primary.provider.Region) {
			continue
		}
		worstCaseCost := primary.cost + cand.cost
		if req.MaxCostUSD > 0 && worstCaseCost > req.MaxCostUSD {
			continue
		}

		score := cand.score
		if normalizeKey(cand.provider.Vendor) == normalizeKey(primary.provider.Vendor) {
			score += cfg.VendorDiversityPenalty
		} else {
			score -= cfg.PreferenceBonus * 0.50
		}
		if normalizeKey(cand.provider.Region) == normalizeKey(primary.provider.Region) {
			score += cfg.RegionDiversityPenalty
		} else {
			score -= cfg.PreferenceBonus * 0.30
		}
		if cand.p95 < primary.p95 {
			score -= 0.08
		}
		if cand.cost < primary.cost {
			score -= 0.05
		}
		if score < bestScore {
			bestScore = score
			best = cand
		}
	}
	if best == nil {
		return nil, 0, []string{"hedging was warranted, but no secondary provider met the diversity and budget constraints"}
	}

	delay := computeHedgeDelay(primary, *best, req, cfg)
	reasons := []string{
		fmt.Sprintf("hedge after %d ms because the primary tail or health signals justify a second lane", delay),
	}
	if normalizeKey(best.provider.Vendor) != normalizeKey(primary.provider.Vendor) {
		reasons = append(reasons, "selected a vendor-diverse hedge to reduce correlated failure risk")
	}
	if normalizeKey(best.provider.Region) != normalizeKey(primary.provider.Region) {
		reasons = append(reasons, "selected a region-diverse hedge to reduce regional tail spikes")
	}
	reasons = append(reasons, fmt.Sprintf("worst-case dual-send cost is %.4f USD", roundUSD(primary.cost+best.cost)))
	return best, delay, reasons
}

func shouldHedge(primary candidate, req RequestShape, cfg PlannerConfig) bool {
	if primary.provider.Health.ErrorRate1m >= cfg.HedgeErrorTrigger {
		return true
	}
	if primary.provider.Health.ThrottleRate1m >= cfg.HedgeThrottleTrigger {
		return true
	}
	if primary.saturation >= cfg.HedgeSaturationTrigger {
		return true
	}
	latencySLO := float64(req.LatencySLOMs)
	if latencySLO <= 0 {
		latencySLO = float64(cfg.DefaultLatencySLOMs)
	}
	return primary.p95 >= latencySLO*cfg.HedgeP95TriggerFraction
}

func computeHedgeDelay(primary, secondary candidate, req RequestShape, cfg PlannerConfig) int {
	delay := primary.p50*0.90 + (primary.p95-primary.p50)*0.25 + primary.provider.Latency.JitterMs*0.50
	if delay <= 0 {
		delay = primary.p50 * 0.80
	}
	if primary.provider.Health.ErrorRate1m >= cfg.HedgeErrorTrigger {
		delay *= 0.60
	}
	if primary.provider.Health.ThrottleRate1m >= cfg.HedgeThrottleTrigger {
		delay *= 0.65
	}
	if primary.saturation >= cfg.HedgeSaturationTrigger {
		delay *= 0.75
	}
	if secondary.p95 < primary.p95 {
		delay *= 0.90
	}
	if req.NeedsReasoning {
		delay *= 1.10
	}
	if req.LatencySLOMs > 0 {
		delay = math.Min(delay, float64(req.LatencySLOMs)*0.65)
	}
	delay = clamp(delay, float64(cfg.MinHedgeDelayMs), float64(cfg.MaxHedgeDelayMs))
	return int(math.Round(delay))
}

func predictHedgedP95(primary, hedge candidate, hedgeDelayMs int) int {
	hedgedTail := math.Min(primary.p95, float64(hedgeDelayMs)+hedge.p95)
	return int(math.Round(hedgedTail))
}

func (c candidate) selectedProvider() SelectedProvider {
	reasons := append([]string(nil), c.reasons...)
	sort.Strings(reasons)
	return SelectedProvider{
		Name:             c.provider.Name,
		Vendor:           c.provider.Vendor,
		Region:           c.provider.Region,
		Model:            c.provider.Model,
		Score:            roundFloat(c.score, 4),
		EstimatedCostUSD: roundUSD(c.cost),
		PredictedP50Ms:   int(math.Round(c.p50)),
		PredictedP95Ms:   int(math.Round(c.p95)),
		Saturation:       roundFloat(c.saturation, 4),
		Reasons:          reasons,
	}
}

// NewObservationStore creates an in-process helper for keeping provider snapshots fresh.
func NewObservationStore(halfLife time.Duration) *ObservationStore {
	if halfLife <= 0 {
		halfLife = 2 * time.Minute
	}
	return &ObservationStore{
		halfLife:  halfLife,
		windowCap: 256,
		now:       time.Now,
		providers: make(map[string]*liveStats),
	}
}

// Start increments inflight counters and returns a finisher closure.
func (s *ObservationStore) Start(name string) func(Observation) {
	startedAt := s.now()
	s.mu.Lock()
	state := s.ensureLocked(name)
	state.inflight++
	s.mu.Unlock()

	var once sync.Once
	return func(obs Observation) {
		once.Do(func() {
			if obs.Latency <= 0 {
				obs.Latency = s.now().Sub(startedAt)
			}
			s.Record(name, obs)
		})
	}
}

// Record updates the live view for one provider.
func (s *ObservationStore) Record(name string, obs Observation) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.ensureLocked(name)
	if state.inflight > 0 {
		state.inflight--
	}
	if obs.Latency > 0 {
		state.latencies.Add(float64(obs.Latency.Milliseconds()))
		state.warm = state.warm || state.latencies.Len() >= 3
	}
	state.errorEWMA = decayEWMA(state.errorEWMA, sampleToFloat(obs.Err != nil), state.lastUpdatedAt, now, s.halfLife)
	state.throttleEWMA = decayEWMA(state.throttleEWMA, sampleToFloat(obs.Throttled || obs.HTTPStatus == 429 || obs.HTTPStatus == 529), state.lastUpdatedAt, now, s.halfLife)
	state.lastUpdatedAt = now
}

// Apply merges live observation data into one provider snapshot.
func (s *ObservationStore) Apply(base ProviderSnapshot) ProviderSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.providers[normalizeKey(base.Name)]
	if !ok {
		return base
	}
	base.Capacity.InFlight = state.inflight
	base.Health.Warm = base.Health.Warm || state.warm
	base.Health.ErrorRate1m = maxFloat(base.Health.ErrorRate1m, state.errorEWMA)
	base.Health.ThrottleRate1m = maxFloat(base.Health.ThrottleRate1m, state.throttleEWMA)
	if q := state.latencies.Quantile(0.50); q > 0 {
		base.Latency.P50Ms = q
	}
	if q := state.latencies.Quantile(0.95); q > 0 {
		base.Latency.P95Ms = q
	}
	return base
}

// ApplyAll merges live observation data into a full provider list.
func (s *ObservationStore) ApplyAll(bases []ProviderSnapshot) []ProviderSnapshot {
	out := make([]ProviderSnapshot, len(bases))
	for i := range bases {
		out[i] = s.Apply(bases[i])
	}
	return out
}

func (s *ObservationStore) ensureLocked(name string) *liveStats {
	key := normalizeKey(name)
	if state, ok := s.providers[key]; ok {
		return state
	}
	state := &liveStats{latencies: NewLatencyWindow(s.windowCap)}
	s.providers[key] = state
	return state
}

func NewLatencyWindow(size int) LatencyWindow {
	if size <= 0 {
		size = 1
	}
	return LatencyWindow{values: make([]float64, size)}
}

func (w *LatencyWindow) Add(value float64) {
	if len(w.values) == 0 {
		return
	}
	w.values[w.next] = value
	w.next = (w.next + 1) % len(w.values)
	if w.count < len(w.values) {
		w.count++
	}
}

func (w LatencyWindow) Len() int {
	return w.count
}

func (w LatencyWindow) Snapshot() []float64 {
	out := make([]float64, w.count)
	if w.count == 0 {
		return out
	}
	if w.count < len(w.values) {
		copy(out, w.values[:w.count])
		return out
	}
	copy(out, w.values)
	return out
}

func (w LatencyWindow) Quantile(q float64) float64 {
	values := w.Snapshot()
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	q = clamp(q, 0, 1)
	if len(values) == 1 {
		return values[0]
	}
	position := q * float64(len(values)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return values[lower]
	}
	weight := position - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}

func decayEWMA(previous, sample float64, lastUpdated, now time.Time, halfLife time.Duration) float64 {
	if lastUpdated.IsZero() {
		return sample
	}
	if halfLife <= 0 {
		return sample
	}
	delta := now.Sub(lastUpdated)
	if delta <= 0 {
		delta = time.Second
	}
	weight := 1 - math.Exp(-math.Ln2*delta.Seconds()/halfLife.Seconds())
	return previous*(1-weight) + sample*weight
}

func sampleToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func providerLabel(p ProviderSnapshot) string {
	name := strings.TrimSpace(p.Name)
	if name != "" {
		return name
	}
	if strings.TrimSpace(p.Model) != "" {
		return p.Model
	}
	if strings.TrimSpace(p.Vendor) != "" {
		return p.Vendor
	}
	return "unknown-provider"
}

func normalizeKey(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func clamp(v, low, high float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return low
	}
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

func defaultFloat(v, fallback float64) float64 {
	if v == 0 {
		return fallback
	}
	return v
}

func roundUSD(v float64) float64 {
	return roundFloat(v, 6)
}

func roundFloat(v float64, places int) float64 {
	factor := math.Pow(10, float64(places))
	return math.Round(v*factor) / factor
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "InferenceHedgePlanner.go")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  go run InferenceHedgePlanner.go plan [-input file] [-pretty]")
	fmt.Fprintln(w, "  go run InferenceHedgePlanner.go example")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "The plan command reads PlanInput JSON from a file or stdin and prints a RoutingPlan JSON document.")
}

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}

	switch os.Args[1] {
	case "plan":
		if err := runPlan(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "example":
		if err := writeJSON(os.Stdout, exampleInput(), true); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
}

func runPlan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	inputPath := fs.String("input", "", "path to input JSON; reads stdin when omitted")
	pretty := fs.Bool("pretty", true, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	payload, err := readInput(*inputPath)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(payload))) == 0 {
		return fmt.Errorf("empty plan input")
	}

	input, err := decodePlanInput(payload)
	if err != nil {
		return err
	}
	plan, err := PlanRouting(input)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, plan, *pretty)
}

func readInput(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func decodePlanInput(payload []byte) (PlanInput, error) {
	var input PlanInput
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&input); err != nil {
		return PlanInput{}, fmt.Errorf("decode plan input: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return PlanInput{}, fmt.Errorf("plan input must contain exactly one JSON object")
	}
	return input, nil
}

func writeJSON(w io.Writer, v any, pretty bool) error {
	enc := json.NewEncoder(w)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(v)
}

func exampleInput() PlanInput {
	return PlanInput{
		Request: RequestShape{
			ID:                   "req_demo_hedge_001",
			InputTokens:          14000,
			CachedInputTokens:    4000,
			ExpectedOutputTokens: 1800,
			NeedsJSON:            true,
			NeedsTools:           true,
			NeedsReasoning:       true,
			WantsStreaming:       true,
			LatencySLOMs:         2200,
			MaxCostUSD:           0.16,
			HedgeAllowed:         true,
			MaxParallel:          2,
			PreferredVendors:     []string{"openai", "anthropic"},
		},
		Providers: []ProviderSnapshot{
			{
				Name:   "openai-us-east-primary",
				Vendor: "openai",
				Region: "us-east-1",
				Model:  "gpt-5.1",
				Capabilities: CapabilitySet{
					SupportsJSON:      true,
					SupportsTools:     true,
					SupportsReasoning: true,
					SupportsStreaming: true,
					MaxContextTokens:  256000,
					MaxOutputTokens:   32000,
				},
				Cost: CostModel{
					InputPer1KUSD:       0.0028,
					CachedInputPer1KUSD: 0.0008,
					OutputPer1KUSD:      0.0110,
				},
				Latency: LatencyProfile{P50Ms: 820, P95Ms: 2750, JitterMs: 110},
				Health:  HealthWindow{ErrorRate1m: 0.03, ThrottleRate1m: 0.11, Warm: true},
				Capacity: CapacityWindow{InFlight: 19, ConcurrencyLimit: 24},
			},
			{
				Name:   "anthropic-eu-west-fallback",
				Vendor: "anthropic",
				Region: "eu-west-1",
				Model:  "claude-4.1-sonnet",
				Capabilities: CapabilitySet{
					SupportsJSON:      true,
					SupportsTools:     true,
					SupportsReasoning: true,
					SupportsStreaming: true,
					MaxContextTokens:  200000,
					MaxOutputTokens:   32000,
				},
				Cost: CostModel{
					InputPer1KUSD:       0.0030,
					CachedInputPer1KUSD: 0.0010,
					OutputPer1KUSD:      0.0150,
				},
				Latency: LatencyProfile{P50Ms: 710, P95Ms: 1620, JitterMs: 90},
				Health:  HealthWindow{ErrorRate1m: 0.01, ThrottleRate1m: 0.02, Warm: true},
				Capacity: CapacityWindow{InFlight: 8, ConcurrencyLimit: 20},
			},
			{
				Name:   "vertex-us-central-safety-net",
				Vendor: "google",
				Region: "us-central1",
				Model:  "gemini-2.8-pro",
				Capabilities: CapabilitySet{
					SupportsJSON:      true,
					SupportsTools:     true,
					SupportsReasoning: true,
					SupportsStreaming: true,
					MaxContextTokens:  200000,
					MaxOutputTokens:   32000,
				},
				Cost: CostModel{
					InputPer1KUSD:       0.0027,
					CachedInputPer1KUSD: 0.0009,
					OutputPer1KUSD:      0.0135,
				},
				Latency: LatencyProfile{P50Ms: 930, P95Ms: 2100, JitterMs: 130},
				Health:  HealthWindow{ErrorRate1m: 0.02, ThrottleRate1m: 0.04, Warm: false},
				Capacity: CapacityWindow{InFlight: 6, ConcurrencyLimit: 18},
			},
		},
	}
}

/*
This solves multi-provider LLM routing when one model or region suddenly goes long-tail, starts throwing 429s, or quietly becomes the most expensive path for the same request. Built because by April 2026 a lot of production AI systems are no longer single-provider. Teams are splitting traffic across OpenAI, Anthropic, Gemini, local gateways, and regional failover stacks, but most routing code is still just a pile of if statements, static priorities, and hand-wavy latency guesses.

Use it when you need a real planner for structured outputs, tool calling, reasoning-heavy prompts, or streaming requests and you want one place to score providers by tail latency, cost, error rate, throttle rate, and concurrency pressure. The trick: it does two jobs cleanly. First, it rejects providers that are obviously wrong for the request or already too unhealthy. Then it ranks the remaining ones, decides whether a hedge is actually justified, and computes a hedge delay that reacts to tail risk instead of blindly duplicating every request.

Drop this into a Go service that already collects provider telemetry, or use the JSON CLI mode to test routing plans in CI before you wire it into a gateway. If you are searching for Go inference hedging planner, LLM provider failover router, OpenAI Anthropic Gemini hedge controller, tail latency planner, or cost-aware AI routing logic, this file is meant to be a practical starting point that you can fork and keep in production.
*/
