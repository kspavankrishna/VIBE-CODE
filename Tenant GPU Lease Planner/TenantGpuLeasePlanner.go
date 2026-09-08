package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

const maxInputBytes = 8 * 1024 * 1024

type PlannerRequest struct {
	Now     string         `json:"now,omitempty"`
	Policy  PlannerPolicy  `json:"policy,omitempty"`
	Pools   []GpuPool      `json:"pools"`
	Tenants []TenantDemand `json:"tenants"`
}

type PlannerPolicy struct {
	AllowCrossRegion       bool    `json:"allowCrossRegion,omitempty"`
	OvercommitRatio        float64 `json:"overcommitRatio,omitempty"`
	MinHealthScore         float64 `json:"minHealthScore,omitempty"`
	MaxPreemptionRate      float64 `json:"maxPreemptionRate,omitempty"`
	MaxCarbonGramsPerHour  float64 `json:"maxCarbonGramsPerHour,omitempty"`
	CostWeight             float64 `json:"costWeight,omitempty"`
	CarbonWeight           float64 `json:"carbonWeight,omitempty"`
	LatencyWeight          float64 `json:"latencyWeight,omitempty"`
	ReliabilityWeight      float64 `json:"reliabilityWeight,omitempty"`
	LocalityWeight         float64 `json:"localityWeight,omitempty"`
	PreferReservedCapacity bool    `json:"preferReservedCapacity,omitempty"`
	ExplainRejected        bool    `json:"explainRejected,omitempty"`
}

type TenantDemand struct {
	ID                          string   `json:"id"`
	Model                       string   `json:"model"`
	Region                      string   `json:"region,omitempty"`
	ResidencyTags               []string `json:"residencyTags,omitempty"`
	PromptTokens                int      `json:"promptTokens,omitempty"`
	CompletionTokens            int      `json:"completionTokens,omitempty"`
	RequestsPerSecond           float64  `json:"requestsPerSecond,omitempty"`
	PeakConcurrentRequests      int      `json:"peakConcurrentRequests,omitempty"`
	P99LatencyMs                float64  `json:"p99LatencyMs,omitempty"`
	MinReplicas                 int      `json:"minReplicas,omitempty"`
	MaxReplicas                 int      `json:"maxReplicas,omitempty"`
	ModelMemoryGB               float64  `json:"modelMemoryGb,omitempty"`
	AdapterMemoryGB             float64  `json:"adapterMemoryGb,omitempty"`
	RuntimeOverheadGB           float64  `json:"runtimeOverheadGb,omitempty"`
	KVCachePerRequestMB         float64  `json:"kvCachePerRequestMb,omitempty"`
	Priority                    int      `json:"priority,omitempty"`
	BudgetPerHourUSD            float64  `json:"budgetPerHourUsd,omitempty"`
	MustRun                     bool     `json:"mustRun,omitempty"`
	AllowsSpot                  bool     `json:"allowsSpot,omitempty"`
	RequiresConfidentialCompute bool     `json:"requiresConfidentialCompute,omitempty"`
}

type GpuPool struct {
	ID                    string   `json:"id"`
	Region                string   `json:"region"`
	GpuType               string   `json:"gpuType"`
	Count                 int      `json:"count"`
	VRAMGB                float64  `json:"vramGb"`
	TokensPerSecond       float64  `json:"tokensPerSecond"`
	HourlyPriceUSD        float64  `json:"hourlyPriceUsd"`
	PowerWatts            float64  `json:"powerWatts,omitempty"`
	CarbonIntensityGCO2KWh float64 `json:"carbonIntensityGco2Kwh,omitempty"`
	PreemptionRate        float64  `json:"preemptionRate,omitempty"`
	HealthScore           float64  `json:"healthScore,omitempty"`
	Spot                  bool     `json:"spot,omitempty"`
	ConfidentialCompute   bool     `json:"confidentialCompute,omitempty"`
	AvailableFrom         string   `json:"availableFrom,omitempty"`
	AvailableUntil        string   `json:"availableUntil,omitempty"`
	Capabilities          []string `json:"capabilities,omitempty"`
	ResidencyTags         []string `json:"residencyTags,omitempty"`
}

type PlanResult struct {
	GeneratedAt     string            `json:"generatedAt"`
	Summary         PlanSummary       `json:"summary"`
	Allocations     []Allocation      `json:"allocations"`
	Rejected        []RejectedTenant  `json:"rejected,omitempty"`
	PoolUtilization []PoolUtilization `json:"poolUtilization"`
	Warnings        []string          `json:"warnings,omitempty"`
}

type PlanSummary struct {
	TenantsRequested        int     `json:"tenantsRequested"`
	TenantsPlaced           int     `json:"tenantsPlaced"`
	TenantsRejected         int     `json:"tenantsRejected"`
	TotalGpuLeases          int     `json:"totalGpuLeases"`
	EstimatedHourlyCostUSD  float64 `json:"estimatedHourlyCostUsd"`
	EstimatedCarbonGramsHr  float64 `json:"estimatedCarbonGramsPerHour"`
	MinimumPlannerScore     float64 `json:"minimumPlannerScore"`
	UsedCrossRegionFallback bool    `json:"usedCrossRegionFallback"`
}

type Allocation struct {
	TenantID                   string   `json:"tenantId"`
	Model                      string   `json:"model"`
	PoolID                     string   `json:"poolId"`
	Region                     string   `json:"region"`
	GpuType                    string   `json:"gpuType"`
	Replicas                   int      `json:"replicas"`
	GpuLeases                  int      `json:"gpuLeases"`
	EstimatedTokensPerSecond   float64  `json:"estimatedTokensPerSecond"`
	EstimatedP99LatencyMs      float64  `json:"estimatedP99LatencyMs"`
	EstimatedCostHourlyUSD     float64  `json:"estimatedCostHourlyUsd"`
	EstimatedCarbonGramsHourly float64  `json:"estimatedCarbonGramsHourly"`
	HeadroomRatio              float64  `json:"headroomRatio"`
	PlannerScore               float64  `json:"plannerScore"`
	Reasons                    []string `json:"reasons"`
}

type RejectedTenant struct {
	TenantID          string   `json:"tenantId"`
	Reason            string   `json:"reason"`
	CandidateFailures []string `json:"candidateFailures,omitempty"`
}

type PoolUtilization struct {
	PoolID                    string   `json:"poolId"`
	Region                    string   `json:"region"`
	GpuType                   string   `json:"gpuType"`
	AllocatedGpuLeases        int      `json:"allocatedGpuLeases"`
	TotalGpuLeases            int      `json:"totalGpuLeases"`
	RemainingGpuLeases        int      `json:"remainingGpuLeases"`
	EstimatedHourlyCostUSD    float64  `json:"estimatedHourlyCostUsd"`
	EstimatedCarbonGramsHourly float64 `json:"estimatedCarbonGramsHourly"`
	Tenants                   []string `json:"tenants,omitempty"`
}

type candidate struct {
	poolIndex       int
	replicas        int
	estimatedTPS    float64
	estimatedP99Ms  float64
	costHourly     float64
	carbonHourly   float64
	headroomRatio   float64
	plannerScore   float64
	crossRegion    bool
	reasons         []string
}

type poolState struct {
	pool             GpuPool
	remaining        int
	allocated        int
	costHourly       float64
	carbonHourly     float64
	tenants           []string
}

func main() {
	pretty := flag.Bool("pretty", false, "pretty print the JSON plan")
	example := flag.Bool("example", false, "print an example planning request")
	flag.Parse()

	if *example {
		writeJSON(os.Stdout, exampleRequest(), *pretty)
		return
	}

	input, err := io.ReadAll(io.LimitReader(os.Stdin, maxInputBytes+1))
	if err != nil {
		fatal(err)
	}
	if len(input) > maxInputBytes {
		fatal(fmt.Errorf("input is larger than %d bytes", maxInputBytes))
	}
	if len(bytes.TrimSpace(input)) == 0 {
		fatal(errors.New("expected a planner request on stdin; run with --example for a template"))
	}

	var request PlannerRequest
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		fatal(fmt.Errorf("decode request: %w", err))
	}

	plan, err := Plan(request)
	if err != nil {
		fatal(err)
	}
	if err := writeJSON(os.Stdout, plan, *pretty); err != nil {
		fatal(err)
	}
}

func Plan(request PlannerRequest) (*PlanResult, error) {
	policy := normalizePolicy(request.Policy)
	now, err := plannerTime(request.Now)
	if err != nil {
		return nil, err
	}

	pools, warnings, err := normalizePools(request.Pools)
	if err != nil {
		return nil, err
	}
	tenants, tenantWarnings, err := normalizeTenants(request.Tenants)
	if err != nil {
		return nil, err
	}
	warnings = append(warnings, tenantWarnings...)

	states := make([]poolState, len(pools))
	for i, pool := range pools {
		states[i] = poolState{pool: pool, remaining: pool.Count}
	}

	sort.SliceStable(tenants, func(i, j int) bool {
		if tenants[i].MustRun != tenants[j].MustRun {
			return tenants[i].MustRun
		}
		if tenants[i].Priority != tenants[j].Priority {
			return tenants[i].Priority > tenants[j].Priority
		}
		return tenants[i].ID < tenants[j].ID
	})

	result := &PlanResult{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Warnings:    warnings,
	}
	minScore := math.Inf(1)

	for _, tenant := range tenants {
		choices, failures := rankCandidates(tenant, states, policy, now)
		if len(choices) == 0 {
			rejected := RejectedTenant{TenantID: tenant.ID, Reason: "no GPU pool satisfied memory, throughput, latency, budget, residency, and risk constraints"}
			if policy.ExplainRejected {
				rejected.CandidateFailures = failures
			}
			result.Rejected = append(result.Rejected, rejected)
			continue
		}

		choice := choices[0]
		state := &states[choice.poolIndex]
		state.remaining -= choice.replicas
		state.allocated += choice.replicas
		state.costHourly += choice.costHourly
		state.carbonHourly += choice.carbonHourly
		state.tenants = append(state.tenants, tenant.ID)

		if choice.plannerScore < minScore {
			minScore = choice.plannerScore
		}
		result.Allocations = append(result.Allocations, Allocation{
			TenantID:                   tenant.ID,
			Model:                      tenant.Model,
			PoolID:                     state.pool.ID,
			Region:                     state.pool.Region,
			GpuType:                    state.pool.GpuType,
			Replicas:                   choice.replicas,
			GpuLeases:                  choice.replicas,
			EstimatedTokensPerSecond:   round(choice.estimatedTPS, 2),
			EstimatedP99LatencyMs:      round(choice.estimatedP99Ms, 2),
			EstimatedCostHourlyUSD:     round(choice.costHourly, 4),
			EstimatedCarbonGramsHourly: round(choice.carbonHourly, 2),
			HeadroomRatio:              round(choice.headroomRatio, 4),
			PlannerScore:               round(choice.plannerScore, 6),
			Reasons:                    choice.reasons,
		})
	}

	for _, state := range states {
		utilization := PoolUtilization{
			PoolID:                    state.pool.ID,
			Region:                    state.pool.Region,
			GpuType:                   state.pool.GpuType,
			AllocatedGpuLeases:        state.allocated,
			TotalGpuLeases:            state.pool.Count,
			RemainingGpuLeases:        state.remaining,
			EstimatedHourlyCostUSD:    round(state.costHourly, 4),
			EstimatedCarbonGramsHourly: round(state.carbonHourly, 2),
			Tenants:                   append([]string(nil), state.tenants...),
		}
		result.PoolUtilization = append(result.PoolUtilization, utilization)
	}

	for _, allocation := range result.Allocations {
		result.Summary.TotalGpuLeases += allocation.GpuLeases
		result.Summary.EstimatedHourlyCostUSD += allocation.EstimatedCostHourlyUSD
		result.Summary.EstimatedCarbonGramsHr += allocation.EstimatedCarbonGramsHourly
		result.Summary.UsedCrossRegionFallback = result.Summary.UsedCrossRegionFallback || allocation.Region != tenantRegionByID(tenants, allocation.TenantID)
	}
	result.Summary.TenantsRequested = len(tenants)
	result.Summary.TenantsPlaced = len(result.Allocations)
	result.Summary.TenantsRejected = len(result.Rejected)
	result.Summary.EstimatedHourlyCostUSD = round(result.Summary.EstimatedHourlyCostUSD, 4)
	result.Summary.EstimatedCarbonGramsHr = round(result.Summary.EstimatedCarbonGramsHr, 2)
	if math.IsInf(minScore, 1) {
		result.Summary.MinimumPlannerScore = 0
	} else {
		result.Summary.MinimumPlannerScore = round(minScore, 6)
	}

	return result, nil
}

func rankCandidates(tenant TenantDemand, states []poolState, policy PlannerPolicy, now time.Time) ([]candidate, []string) {
	var candidates []candidate
	var failures []string
	for i := range states {
		choice, reason, ok := evaluateCandidate(tenant, states[i], i, policy, now)
		if ok {
			candidates = append(candidates, choice)
		} else {
			failures = append(failures, fmt.Sprintf("%s: %s", states[i].pool.ID, reason))
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].plannerScore != candidates[j].plannerScore {
			return candidates[i].plannerScore > candidates[j].plannerScore
		}
		if candidates[i].costHourly != candidates[j].costHourly {
			return candidates[i].costHourly < candidates[j].costHourly
		}
		if candidates[i].carbonHourly != candidates[j].carbonHourly {
			return candidates[i].carbonHourly < candidates[j].carbonHourly
		}
		return states[candidates[i].poolIndex].pool.ID < states[candidates[j].poolIndex].pool.ID
	})
	return candidates, failures
}

func evaluateCandidate(tenant TenantDemand, state poolState, poolIndex int, policy PlannerPolicy, now time.Time) (candidate, string, bool) {
	pool := state.pool
	if state.remaining <= 0 {
		return candidate{}, "no remaining GPU leases", false
	}
	if !poolAvailable(pool, now) {
		return candidate{}, "pool is outside its availability window", false
	}
	if tenant.Region != "" && pool.Region != tenant.Region && !policy.AllowCrossRegion {
		return candidate{}, "cross-region placement is disabled", false
	}
	if len(tenant.ResidencyTags) > 0 && !intersects(tenant.ResidencyTags, pool.ResidencyTags) {
		return candidate{}, "pool does not match required data residency tags", false
	}
	if tenant.RequiresConfidentialCompute && !pool.ConfidentialCompute {
		return candidate{}, "tenant requires confidential compute", false
	}
	if pool.Spot && !tenant.AllowsSpot {
		return candidate{}, "spot pool rejected by tenant policy", false
	}
	if pool.HealthScore < policy.MinHealthScore {
		return candidate{}, "pool health score is below policy minimum", false
	}
	if pool.PreemptionRate > policy.MaxPreemptionRate && !tenant.AllowsSpot {
		return candidate{}, "pool preemption risk is above policy maximum", false
	}

	replicas, p99, memoryGB, ok := findReplicaCount(tenant, pool, state.remaining, policy.OvercommitRatio)
	if !ok {
		return candidate{}, "no replica count fits VRAM, throughput, latency, and remaining leases", false
	}

	estimatedTPS := float64(replicas) * pool.TokensPerSecond
	requiredTPS := requiredThroughputTPS(tenant)
	headroom := estimatedTPS / math.Max(1, requiredTPS)
	cost := float64(replicas) * pool.HourlyPriceUSD
	carbon := carbonGramsPerHour(pool, replicas)
	if tenant.BudgetPerHourUSD > 0 && cost > tenant.BudgetPerHourUSD {
		return candidate{}, fmt.Sprintf("hourly cost %.4f exceeds tenant budget %.4f", cost, tenant.BudgetPerHourUSD), false
	}
	if policy.MaxCarbonGramsPerHour > 0 && carbon > policy.MaxCarbonGramsPerHour {
		return candidate{}, fmt.Sprintf("carbon estimate %.2f g/h exceeds policy %.2f g/h", carbon, policy.MaxCarbonGramsPerHour), false
	}

	reasons := []string{
		fmt.Sprintf("%d replica leases satisfy %.2f required tokens/s with %.2fx headroom", replicas, requiredTPS, headroom),
		fmt.Sprintf("estimated %.2f GB per replica fits %.2f GB VRAM", memoryGB, pool.VRAMGB),
	}
	if tenant.Region != "" && tenant.Region != pool.Region {
		reasons = append(reasons, "used cross-region fallback")
	}
	if pool.Spot {
		reasons = append(reasons, "spot capacity accepted by tenant policy")
	}
	if pool.ConfidentialCompute {
		reasons = append(reasons, "confidential compute available")
	}

	choice := candidate{
		poolIndex:      poolIndex,
		replicas:       replicas,
		estimatedTPS:   estimatedTPS,
		estimatedP99Ms: p99,
		costHourly:    cost,
		carbonHourly:  carbon,
		headroomRatio:  headroom,
		crossRegion:   tenant.Region != "" && tenant.Region != pool.Region,
		reasons:        reasons,
	}
	choice.plannerScore = scoreCandidate(tenant, pool, choice, policy)
	return choice, "", true
}

func findReplicaCount(tenant TenantDemand, pool GpuPool, remaining int, overcommit float64) (int, float64, float64, bool) {
	minReplicas := maxInt(1, tenant.MinReplicas)
	maxReplicas := tenant.MaxReplicas
	if maxReplicas <= 0 || maxReplicas > remaining {
		maxReplicas = remaining
	}
	if maxReplicas < minReplicas {
		return 0, 0, 0, false
	}

	for replicas := minReplicas; replicas <= maxReplicas; replicas++ {
		memoryGB := replicaMemoryGB(tenant, replicas)
		if memoryGB > pool.VRAMGB*overcommit {
			continue
		}
		capacityTPS := float64(replicas) * pool.TokensPerSecond
		if capacityTPS < requiredThroughputTPS(tenant) {
			continue
		}
		p99 := predictedP99LatencyMs(tenant, pool, replicas)
		if tenant.P99LatencyMs > 0 && p99 > tenant.P99LatencyMs {
			continue
		}
		return replicas, p99, memoryGB, true
	}
	return 0, 0, 0, false
}

func replicaMemoryGB(tenant TenantDemand, replicas int) float64 {
	concurrencyPerReplica := 1
	if tenant.PeakConcurrentRequests > 0 {
		concurrencyPerReplica = int(math.Ceil(float64(tenant.PeakConcurrentRequests) / float64(maxInt(1, replicas))))
	}
	kvMB := tenant.KVCachePerRequestMB
	if kvMB <= 0 {
		kvMB = estimateKVCacheMB(tenant)
	}
	return tenant.ModelMemoryGB + tenant.AdapterMemoryGB + tenant.RuntimeOverheadGB + (float64(concurrencyPerReplica)*kvMB)/1024.0
}

func predictedP99LatencyMs(tenant TenantDemand, pool GpuPool, replicas int) float64 {
	sequenceTokens := maxInt(1, tenant.PromptTokens+tenant.CompletionTokens)
	serviceMs := (float64(sequenceTokens) / pool.TokensPerSecond) * 1000.0
	capacityTPS := math.Max(1, float64(replicas)*pool.TokensPerSecond)
	utilization := clamp(requiredThroughputTPS(tenant)/capacityTPS, 0, 0.98)
	queuePenalty := 1.0 + math.Pow(utilization, 4)/math.Max(0.05, 1.0-utilization)
	return 15.0 + serviceMs*queuePenalty
}

func requiredThroughputTPS(tenant TenantDemand) float64 {
	sequenceTokens := float64(maxInt(1, tenant.PromptTokens+tenant.CompletionTokens))
	fromRPS := tenant.RequestsPerSecond * sequenceTokens
	fromConcurrency := 0.0
	if tenant.PeakConcurrentRequests > 0 && tenant.P99LatencyMs > 0 {
		fromConcurrency = (float64(tenant.PeakConcurrentRequests) * sequenceTokens) / math.Max(0.001, tenant.P99LatencyMs/1000.0)
		fromConcurrency *= 0.75
	}
	return math.Max(1.0, math.Max(fromRPS, fromConcurrency))
}

func scoreCandidate(tenant TenantDemand, pool GpuPool, choice candidate, policy PlannerPolicy) float64 {
	costScore := 1.0 / (1.0 + choice.costHourly)
	if tenant.BudgetPerHourUSD > 0 {
		costScore = clamp(1.0-choice.costHourly/tenant.BudgetPerHourUSD, 0, 1)
	}

	carbonScore := 1.0 / (1.0 + choice.carbonHourly/1000.0)
	if policy.MaxCarbonGramsPerHour > 0 {
		carbonScore = clamp(1.0-choice.carbonHourly/policy.MaxCarbonGramsPerHour, 0, 1)
	}

	latencyScore := clamp(choice.headroomRatio/3.0, 0, 1)
	if tenant.P99LatencyMs > 0 {
		latencyScore = clamp(1.0-choice.estimatedP99Ms/tenant.P99LatencyMs, 0, 1)
	}

	reliabilityScore := clamp(pool.HealthScore*(1.0-pool.PreemptionRate), 0, 1)
	if policy.PreferReservedCapacity && pool.Spot {
		reliabilityScore *= 0.75
	}

	localityScore := 1.0
	if tenant.Region != "" && tenant.Region != pool.Region {
		localityScore = 0.35
	}

	return policy.CostWeight*costScore + policy.CarbonWeight*carbonScore + policy.LatencyWeight*latencyScore + policy.ReliabilityWeight*reliabilityScore + policy.LocalityWeight*localityScore
}

func normalizePolicy(policy PlannerPolicy) PlannerPolicy {
	if policy.OvercommitRatio <= 0 {
		policy.OvercommitRatio = 1.0
	}
	policy.OvercommitRatio = clamp(policy.OvercommitRatio, 0.5, 1.25)
	if policy.MinHealthScore <= 0 {
		policy.MinHealthScore = 0.70
	}
	if policy.MaxPreemptionRate <= 0 {
		policy.MaxPreemptionRate = 0.08
	}
	if policy.CostWeight == 0 && policy.CarbonWeight == 0 && policy.LatencyWeight == 0 && policy.ReliabilityWeight == 0 && policy.LocalityWeight == 0 {
		policy.CostWeight = 0.24
		policy.CarbonWeight = 0.14
		policy.LatencyWeight = 0.22
		policy.ReliabilityWeight = 0.30
		policy.LocalityWeight = 0.10
	}
	weightSum := policy.CostWeight + policy.CarbonWeight + policy.LatencyWeight + policy.ReliabilityWeight + policy.LocalityWeight
	if weightSum <= 0 {
		weightSum = 1
	}
	policy.CostWeight /= weightSum
	policy.CarbonWeight /= weightSum
	policy.LatencyWeight /= weightSum
	policy.ReliabilityWeight /= weightSum
	policy.LocalityWeight /= weightSum
	return policy
}

func normalizePools(pools []GpuPool) ([]GpuPool, []string, error) {
	if len(pools) == 0 {
		return nil, nil, errors.New("at least one GPU pool is required")
	}
	seen := map[string]bool{}
	var warnings []string
	out := make([]GpuPool, 0, len(pools))
	for _, pool := range pools {
		pool.ID = strings.TrimSpace(pool.ID)
		pool.Region = strings.TrimSpace(pool.Region)
		pool.GpuType = strings.TrimSpace(pool.GpuType)
		if pool.ID == "" {
			return nil, nil, errors.New("gpu pool id is required")
		}
		if seen[pool.ID] {
			return nil, nil, fmt.Errorf("duplicate gpu pool id %q", pool.ID)
		}
		seen[pool.ID] = true
		if pool.Region == "" {
			return nil, nil, fmt.Errorf("gpu pool %q is missing region", pool.ID)
		}
		if pool.GpuType == "" {
			return nil, nil, fmt.Errorf("gpu pool %q is missing gpuType", pool.ID)
		}
		if pool.Count <= 0 {
			return nil, nil, fmt.Errorf("gpu pool %q must have positive count", pool.ID)
		}
		if pool.VRAMGB <= 0 {
			return nil, nil, fmt.Errorf("gpu pool %q must have positive vramGb", pool.ID)
		}
		if pool.TokensPerSecond <= 0 {
			return nil, nil, fmt.Errorf("gpu pool %q must have positive tokensPerSecond", pool.ID)
		}
		if pool.HourlyPriceUSD < 0 {
			return nil, nil, fmt.Errorf("gpu pool %q has negative hourlyPriceUsd", pool.ID)
		}
		if pool.HealthScore <= 0 {
			pool.HealthScore = 1.0
			warnings = append(warnings, fmt.Sprintf("gpu pool %s defaulted healthScore to 1.0", pool.ID))
		}
		pool.HealthScore = clamp(pool.HealthScore, 0, 1)
		pool.PreemptionRate = clamp(pool.PreemptionRate, 0, 1)
		if pool.PowerWatts < 0 || pool.CarbonIntensityGCO2KWh < 0 {
			return nil, nil, fmt.Errorf("gpu pool %q has negative carbon inputs", pool.ID)
		}
		out = append(out, pool)
	}
	return out, warnings, nil
}

func normalizeTenants(tenants []TenantDemand) ([]TenantDemand, []string, error) {
	if len(tenants) == 0 {
		return nil, nil, errors.New("at least one tenant demand is required")
	}
	seen := map[string]bool{}
	var warnings []string
	out := make([]TenantDemand, 0, len(tenants))
	for _, tenant := range tenants {
		tenant.ID = strings.TrimSpace(tenant.ID)
		tenant.Model = strings.TrimSpace(tenant.Model)
		tenant.Region = strings.TrimSpace(tenant.Region)
		if tenant.ID == "" {
			return nil, nil, errors.New("tenant id is required")
		}
		if seen[tenant.ID] {
			return nil, nil, fmt.Errorf("duplicate tenant id %q", tenant.ID)
		}
		seen[tenant.ID] = true
		if tenant.Model == "" {
			return nil, nil, fmt.Errorf("tenant %q is missing model", tenant.ID)
		}
		if tenant.PromptTokens < 0 || tenant.CompletionTokens < 0 || tenant.PeakConcurrentRequests < 0 {
			return nil, nil, fmt.Errorf("tenant %q has negative token or concurrency input", tenant.ID)
		}
		if tenant.RequestsPerSecond < 0 || tenant.P99LatencyMs < 0 || tenant.BudgetPerHourUSD < 0 {
			return nil, nil, fmt.Errorf("tenant %q has negative rate, latency, or budget input", tenant.ID)
		}
		if tenant.ModelMemoryGB <= 0 {
			estimated := estimateModelMemoryGB(tenant.Model)
			if estimated <= 0 {
				return nil, nil, fmt.Errorf("tenant %q must set modelMemoryGb or use a model name containing a size like 7b, 70b, or 405b", tenant.ID)
			}
			tenant.ModelMemoryGB = estimated
			warnings = append(warnings, fmt.Sprintf("tenant %s estimated modelMemoryGb as %.2f from model name", tenant.ID, estimated))
		}
		if tenant.RuntimeOverheadGB <= 0 {
			tenant.RuntimeOverheadGB = 3.0
		}
		if tenant.PromptTokens == 0 && tenant.CompletionTokens == 0 {
			tenant.PromptTokens = 2048
			tenant.CompletionTokens = 512
			warnings = append(warnings, fmt.Sprintf("tenant %s defaulted token shape to 2048 prompt and 512 completion tokens", tenant.ID))
		}
		out = append(out, tenant)
	}
	return out, warnings, nil
}

func estimateModelMemoryGB(model string) float64 {
	lower := strings.ToLower(model)
	for _, suffix := range []string{"b", "B"} {
		_ = suffix
	}
	for i := 0; i < len(lower); i++ {
		if lower[i] < '0' || lower[i] > '9' {
			continue
		}
		j := i
		for j < len(lower) && ((lower[j] >= '0' && lower[j] <= '9') || lower[j] == '.') {
			j++
		}
		if j < len(lower) && lower[j] == 'b' {
			value, ok := parseFloat(lower[i:j])
			if ok {
				return value*1.15 + 2.0
			}
		}
	}
	return 0
}

func estimateKVCacheMB(tenant TenantDemand) float64 {
	sequence := float64(maxInt(1, tenant.PromptTokens+tenant.CompletionTokens))
	modelGB := math.Max(1, tenant.ModelMemoryGB)
	return clamp(sequence*modelGB*0.00018, 32, 4096)
}

func carbonGramsPerHour(pool GpuPool, replicas int) float64 {
	if pool.PowerWatts <= 0 || pool.CarbonIntensityGCO2KWh <= 0 {
		return 0
	}
	kWh := (float64(replicas) * pool.PowerWatts) / 1000.0
	return kWh * pool.CarbonIntensityGCO2KWh
}

func poolAvailable(pool GpuPool, now time.Time) bool {
	if pool.AvailableFrom != "" {
		from, err := time.Parse(time.RFC3339, pool.AvailableFrom)
		if err == nil && now.Before(from) {
			return false
		}
	}
	if pool.AvailableUntil != "" {
		until, err := time.Parse(time.RFC3339, pool.AvailableUntil)
		if err == nil && now.After(until) {
			return false
		}
	}
	return true
}

func plannerTime(value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Now().UTC(), nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("now must be RFC3339: %w", err)
	}
	return parsed.UTC(), nil
}

func tenantRegionByID(tenants []TenantDemand, id string) string {
	for _, tenant := range tenants {
		if tenant.ID == id {
			return tenant.Region
		}
	}
	return ""
}

func intersects(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, item := range right {
		seen[strings.ToLower(strings.TrimSpace(item))] = true
	}
	for _, item := range left {
		if seen[strings.ToLower(strings.TrimSpace(item))] {
			return true
		}
	}
	return false
}

func parseFloat(value string) (float64, bool) {
	var out float64
	_, err := fmt.Sscanf(value, "%f", &out)
	return out, err == nil
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func round(value float64, places int) float64 {
	factor := math.Pow10(places)
	return math.Round(value*factor) / factor
}

func writeJSON(writer io.Writer, value any, pretty bool) error {
	encoder := json.NewEncoder(writer)
	if pretty {
		encoder.SetIndent("", "  ")
	}
	return encoder.Encode(value)
}

func fatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "TenantGpuLeasePlanner: %v\n", err)
	os.Exit(1)
}

func exampleRequest() PlannerRequest {
	return PlannerRequest{
		Now: "2026-04-15T12:00:00Z",
		Policy: PlannerPolicy{
			AllowCrossRegion:      true,
			ExplainRejected:       true,
			MaxPreemptionRate:     0.10,
			MaxCarbonGramsPerHour: 900,
		},
		Pools: []GpuPool{
			{
				ID:                     "iad-h100-reserved",
				Region:                 "us-east-1",
				GpuType:                "h100-80gb",
				Count:                  8,
				VRAMGB:                 80,
				TokensPerSecond:        9800,
				HourlyPriceUSD:         3.90,
				PowerWatts:             700,
				CarbonIntensityGCO2KWh: 370,
				HealthScore:            0.99,
				ResidencyTags:          []string{"us"},
				ConfidentialCompute:    true,
			},
			{
				ID:                     "ord-l40s-spot",
				Region:                 "us-central-1",
				GpuType:                "l40s-48gb",
				Count:                  16,
				VRAMGB:                 48,
				TokensPerSecond:        3300,
				HourlyPriceUSD:         0.92,
				PowerWatts:             350,
				CarbonIntensityGCO2KWh: 290,
				PreemptionRate:         0.07,
				HealthScore:            0.96,
				Spot:                   true,
				ResidencyTags:          []string{"us"},
			},
		},
		Tenants: []TenantDemand{
			{
				ID:                          "rag-prod",
				Model:                       "llama-3.3-70b",
				Region:                      "us-east-1",
				ResidencyTags:               []string{"us"},
				PromptTokens:                6000,
				CompletionTokens:            800,
				RequestsPerSecond:           6.5,
				PeakConcurrentRequests:      96,
				P99LatencyMs:                1800,
				BudgetPerHourUSD:            30,
				RequiresConfidentialCompute: true,
				Priority:                    90,
			},
			{
				ID:                     "eval-batch",
				Model:                  "mistral-7b",
				Region:                 "us-east-1",
				PromptTokens:           4096,
				CompletionTokens:       1024,
				RequestsPerSecond:      2,
				PeakConcurrentRequests: 48,
				P99LatencyMs:           6000,
				BudgetPerHourUSD:       8,
				AllowsSpot:             true,
				Priority:               20,
			},
		},
	}
}

/*
This solves the April 2026 problem where teams running LLM inference, agent workloads, RAG APIs, batch evals, and private model serving need a clear GPU lease plan before they burn money on H100, B200, L40S, MI300, or spot capacity. Built because I kept seeing developers guess GPU counts from a spreadsheet and then miss p99 latency, VRAM, KV cache, data residency, carbon, and budget constraints once traffic arrived. Use it when you need a Go GPU lease planner, multi tenant inference scheduler, Kubernetes GPU capacity planner, carbon aware AI infrastructure tool, or production DevOps utility that reads JSON and returns a deterministic placement plan. The trick: it treats every pool as a constrained lease market, estimates replica memory from model size plus KV cache, checks throughput and latency together, then scores viable pools by cost, carbon, reliability, locality, and headroom instead of picking the cheapest GPU blindly. Drop this into a platform repo, CI capacity check, internal developer portal, or pre-deploy gate so Pavan can see exactly why a tenant landed on a pool, why another tenant was rejected, and what the hourly cost and carbon estimate will be before the deployment touches production.
*/
