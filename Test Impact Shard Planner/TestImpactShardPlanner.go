package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	toolName    = "TestImpactShardPlanner"
	toolVersion = "1.0.0"
)

type Options struct {
	ManifestPath       string
	ChangedPath        string
	Format             string
	BudgetOverride     int
	ParallelOverride   int
	MinRiskOverride    float64
	IncludeNonBlocking bool
	FailOnUncovered    bool
	SelfTest           bool
}

type Manifest struct {
	ChangedPaths       []string   `json:"changed_paths"`
	BudgetSeconds      int        `json:"budget_seconds"`
	Parallelism        int        `json:"parallelism"`
	MinRisk            float64    `json:"min_risk"`
	AlwaysRun          []string   `json:"always_run"`
	IncludeNonBlocking bool       `json:"include_nonblocking"`
	Tests              []TestSpec `json:"tests"`
	RiskRules          []RiskRule `json:"risk_rules"`
}

type TestSpec struct {
	Name           string            `json:"name"`
	Command        string            `json:"command"`
	Paths          []string          `json:"paths"`
	RuntimeSeconds int               `json:"runtime_seconds"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	FlakeRate      float64           `json:"flake_rate"`
	Criticality     float64           `json:"criticality"`
	Tier            string            `json:"tier"`
	Owners          []string          `json:"owners"`
	Resources       []string          `json:"resources"`
	Env             map[string]string `json:"env"`
	MustRun         bool              `json:"must_run"`
}

type RiskRule struct {
	Pattern string   `json:"pattern"`
	Weight  float64  `json:"weight"`
	Reason  string   `json:"reason"`
	Tests   []string `json:"tests"`
}

type PlanReport struct {
	Tool                 string        `json:"tool"`
	Version              string        `json:"version"`
	GeneratedAt          string        `json:"generated_at"`
	Status               string        `json:"status"`
	ChangedPaths         []string      `json:"changed_paths"`
	Selected             []PlannedTest `json:"selected"`
	Skipped              []SkippedTest `json:"skipped"`
	UncoveredPaths       []string      `json:"uncovered_paths"`
	Warnings             []string      `json:"warnings"`
	BudgetSeconds        int           `json:"budget_seconds"`
	Parallelism          int           `json:"parallelism"`
	TotalRuntimeSeconds  int           `json:"total_runtime_seconds"`
	EstimatedWallSeconds int           `json:"estimated_wall_seconds"`
	RiskScore            float64       `json:"risk_score"`
}

type PlannedTest struct {
	Name           string            `json:"name"`
	Command        string            `json:"command"`
	Tier            string            `json:"tier"`
	RuntimeSeconds int               `json:"runtime_seconds"`
	TimeoutMinutes int               `json:"timeout_minutes"`
	FlakeRate      float64           `json:"flake_rate"`
	Criticality     float64           `json:"criticality"`
	RiskScore       float64           `json:"risk_score"`
	MatchedPaths    []string          `json:"matched_paths"`
	Reasons         []string          `json:"reasons"`
	Owners          []string          `json:"owners,omitempty"`
	Resources       []string          `json:"resources,omitempty"`
	Env             map[string]string `json:"env,omitempty"`
	Forced          bool              `json:"forced"`
}

type SkippedTest struct {
	Name           string   `json:"name"`
	Command        string   `json:"command"`
	Tier            string   `json:"tier"`
	RuntimeSeconds int      `json:"runtime_seconds"`
	RiskScore       float64  `json:"risk_score"`
	MatchedPaths    []string `json:"matched_paths"`
	Reason          string   `json:"reason"`
}

type candidate struct {
	test        TestSpec
	risk        float64
	cost        float64
	matches     []string
	reasons     []string
	forced      bool
	nonBlocking bool
}

func main() {
	code, err := run(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", toolName, err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(args []string, stdout io.Writer, stderr io.Writer) (int, error) {
	opts, err := parseOptions(args)
	if err != nil {
		return 1, err
	}
	if opts.SelfTest {
		return runSelfTest(stdout)
	}

	manifest, err := loadManifest(opts.ManifestPath)
	if err != nil {
		return 1, err
	}
	if opts.ChangedPath != "" {
		changed, err := loadChangedPaths(opts.ChangedPath)
		if err != nil {
			return 1, err
		}
		manifest.ChangedPaths = changed
	}
	applyOverrides(&manifest, opts)
	if err := normalizeManifest(&manifest); err != nil {
		return 1, err
	}

	report := Plan(manifest)
	if err := renderReport(stdout, report, opts.Format); err != nil {
		return 1, err
	}
	if opts.FailOnUncovered && len(report.UncoveredPaths) > 0 {
		fmt.Fprintln(stderr, "uncovered changed paths remain; refusing to pass impact gate")
		return 2, nil
	}
	return 0, nil
}

func parseOptions(args []string) (Options, error) {
	var opts Options
	flags := flag.NewFlagSet(toolName, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.ManifestPath, "manifest", "", "JSON manifest path; defaults to stdin")
	flags.StringVar(&opts.ChangedPath, "changed", "", "changed path list as JSON, JSON object, or newline text")
	flags.StringVar(&opts.Format, "format", "markdown", "markdown, json, or gha-matrix")
	flags.IntVar(&opts.BudgetOverride, "budget-seconds", -1, "override runtime budget in seconds")
	flags.IntVar(&opts.ParallelOverride, "parallelism", -1, "override estimated parallel worker count")
	flags.Float64Var(&opts.MinRiskOverride, "min-risk", -1, "override minimum risk score for optional tests")
	flags.BoolVar(&opts.IncludeNonBlocking, "include-nonblocking", false, "allow advisory tests into the selected plan")
	flags.BoolVar(&opts.FailOnUncovered, "fail-on-uncovered", false, "exit 2 when selected tests do not cover all changed paths")
	flags.BoolVar(&opts.SelfTest, "self-test", false, "run built-in planner checks")
	flags.Usage = func() {}
	if err := flags.Parse(args); err != nil {
		return opts, usageError(err)
	}
	if flags.NArg() > 0 {
		return opts, usageError(fmt.Errorf("unexpected positional argument %q", flags.Arg(0)))
	}
	opts.Format = strings.ToLower(strings.TrimSpace(opts.Format))
	switch opts.Format {
	case "markdown", "md", "json", "gha-matrix", "matrix":
		return opts, nil
	default:
		return opts, usageError(fmt.Errorf("unknown format %q", opts.Format))
	}
}

func usageError(err error) error {
	return fmt.Errorf("%w\n\nUsage:\n  %s --manifest impact.json --changed changed.txt --format markdown\n  %s --manifest impact.json --format gha-matrix > matrix.json\n  %s --self-test\n\nManifest fields: changed_paths, budget_seconds, parallelism, min_risk, always_run, tests, risk_rules", err, toolName, toolName, toolName)
}

func loadManifest(pathValue string) (Manifest, error) {
	var raw []byte
	var err error
	if pathValue == "" || pathValue == "-" {
		raw, err = io.ReadAll(os.Stdin)
		if err != nil {
			return Manifest{}, fmt.Errorf("read stdin: %w", err)
		}
		if len(bytes.TrimSpace(raw)) == 0 {
			return Manifest{}, errors.New("manifest is empty; pass --manifest or pipe JSON")
		}
	} else {
		raw, err = os.ReadFile(pathValue)
		if err != nil {
			return Manifest{}, fmt.Errorf("read manifest: %w", err)
		}
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest JSON: %w", err)
	}
	return manifest, nil
}

func loadChangedPaths(pathValue string) ([]string, error) {
	raw, err := os.ReadFile(pathValue)
	if err != nil {
		return nil, fmt.Errorf("read changed paths: %w", err)
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("changed path list is empty")
	}
	if trimmed[0] == '[' {
		var paths []string
		if err := json.Unmarshal(trimmed, &paths); err != nil {
			return nil, fmt.Errorf("parse changed path JSON array: %w", err)
		}
		return paths, nil
	}
	if trimmed[0] == '{' {
		var payload struct {
			ChangedPaths []string `json:"changed_paths"`
			Files        []string `json:"files"`
			Paths        []string `json:"paths"`
		}
		if err := json.Unmarshal(trimmed, &payload); err != nil {
			return nil, fmt.Errorf("parse changed path JSON object: %w", err)
		}
		switch {
		case len(payload.ChangedPaths) > 0:
			return payload.ChangedPaths, nil
		case len(payload.Files) > 0:
			return payload.Files, nil
		case len(payload.Paths) > 0:
			return payload.Paths, nil
		default:
			return nil, errors.New("changed path object has no changed_paths, files, or paths array")
		}
	}
	var paths []string
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		paths = append(paths, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan changed paths: %w", err)
	}
	return paths, nil
}

func applyOverrides(manifest *Manifest, opts Options) {
	if opts.BudgetOverride >= 0 {
		manifest.BudgetSeconds = opts.BudgetOverride
	}
	if opts.ParallelOverride > 0 {
		manifest.Parallelism = opts.ParallelOverride
	}
	if opts.MinRiskOverride >= 0 {
		manifest.MinRisk = opts.MinRiskOverride
	}
	if opts.IncludeNonBlocking {
		manifest.IncludeNonBlocking = true
	}
}

func normalizeManifest(manifest *Manifest) error {
	if len(manifest.Tests) == 0 {
		return errors.New("manifest needs at least one test")
	}
	manifest.ChangedPaths = uniqueSorted(normalizePaths(manifest.ChangedPaths))
	if len(manifest.ChangedPaths) == 0 {
		return errors.New("manifest needs changed_paths or --changed")
	}
	if manifest.Parallelism <= 0 {
		manifest.Parallelism = 1
	}
	if manifest.MinRisk <= 0 {
		manifest.MinRisk = 0.25
	}
	if manifest.MinRisk > 1000 {
		return errors.New("min_risk is unreasonably high")
	}
	manifest.AlwaysRun = uniqueSorted(names(manifest.AlwaysRun))
	for i := range manifest.Tests {
		test := &manifest.Tests[i]
		test.Name = strings.TrimSpace(test.Name)
		test.Command = strings.TrimSpace(test.Command)
		test.Tier = normalizeTier(test.Tier)
		test.Paths = uniqueSorted(normalizePatterns(test.Paths))
		test.Owners = uniqueSorted(names(test.Owners))
		test.Resources = uniqueSorted(names(test.Resources))
		if test.Name == "" {
			return fmt.Errorf("tests[%d] has empty name", i)
		}
		if test.Command == "" {
			return fmt.Errorf("test %q has empty command", test.Name)
		}
		if test.RuntimeSeconds <= 0 {
			test.RuntimeSeconds = 60
		}
		if test.TimeoutSeconds > 0 && test.TimeoutSeconds < test.RuntimeSeconds {
			return fmt.Errorf("test %q timeout_seconds is lower than runtime_seconds", test.Name)
		}
		if test.FlakeRate < 0 || test.FlakeRate > 1 {
			return fmt.Errorf("test %q flake_rate must be between 0 and 1", test.Name)
		}
		if test.Criticality < 0 {
			return fmt.Errorf("test %q criticality cannot be negative", test.Name)
		}
		if test.Env == nil {
			test.Env = map[string]string{}
		}
	}
	for i := range manifest.RiskRules {
		rule := &manifest.RiskRules[i]
		rule.Pattern = normalizePattern(rule.Pattern)
		rule.Reason = strings.TrimSpace(rule.Reason)
		rule.Tests = uniqueSorted(names(rule.Tests))
		if rule.Pattern == "" {
			return fmt.Errorf("risk_rules[%d] has empty pattern", i)
		}
		if rule.Weight <= 0 {
			return fmt.Errorf("risk rule %q needs positive weight", rule.Pattern)
		}
		if rule.Reason == "" {
			rule.Reason = "matched risk rule " + rule.Pattern
		}
		if _, err := compilePattern(rule.Pattern); err != nil {
			return fmt.Errorf("risk rule %q: %w", rule.Pattern, err)
		}
	}
	for _, test := range manifest.Tests {
		for _, pattern := range test.Paths {
			if _, err := compilePattern(pattern); err != nil {
				return fmt.Errorf("test %q path pattern %q: %w", test.Name, pattern, err)
			}
		}
	}
	return nil
}

func Plan(manifest Manifest) PlanReport {
	always := map[string]bool{}
	for _, name := range manifest.AlwaysRun {
		always[name] = true
	}

	var candidates []candidate
	var skipped []SkippedTest
	for _, test := range manifest.Tests {
		c := scoreCandidate(test, manifest.ChangedPaths, manifest.RiskRules, always[test.Name])
		c.nonBlocking = test.Tier == "nonblocking" || test.Tier == "advisory"
		if c.nonBlocking && !manifest.IncludeNonBlocking && !c.forced {
			skipped = append(skipped, skippedFromCandidate(c, "nonblocking test excluded"))
			continue
		}
		if !c.forced && c.risk < manifest.MinRisk {
			skipped = append(skipped, skippedFromCandidate(c, "risk below min_risk"))
			continue
		}
		candidates = append(candidates, c)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		if a.forced != b.forced {
			return a.forced
		}
		pa := priority(a)
		pb := priority(b)
		if pa != pb {
			return pa > pb
		}
		if a.risk != b.risk {
			return a.risk > b.risk
		}
		if a.test.RuntimeSeconds != b.test.RuntimeSeconds {
			return a.test.RuntimeSeconds < b.test.RuntimeSeconds
		}
		return a.test.Name < b.test.Name
	})

	selectedNames := map[string]bool{}
	var selected []PlannedTest
	totalRuntime := 0
	for _, c := range candidates {
		if selectedNames[c.test.Name] {
			continue
		}
		if c.forced {
			selected = append(selected, plannedFromCandidate(c))
			selectedNames[c.test.Name] = true
			totalRuntime += c.test.RuntimeSeconds
			continue
		}
		if manifest.BudgetSeconds > 0 && totalRuntime+c.test.RuntimeSeconds > manifest.BudgetSeconds {
			skipped = append(skipped, skippedFromCandidate(c, "runtime budget exhausted"))
			continue
		}
		selected = append(selected, plannedFromCandidate(c))
		selectedNames[c.test.Name] = true
		totalRuntime += c.test.RuntimeSeconds
	}

	uncovered := uncoveredPaths(manifest.ChangedPaths, selected)
	warnings := buildWarnings(manifest, selected, skipped, uncovered, totalRuntime)
	status := "pass"
	if len(uncovered) > 0 {
		status = "needs_attention"
	}
	if len(selected) == 0 {
		status = "empty_plan"
	}

	risk := 0.0
	for _, test := range selected {
		risk += test.RiskScore
	}
	return PlanReport{
		Tool:                 toolName,
		Version:              toolVersion,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
		Status:               status,
		ChangedPaths:         append([]string{}, manifest.ChangedPaths...),
		Selected:             selected,
		Skipped:              normalizeSkipped(skipped),
		UncoveredPaths:       uncovered,
		Warnings:             warnings,
		BudgetSeconds:        manifest.BudgetSeconds,
		Parallelism:          manifest.Parallelism,
		TotalRuntimeSeconds:  totalRuntime,
		EstimatedWallSeconds: estimateWallSeconds(selected, manifest.Parallelism),
		RiskScore:            round2(risk),
	}
}

func scoreCandidate(test TestSpec, changedPaths []string, rules []RiskRule, forcedByName bool) candidate {
	matchSet := map[string]bool{}
	reasonSet := map[string]bool{}
	for _, changed := range changedPaths {
		for _, pattern := range test.Paths {
			if matchPattern(pattern, changed) {
				matchSet[changed] = true
				reasonSet["matches "+pattern] = true
			}
		}
	}

	risk := float64(len(matchSet)) * 2.5
	for _, rule := range rules {
		if !ruleAppliesToTest(rule, test) && len(rule.Tests) > 0 {
			continue
		}
		for _, changed := range changedPaths {
			if matchPattern(rule.Pattern, changed) {
				if len(rule.Tests) == 0 && len(matchSet) == 0 && !testMatchesPath(test, changed) {
					continue
				}
				risk += rule.Weight
				matchSet[changed] = true
				reasonSet[rule.Reason] = true
			}
		}
	}

	forced := forcedByName || test.MustRun
	if len(matchSet) > 0 || forced {
		if test.Criticality > 0 {
			risk += test.Criticality
			reasonSet[fmt.Sprintf("criticality %.2f", test.Criticality)] = true
		}
		switch test.Tier {
		case "blocking":
			risk += 0.75
			reasonSet["blocking tier"] = true
		case "security":
			risk += 1.25
			reasonSet["security tier"] = true
		case "migration":
			risk += 0.9
			reasonSet["migration tier"] = true
		}
	}
	if forced {
		risk += 1000
		reasonSet["forced by always_run or must_run"] = true
	}
	matches := sortedKeys(matchSet)
	reasons := sortedKeys(reasonSet)
	cost := float64(test.RuntimeSeconds) * (1 + (test.FlakeRate * 2))
	if cost <= 0 {
		cost = 1
	}
	return candidate{
		test:    test,
		risk:    round2(risk),
		cost:    cost,
		matches: matches,
		reasons: reasons,
		forced:  forced,
	}
}

func ruleAppliesToTest(rule RiskRule, test TestSpec) bool {
	if len(rule.Tests) == 0 {
		return true
	}
	for _, name := range rule.Tests {
		if name == test.Name {
			return true
		}
	}
	return false
}

func testMatchesPath(test TestSpec, changedPath string) bool {
	for _, pattern := range test.Paths {
		if matchPattern(pattern, changedPath) {
			return true
		}
	}
	return false
}

func priority(c candidate) float64 {
	return c.risk / c.cost
}

func plannedFromCandidate(c candidate) PlannedTest {
	timeout := c.test.TimeoutSeconds
	if timeout <= 0 {
		timeout = int(math.Ceil(float64(c.test.RuntimeSeconds) * 1.6))
	}
	if timeout < 60 {
		timeout = 60
	}
	return PlannedTest{
		Name:           c.test.Name,
		Command:        c.test.Command,
		Tier:            c.test.Tier,
		RuntimeSeconds: c.test.RuntimeSeconds,
		TimeoutMinutes: int(math.Ceil(float64(timeout) / 60)),
		FlakeRate:      c.test.FlakeRate,
		Criticality:     c.test.Criticality,
		RiskScore:       c.risk,
		MatchedPaths:    append([]string{}, c.matches...),
		Reasons:         append([]string{}, c.reasons...),
		Owners:          append([]string{}, c.test.Owners...),
		Resources:       append([]string{}, c.test.Resources...),
		Env:             cloneEnv(c.test.Env),
		Forced:          c.forced,
	}
}

func skippedFromCandidate(c candidate, reason string) SkippedTest {
	return SkippedTest{
		Name:           c.test.Name,
		Command:        c.test.Command,
		Tier:            c.test.Tier,
		RuntimeSeconds: c.test.RuntimeSeconds,
		RiskScore:       c.risk,
		MatchedPaths:    append([]string{}, c.matches...),
		Reason:          reason,
	}
}

func uncoveredPaths(changed []string, selected []PlannedTest) []string {
	covered := map[string]bool{}
	for _, test := range selected {
		for _, changedPath := range test.MatchedPaths {
			covered[changedPath] = true
		}
	}
	var uncovered []string
	for _, changedPath := range changed {
		if !covered[changedPath] {
			uncovered = append(uncovered, changedPath)
		}
	}
	sort.Strings(uncovered)
	return uncovered
}

func buildWarnings(manifest Manifest, selected []PlannedTest, skipped []SkippedTest, uncovered []string, totalRuntime int) []string {
	var warnings []string
	if len(selected) == 0 {
		warnings = append(warnings, "no tests selected; check path patterns and min_risk")
	}
	if len(uncovered) > 0 {
		warnings = append(warnings, fmt.Sprintf("%d changed path(s) have no selected test coverage", len(uncovered)))
	}
	if manifest.BudgetSeconds > 0 && totalRuntime > manifest.BudgetSeconds {
		warnings = append(warnings, "forced tests exceed runtime budget")
	}
	for _, skippedTest := range skipped {
		if skippedTest.Reason == "runtime budget exhausted" && skippedTest.RiskScore >= manifest.MinRisk*2 {
			warnings = append(warnings, "high-risk test skipped by budget: "+skippedTest.Name)
		}
	}
	resourceOwners := map[string][]string{}
	for _, test := range selected {
		for _, resource := range test.Resources {
			resourceOwners[resource] = append(resourceOwners[resource], test.Name)
		}
	}
	for resource, tests := range resourceOwners {
		if len(tests) > 1 && manifest.Parallelism > 1 {
			sort.Strings(tests)
			warnings = append(warnings, fmt.Sprintf("parallel plan shares resource %q across %s", resource, strings.Join(tests, ", ")))
		}
	}
	return uniqueSorted(warnings)
}

func estimateWallSeconds(selected []PlannedTest, parallelism int) int {
	if len(selected) == 0 {
		return 0
	}
	if parallelism <= 1 {
		total := 0
		for _, test := range selected {
			total += test.RuntimeSeconds
		}
		return total
	}
	workers := make([]int, parallelism)
	items := append([]PlannedTest{}, selected...)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].RuntimeSeconds != items[j].RuntimeSeconds {
			return items[i].RuntimeSeconds > items[j].RuntimeSeconds
		}
		return items[i].Name < items[j].Name
	})
	for _, test := range items {
		minIndex := 0
		for i := 1; i < len(workers); i++ {
			if workers[i] < workers[minIndex] {
				minIndex = i
			}
		}
		workers[minIndex] += test.RuntimeSeconds
	}
	maxValue := 0
	for _, value := range workers {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func renderReport(w io.Writer, report PlanReport, formatValue string) error {
	switch formatValue {
	case "json":
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	case "gha-matrix", "matrix":
		return renderMatrix(w, report)
	case "markdown", "md":
		_, err := io.WriteString(w, renderMarkdown(report))
		return err
	default:
		return fmt.Errorf("unknown format %q", formatValue)
	}
}

func renderMatrix(w io.Writer, report PlanReport) error {
	type item struct {
		Name           string            `json:"name"`
		Command        string            `json:"command"`
		Tier           string            `json:"tier"`
		TimeoutMinutes int               `json:"timeout_minutes"`
		RiskScore      float64           `json:"risk_score"`
		Owners         []string          `json:"owners,omitempty"`
		Resources      []string          `json:"resources,omitempty"`
		Env            map[string]string `json:"env,omitempty"`
	}
	payload := struct {
		Include []item `json:"include"`
	}{}
	for _, test := range report.Selected {
		payload.Include = append(payload.Include, item{
			Name:           test.Name,
			Command:        test.Command,
			Tier:           test.Tier,
			TimeoutMinutes: test.TimeoutMinutes,
			RiskScore:      test.RiskScore,
			Owners:         test.Owners,
			Resources:      test.Resources,
			Env:            test.Env,
		})
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(payload)
}

func renderMarkdown(report PlanReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s Report\n\n", report.Tool)
	fmt.Fprintf(&b, "- Status: %s\n", report.Status)
	fmt.Fprintf(&b, "- Changed paths: %d\n", len(report.ChangedPaths))
	fmt.Fprintf(&b, "- Selected tests: %d\n", len(report.Selected))
	fmt.Fprintf(&b, "- Skipped tests: %d\n", len(report.Skipped))
	fmt.Fprintf(&b, "- Total runtime: %s\n", humanSeconds(report.TotalRuntimeSeconds))
	fmt.Fprintf(&b, "- Estimated wall time: %s with parallelism %d\n", humanSeconds(report.EstimatedWallSeconds), report.Parallelism)
	if report.BudgetSeconds > 0 {
		fmt.Fprintf(&b, "- Runtime budget: %s\n", humanSeconds(report.BudgetSeconds))
	} else {
		fmt.Fprintf(&b, "- Runtime budget: unlimited\n")
	}
	fmt.Fprintf(&b, "- Aggregate risk score: %.2f\n\n", report.RiskScore)

	if len(report.Warnings) > 0 {
		b.WriteString("## Warnings\n\n")
		for _, warning := range report.Warnings {
			fmt.Fprintf(&b, "- %s\n", warning)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Selected Tests\n\n")
	if len(report.Selected) == 0 {
		b.WriteString("No tests selected.\n\n")
	} else {
		b.WriteString("| Test | Tier | Runtime | Risk | Why | Command |\n")
		b.WriteString("| --- | --- | ---: | ---: | --- | --- |\n")
		for _, test := range report.Selected {
			fmt.Fprintf(
				&b,
				"| %s | %s | %s | %.2f | %s | `%s` |\n",
				escapeMarkdown(test.Name),
				escapeMarkdown(test.Tier),
				humanSeconds(test.RuntimeSeconds),
				test.RiskScore,
				escapeMarkdown(strings.Join(test.Reasons, "; ")),
				escapeBackticks(test.Command),
			)
		}
		b.WriteString("\n")
	}

	if len(report.UncoveredPaths) > 0 {
		b.WriteString("## Uncovered Changed Paths\n\n")
		for _, changedPath := range report.UncoveredPaths {
			fmt.Fprintf(&b, "- `%s`\n", escapeBackticks(changedPath))
		}
		b.WriteString("\n")
	}

	if len(report.Skipped) > 0 {
		b.WriteString("## Skipped Tests\n\n")
		b.WriteString("| Test | Runtime | Risk | Reason |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, test := range report.Skipped {
			fmt.Fprintf(
				&b,
				"| %s | %s | %.2f | %s |\n",
				escapeMarkdown(test.Name),
				humanSeconds(test.RuntimeSeconds),
				test.RiskScore,
				escapeMarkdown(test.Reason),
			)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func normalizeSkipped(skipped []SkippedTest) []SkippedTest {
	out := append([]SkippedTest{}, skipped...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Reason != out[j].Reason {
			return out[i].Reason < out[j].Reason
		}
		if out[i].RiskScore != out[j].RiskScore {
			return out[i].RiskScore > out[j].RiskScore
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func normalizePaths(values []string) []string {
	var out []string
	for _, value := range values {
		normalized := normalizePath(value)
		if normalized != "" {
			out = append(out, normalized)
		}
	}
	return out
}

func normalizePath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	value = strings.TrimPrefix(value, "./")
	value = strings.TrimPrefix(value, "/")
	if value == "" {
		return ""
	}
	cleaned := path.Clean(value)
	if cleaned == "." {
		return ""
	}
	return cleaned
}

func normalizePatterns(values []string) []string {
	var out []string
	for _, value := range values {
		normalized := normalizePattern(value)
		if normalized != "" {
			out = append(out, normalized)
		}
	}
	return out
}

func normalizePattern(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	value = strings.TrimPrefix(value, "./")
	value = strings.TrimPrefix(value, "/")
	return value
}

func normalizeTier(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "blocking"
	}
	return value
}

func names(values []string) []string {
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func uniqueSorted(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(set map[string]bool) []string {
	var out []string
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func cloneEnv(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := map[string]string{}
	for key, value := range input {
		out[key] = value
	}
	return out
}

func matchPattern(patternValue string, pathValue string) bool {
	patternValue = normalizePattern(patternValue)
	pathValue = normalizePath(pathValue)
	if patternValue == "" || pathValue == "" {
		return false
	}
	if patternValue == pathValue {
		return true
	}
	if !strings.ContainsAny(patternValue, "*?[") {
		prefix := strings.TrimSuffix(patternValue, "/")
		return strings.HasPrefix(pathValue, prefix+"/")
	}
	re, err := compilePattern(patternValue)
	if err != nil {
		return false
	}
	return re.MatchString(pathValue)
}

func compilePattern(patternValue string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(patternValue); i++ {
		ch := patternValue[i]
		switch ch {
		case '*':
			if i+1 < len(patternValue) && patternValue[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '.':
			b.WriteString("\\.")
		case '+', '(', ')', '|', '^', '$', '{', '}', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}

func humanSeconds(seconds int) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	minutes := seconds / 60
	remaining := seconds % 60
	if minutes < 60 {
		if remaining == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm %ds", minutes, remaining)
	}
	hours := minutes / 60
	minutes = minutes % 60
	if minutes == 0 && remaining == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	if remaining == 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, remaining)
}

func escapeMarkdown(value string) string {
	replacer := strings.NewReplacer("|", "\\|", "\n", " ", "\r", " ")
	return replacer.Replace(value)
}

func escapeBackticks(value string) string {
	return strings.ReplaceAll(value, "`", "'")
}

func runSelfTest(stdout io.Writer) (int, error) {
	manifest := Manifest{
		ChangedPaths:  []string{"services/api/handler.go", "go.mod", "docs/usage.md"},
		BudgetSeconds: 480,
		Parallelism:   2,
		MinRisk:       1.0,
		AlwaysRun:     []string{"dependency-review"},
		Tests: []TestSpec{
			{
				Name:           "api-unit",
				Command:        "go test ./services/api/...",
				Paths:          []string{"services/api/**"},
				RuntimeSeconds: 120,
				Criticality:    2,
				Tier:           "blocking",
				Owners:         []string{"platform"},
			},
			{
				Name:           "dependency-review",
				Command:        "govulncheck ./...",
				Paths:          []string{"go.mod", "go.sum"},
				RuntimeSeconds: 90,
				Criticality:    3,
				Tier:           "security",
				MustRun:        true,
			},
			{
				Name:           "browser-smoke",
				Command:        "npm run smoke",
				Paths:          []string{"web/**"},
				RuntimeSeconds: 260,
				Criticality:    1,
				Tier:           "blocking",
			},
		},
		RiskRules: []RiskRule{
			{Pattern: "go.mod", Weight: 5, Reason: "dependency graph changed", Tests: []string{"dependency-review"}},
		},
	}
	if err := normalizeManifest(&manifest); err != nil {
		return 1, err
	}
	report := Plan(manifest)
	if !containsSelected(report, "api-unit") {
		return 1, errors.New("self-test expected api-unit")
	}
	if !containsSelected(report, "dependency-review") {
		return 1, errors.New("self-test expected dependency-review")
	}
	if containsSelected(report, "browser-smoke") {
		return 1, errors.New("self-test did not expect browser-smoke")
	}
	if !containsString(report.UncoveredPaths, "docs/usage.md") {
		return 1, errors.New("self-test expected docs/usage.md to remain uncovered")
	}
	var matrix bytes.Buffer
	if err := renderMatrix(&matrix, report); err != nil {
		return 1, err
	}
	if !bytes.Contains(matrix.Bytes(), []byte(`"include"`)) {
		return 1, errors.New("self-test matrix missing include")
	}
	fmt.Fprintf(stdout, "%s self-test passed\n", toolName)
	return 0, nil
}

func containsSelected(report PlanReport, name string) bool {
	for _, test := range report.Selected {
		if test.Name == name {
			return true
		}
	}
	return false
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

/*
This solves the April 2026 problem where large repositories have too many tests for every pull request, but the cheap "run only what changed" shortcut misses cross-cutting risk from dependency files, generated clients, shared schemas, feature flags, edge workers, MCP tool servers, AI eval harnesses, and data pipeline contracts. Built because Pavan would rather spend CI minutes on the shards that actually defend a release than burn a full test fleet on every small edit or, worse, skip the one shard that knew the blast radius. Use it when a monorepo, AI tooling platform, web app, DevOps automation repo, research system, Go service, TypeScript frontend, Python pipeline, smart IoT backend, carbon credit data workflow, or edge compute worker needs a deterministic test impact planner that can be reviewed in plain JSON. The trick: this single Go file accepts a manifest of tests, changed paths, risk rules, runtime budgets, flake rates, criticality scores, owners, and shared resources, then ranks shards by risk per second, keeps forced gates, warns about uncovered files, estimates wall time, and emits Markdown, JSON, or a GitHub Actions matrix without pulling in a service or vendor SDK. Drop this into CI, pre-merge automation, release trains, agent-generated pull request checks, flaky test cleanup, build graph migration, developer productivity dashboards, and infrastructure repositories where the useful search terms are test impact analysis, CI shard planner, GitHub Actions matrix generator, monorepo selective testing, runtime budget gate, risk based testing, DevOps release confidence, AI coding agent CI safety, edge compute test planning, and production-ready developer tooling.
*/