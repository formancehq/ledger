package state

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type historicalBalanceFSMPerfLatency struct {
	Name                string  `json:"name"`
	Unit                string  `json:"unit"`
	Samples             int     `json:"samples"`
	Min                 float64 `json:"min"`
	P50                 float64 `json:"p50"`
	P95                 float64 `json:"p95"`
	P99                 float64 `json:"p99"`
	Max                 float64 `json:"max"`
	Mean                float64 `json:"mean"`
	OperationsPerSecond float64 `json:"operationsPerSecond"`
	SamplesNS           []int64 `json:"samplesNs"`
}

type historicalBalanceFSMPerfComparison struct {
	FourTargets          historicalBalanceFSMPerfLatency `json:"fourTargetsBaseline"`
	FiveTargets          historicalBalanceFSMPerfLatency `json:"fiveTargetsDelivered"`
	P50RegressionPercent float64                         `json:"p50RegressionPercent"`
	P99RegressionPercent float64                         `json:"p99RegressionPercent"`
}

type historicalBalanceFSMPerfReport struct {
	SchemaVersion         int                                `json:"schemaVersion"`
	GeneratedAt           string                             `json:"generatedAt"`
	Profile               string                             `json:"profile"`
	GitCommit             string                             `json:"gitCommit"`
	GitTree               string                             `json:"gitTree"`
	WorkingTree           string                             `json:"workingTree"`
	Machine               string                             `json:"machine"`
	GoVersion             string                             `json:"goVersion"`
	DeliveredTargets      int                                `json:"deliveredFanOutTargets"`
	FanOutOnly            historicalBalanceFSMPerfComparison `json:"fanOutOnly"`
	FSMApplyAndDurability historicalBalanceFSMPerfComparison `json:"fsmApplyAndDurability"`
	Scope                 string                             `json:"scope"`
	Limitation            string                             `json:"limitation"`
}

// TestHistoricalBalanceFSMNotificationPerformanceEvidence isolates the delivered
// fifth synchronous notification target from the asynchronous history I/O
// measured by the builder harness.
func TestHistoricalBalanceFSMNotificationPerformanceEvidence(t *testing.T) {
	if os.Getenv("HISTORICAL_BALANCE_PERF") != "1" {
		t.Skip("set HISTORICAL_BALANCE_PERF=1 to run the historical-balance FSM notification evidence harness")
	}

	profile, fsmSamples, fanOutSamples := historicalBalanceFSMPerfProfile(t)
	fanOutFour := measureHistoricalBalanceFanOut(t, 4, fanOutSamples)
	fanOutFive := measureHistoricalBalanceFanOut(t, 5, fanOutSamples)
	fsmFour := measureHistoricalBalanceFSMApply(t, 4, fsmSamples)
	fsmFive := measureHistoricalBalanceFSMApply(t, 5, fsmSamples)
	report := historicalBalanceFSMPerfReport{
		SchemaVersion:    1,
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		Profile:          profile,
		GitCommit:        historicalBalanceFSMPerfValue(os.Getenv("HISTORICAL_BALANCE_PERF_GIT_COMMIT")),
		GitTree:          historicalBalanceFSMPerfValue(os.Getenv("HISTORICAL_BALANCE_PERF_GIT_TREE")),
		WorkingTree:      historicalBalanceFSMPerfValue(os.Getenv("HISTORICAL_BALANCE_PERF_WORKTREE")),
		Machine:          historicalBalanceFSMPerfValue(os.Getenv("HISTORICAL_BALANCE_PERF_MACHINE")),
		GoVersion:        runtime.Version(),
		DeliveredTargets: 5,
		FanOutOnly:       historicalBalanceFSMPerfComparisonResult(fanOutFour, fanOutFive),
		FSMApplyAndDurability: historicalBalanceFSMPerfComparisonResult(
			fsmFour,
			fsmFive,
		),
		Scope:      "local single-node Machine.ApplyEntries through Pebble NoSync commit, synchronous FanOut notification, then explicit primary SyncWAL; identical transaction shape and fresh volume keys",
		Limitation: "this is below Raft transport/admission and above neither HTTP nor gRPC; it measures the delivered synchronous four-to-five fan-out change separately from asynchronous historical-balance projection I/O",
	}

	writeHistoricalBalanceFSMPerfReport(t, report)
}

func historicalBalanceFSMPerfProfile(t *testing.T) (string, int, int) {
	t.Helper()

	switch profile := os.Getenv("HISTORICAL_BALANCE_PERF_PROFILE"); profile {
	case "", "local":
		return "local", 300, 100_000
	case "smoke":
		return "smoke", 60, 20_000
	case "full":
		return "full", 1_000, 500_000
	default:
		t.Fatalf("unknown HISTORICAL_BALANCE_PERF_PROFILE %q; expected smoke, local, or full", profile)

		return "", 0, 0
	}
}

func measureHistoricalBalanceFanOut(t *testing.T, targetCount, samples int) historicalBalanceFSMPerfLatency {
	t.Helper()

	targets := make([]*signal.Notifications, targetCount)
	for index := range targets {
		targets[index] = signal.NewNotifications()
	}
	fanOut := signal.NewFanOut(targets...)
	for sequence := range 1_000 {
		fanOut.NotifyLogsCommitted(uint64(sequence + 1))
		for _, target := range targets {
			<-target.LogCommitted.C()
		}
	}
	durations := make([]int64, samples)
	for index := range samples {
		started := time.Now()
		fanOut.NotifyLogsCommitted(uint64(index + 1))
		durations[index] = time.Since(started).Nanoseconds()
		for _, target := range targets {
			<-target.LogCommitted.C()
		}
	}

	return summarizeHistoricalBalanceFSMLatency(fmt.Sprintf("fanout_%d_targets", targetCount), durations, time.Nanosecond)
}

func measureHistoricalBalanceFSMApply(t *testing.T, targetCount, samples int) historicalBalanceFSMPerfLatency {
	t.Helper()

	machine, store, _ := newTestMachine(t)
	targets := make([]*signal.Notifications, targetCount)
	for index := range targets {
		targets[index] = signal.NewNotifications()
	}
	machine.notifier = signal.NewFanOut(targets...)
	const ledger = "historical-balance-fsm-performance"
	_, err := machine.ApplyEntries(
		context.Background(),
		store,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger))),
	)
	if err != nil {
		t.Fatalf("creating historical-balance FSM performance ledger with %d targets: %v", targetCount, err)
	}
	for _, target := range targets {
		<-target.LogCommitted.C()
	}

	const warmups = 20
	durations := make([]int64, samples)
	for index := range warmups + samples {
		sequence := uint64(index + 2)
		order := createTransactionOrder(
			ledger,
			true,
			&commonpb.Posting{
				Source:      fmt.Sprintf("sources:%08d", sequence),
				Destination: fmt.Sprintf("destinations:%08d", sequence),
				Asset:       "USD/2",
				Amount:      commonpb.NewUint256FromUint64(sequence),
			},
		)
		entry := makeEntry(t, sequence, makeProposal(sequence, order))
		started := time.Now()
		_, applyErr := machine.ApplyEntries(context.Background(), store, entry)
		if applyErr == nil {
			applyErr = store.SyncWAL()
		}
		elapsed := time.Since(started)
		if applyErr != nil {
			t.Fatalf("applying historical-balance FSM performance sample %d with %d targets: %v", index, targetCount, applyErr)
		}
		for _, target := range targets {
			<-target.LogCommitted.C()
		}
		if index >= warmups {
			durations[index-warmups] = elapsed.Nanoseconds()
		}
	}

	return summarizeHistoricalBalanceFSMLatency(fmt.Sprintf("fsm_apply_syncwal_%d_targets", targetCount), durations, time.Millisecond)
}

func historicalBalanceFSMPerfComparisonResult(four, five historicalBalanceFSMPerfLatency) historicalBalanceFSMPerfComparison {
	return historicalBalanceFSMPerfComparison{
		FourTargets:          four,
		FiveTargets:          five,
		P50RegressionPercent: historicalBalanceFSMPerfRegression(five.P50, four.P50),
		P99RegressionPercent: historicalBalanceFSMPerfRegression(five.P99, four.P99),
	}
}

func historicalBalanceFSMPerfRegression(after, before float64) float64 {
	if before == 0 {
		return 0
	}

	return (after/before - 1) * 100
}

func summarizeHistoricalBalanceFSMLatency(name string, samples []int64, unit time.Duration) historicalBalanceFSMPerfLatency {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	total := int64(0)
	for _, sample := range samples {
		total += sample
	}
	convert := func(value int64) float64 { return float64(value) / float64(unit) }

	return historicalBalanceFSMPerfLatency{
		Name: name, Unit: unit.String(), Samples: len(samples),
		Min: convert(ordered[0]), P50: convert(historicalBalanceFSMPerfPercentile(ordered, 0.50)),
		P95: convert(historicalBalanceFSMPerfPercentile(ordered, 0.95)), P99: convert(historicalBalanceFSMPerfPercentile(ordered, 0.99)),
		Max: convert(ordered[len(ordered)-1]), Mean: convert(total / int64(len(samples))),
		OperationsPerSecond: float64(len(samples)) / (float64(total) / float64(time.Second)),
		SamplesNS:           slices.Clone(samples),
	}
}

func historicalBalanceFSMPerfPercentile(ordered []int64, quantile float64) int64 {
	index := int(math.Ceil(quantile*float64(len(ordered)))) - 1
	index = max(0, min(index, len(ordered)-1))

	return ordered[index]
}

func writeHistoricalBalanceFSMPerfReport(t *testing.T, report historicalBalanceFSMPerfReport) {
	t.Helper()

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshaling historical-balance FSM performance evidence: %v", err)
	}
	output := os.Getenv("HISTORICAL_BALANCE_FSM_PERF_OUTPUT")
	if output == "" {
		t.Logf("HISTORICAL_BALANCE_FSM_PERF_OUTPUT is unset; raw evidence follows:\n%s", encoded)

		return
	}
	if !filepath.IsAbs(output) {
		cwd, cwdErr := os.Getwd()
		if cwdErr != nil {
			t.Fatalf("resolving historical-balance FSM performance output: %v", cwdErr)
		}
		output = filepath.Join(cwd, output)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		t.Fatalf("creating historical-balance FSM performance output directory: %v", err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("writing historical-balance FSM performance evidence: %v", err)
	}
	t.Logf("wrote historical-balance FSM performance evidence to %s", output)
}

func historicalBalanceFSMPerfValue(value string) string {
	if value == "" {
		return "unknown"
	}

	return value
}
