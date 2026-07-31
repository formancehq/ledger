package balancehistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	builderPerfClusterID           = "balance-history-performance-evidence"
	builderPerfPrimaryWarmupWrites = 20
)

type builderPerfSourceFixture struct {
	mu        sync.RWMutex
	proposals []VerifiedProposal
	positions []Position
	mock      *MockSource
}

func newBuilderPerfSource(t *testing.T) *builderPerfSourceFixture {
	t.Helper()

	source := &builderPerfSourceFixture{}
	source.mock = NewMockSource(gomock.NewController(t))
	source.mock.EXPECT().Head(gomock.Any()).AnyTimes().DoAndReturn(source.head)
	source.mock.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(source.read)

	return source
}

func (s *builderPerfSourceFixture) head(_ context.Context) (Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.positions) == 0 {
		return Position{}, nil
	}

	return cloneBuilderPerfPosition(s.positions[len(s.positions)-1]), nil
}

func (s *builderPerfSourceFixture) read(_ context.Context, after Position, maxProposals int) (Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	head := Position{}
	if len(s.positions) > 0 {
		head = cloneBuilderPerfPosition(s.positions[len(s.positions)-1])
	}
	if after.AuditSequence > uint64(len(s.proposals)) {
		return Batch{}, fmt.Errorf("performance source cursor %d exceeds head %d", after.AuditSequence, len(s.proposals))
	}
	if after.AuditSequence > 0 {
		position := s.positions[after.AuditSequence-1]
		if !position.equal(after) {
			return Batch{}, fmt.Errorf("performance source cursor at audit %d does not match the hash/log position", after.AuditSequence)
		}
	}
	if maxProposals <= 0 {
		maxProposals = 1
	}
	start := int(after.AuditSequence)
	end := min(start+maxProposals, len(s.proposals))
	if start == end {
		return Batch{Next: cloneBuilderPerfPosition(after), Head: head}, nil
	}

	return Batch{
		Proposals: append([]VerifiedProposal(nil), s.proposals[start:end]...),
		Next:      cloneBuilderPerfPosition(s.positions[end-1]),
		Head:      head,
	}, nil
}

func (s *builderPerfSourceFixture) appendCreateLedger(t *testing.T) Position {
	t.Helper()

	return s.appendLog(t, builderPerfCreateLedgerLog(1))
}

func (s *builderPerfSourceFixture) appendTransaction(t *testing.T) Position {
	t.Helper()

	s.mu.RLock()
	nextLog := uint64(len(s.proposals) + 1)
	s.mu.RUnlock()

	return s.appendLog(t, builderPerfTransactionLog(nextLog))
}

func (s *builderPerfSourceFixture) appendLog(t *testing.T, log *commonpb.Log) Position {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	auditSequence := uint64(len(s.proposals) + 1)
	previousHash := []byte(nil)
	if len(s.positions) > 0 {
		previousHash = s.positions[len(s.positions)-1].AuditHash
	}
	item := &auditpb.AuditItem{
		OrderIndex:      0,
		LogSequence:     log.GetSequence(),
		SerializedOrder: fmt.Appendf(nil, "performance-order-%08d", auditSequence),
	}
	entry := &auditpb.AuditEntry{
		Sequence:   auditSequence,
		Timestamp:  &commonpb.Timestamp{Data: auditSequence},
		ProposalId: auditSequence,
		OrderCount: 1,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: log.GetSequence(),
			MaxLogSequence: log.GetSequence(),
		}},
	}
	header, err := state.BuildHashedHeaderPayload(entry)
	if err != nil {
		t.Fatalf("building performance audit header %d: %v", auditSequence, err)
	}
	_, entry.Hash = processing.NewHashGenerator(
		commonpb.HashAlgorithm(entry.GetHashVersion()),
		builderPerfClusterID,
	).Compute(nil, previousHash, [][]byte{header, state.BuildPerItemPayload(item)})
	position := Position{
		AuditSequence: auditSequence,
		LogSequence:   log.GetSequence(),
		AuditHash:     append([]byte(nil), entry.GetHash()...),
	}
	s.proposals = append(s.proposals, VerifiedProposal{
		Entry: entry,
		Items: []*auditpb.AuditItem{item},
		Logs:  []*commonpb.Log{log},
	})
	s.positions = append(s.positions, position)

	return cloneBuilderPerfPosition(position)
}

func cloneBuilderPerfPosition(position Position) Position {
	position.AuditHash = append([]byte(nil), position.AuditHash...)

	return position
}

func builderPerfCreateLedgerLog(sequence uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: "default", Id: 1},
		}},
	}
}

func builderPerfTransactionLog(sequence uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "default",
			Log: &commonpb.LedgerLog{
				Id: sequence,
				Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
						Postings: []*commonpb.Posting{{
							Source:      "world",
							Destination: fmt.Sprintf("users:%06d", sequence%1_000),
							Asset:       fmt.Sprintf("ASSET%02d/%d", sequence%8, 2+sequence%2),
							Color:       string(rune('A' + sequence%4)),
							Amount:      commonpb.NewUint256FromUint64(sequence + 1),
						}},
						Timestamp:  &commonpb.Timestamp{Data: sequence * 10},
						InsertedAt: &commonpb.Timestamp{Data: sequence * 11},
					}},
				}},
			},
		}}},
	}
}

type builderPerfLatency struct {
	Name                 string         `json:"name"`
	Unit                 string         `json:"unit"`
	Samples              int            `json:"samples"`
	Min                  float64        `json:"min"`
	P50                  float64        `json:"p50"`
	P95                  float64        `json:"p95"`
	P99                  float64        `json:"p99"`
	Max                  float64        `json:"max"`
	Mean                 float64        `json:"mean"`
	OperationsPerSecond  float64        `json:"operationsPerSecond"`
	AllocatedBytesPerOp  float64        `json:"allocatedBytesPerOp,omitempty"`
	AllocationsPerOp     float64        `json:"allocationsPerOp,omitempty"`
	GoCPUSeconds         float64        `json:"goCpuSeconds,omitempty"`
	GoCPUMicrosecondsPer float64        `json:"goCpuMicrosecondsPerOp,omitempty"`
	IO                   *builderPerfIO `json:"io,omitempty"`
	SamplesNS            []int64        `json:"samplesNs"`
}

type builderPerfIO struct {
	WALBytesIn      uint64 `json:"walBytesIn"`
	WALBytesWritten uint64 `json:"walBytesWritten"`
	Flushes         int64  `json:"flushes"`
	Compactions     int64  `json:"compactions"`
	DiskBytesBefore uint64 `json:"diskBytesBefore"`
	DiskBytesAfter  uint64 `json:"diskBytesAfter"`
}

type builderPerfRuntimeSnapshot struct {
	AllocatedBytes uint64
	Allocations    uint64
	GoCPUSeconds   float64
}

type builderPerfPhase struct {
	Name                    string         `json:"name"`
	AuditProposals          int            `json:"auditProposals"`
	Postings                int            `json:"postings"`
	Effects                 int            `json:"effects"`
	ElapsedMS               float64        `json:"elapsedMs"`
	AuditsPerSecond         float64        `json:"auditsPerSecond"`
	EffectsPerSecond        float64        `json:"effectsPerSecond"`
	AllocatedBytesPerEffect float64        `json:"allocatedBytesPerEffect"`
	AllocationsPerEffect    float64        `json:"allocationsPerEffect"`
	GoCPUSeconds            float64        `json:"goCpuSeconds"`
	GoCPUMicrosPerEffect    float64        `json:"goCpuMicrosecondsPerEffect"`
	DiskBytes               uint64         `json:"diskBytes"`
	BytesPerEffect          float64        `json:"bytesPerEffect"`
	Runs                    int            `json:"runs"`
	IO                      *builderPerfIO `json:"io"`
}

type builderPerfHistoryIOSnapshot struct {
	WALBytesIn      uint64
	WALBytesWritten uint64
	Flushes         int64
	Compactions     int64
	DiskBytes       uint64
}

type builderPerfAssertion struct {
	Name      string  `json:"name"`
	Target    string  `json:"target"`
	Observed  float64 `json:"observed"`
	Unit      string  `json:"unit"`
	Satisfied bool    `json:"satisfied"`
	Scope     string  `json:"scope"`
}

type builderPerfWriteImpact struct {
	Baseline                            builderPerfLatency      `json:"baseline"`
	BaselineBefore                      builderPerfLatency      `json:"baselineBeforePooled"`
	BaselineAfter                       builderPerfLatency      `json:"baselineAfterPooled"`
	SteadyTail                          builderPerfLatency      `json:"steadyTail"`
	SteadyWithoutCompaction             builderPerfLatency      `json:"steadyWithoutLogicalCompaction"`
	SteadyWithSynchronousDurability     builderPerfLatency      `json:"steadyWithSynchronousDurability"`
	Coalesced50MSTail                   builderPerfLatency      `json:"coalesced50msTail"`
	Coalesced100MSTail                  builderPerfLatency      `json:"coalesced100msTail"`
	Coalesced50MSWithoutCompactionTail  builderPerfLatency      `json:"coalesced50msWithoutLogicalCompactionTail"`
	CoalescedTickerTail                 builderPerfLatency      `json:"coalescedTickerTail"`
	RegulatedBackfill                   builderPerfLatency      `json:"regulatedBackfill"`
	BackfillBaseline                    builderPerfLatency      `json:"backfillBaseline"`
	SteadyP99RegressionPercent          float64                 `json:"steadyP99RegressionPercent"`
	MedianTrialP99RegressionPercent     float64                 `json:"medianTrialP99RegressionPercent"`
	MinTrialP99RegressionPercent        float64                 `json:"minTrialP99RegressionPercent"`
	MaxTrialP99RegressionPercent        float64                 `json:"maxTrialP99RegressionPercent"`
	MedianBaselineP99DriftPercent       float64                 `json:"medianBaselineP99DriftPercent"`
	MaxBaselineP99DriftPercent          float64                 `json:"maxBaselineP99DriftPercent"`
	PooledBaselineP99DriftPercent       float64                 `json:"pooledBaselineP99DriftPercent"`
	BaselineBracketStable               bool                    `json:"baselineBracketStable"`
	Conclusion                          string                  `json:"conclusion"`
	NoCompactionP99Regression           float64                 `json:"noCompactionP99RegressionPercent"`
	SynchronousDurabilityP99Regression  float64                 `json:"synchronousDurabilityP99RegressionPercent"`
	Coalesced50MSP99Regression          float64                 `json:"coalesced50msP99RegressionPercent"`
	Coalesced100MSP99Regression         float64                 `json:"coalesced100msP99RegressionPercent"`
	Coalesced50MSNoCompactionP99        float64                 `json:"coalesced50msNoCompactionP99RegressionPercent"`
	CoalescedTickerP99Regression        float64                 `json:"coalescedTickerP99RegressionPercent"`
	BackfillP99RegressionPercent        float64                 `json:"backfillP99RegressionPercent"`
	SteadyProducedAudits                uint64                  `json:"steadyProducedAudits"`
	PrimaryWarmupWrites                 int                     `json:"primaryWarmupWritesPerTrial"`
	BackfillAuditProposals              int                     `json:"backfillAuditProposals"`
	BackfillOverlappedAllWrites         bool                    `json:"backfillOverlappedAllWrites"`
	BackfillManifestVersion             uint64                  `json:"backfillManifestVersion"`
	BackfillLogicalRuns                 int                     `json:"backfillLogicalRuns"`
	BackfillHistoryIO                   *builderPerfIO          `json:"backfillHistoryIo"`
	BackfillYieldMS                     float64                 `json:"backfillYieldMs"`
	DurabilityIntervalMS                float64                 `json:"durabilityIntervalMs"`
	InterleavedTrials                   []builderPerfWriteTrial `json:"interleavedTrials"`
	LogicalCompactionDiagnostic         builderPerfWriteTrial   `json:"logicalCompactionDiagnostic"`
	SynchronousDurabilityDiagnostic     builderPerfWriteTrial   `json:"synchronousDurabilityDiagnostic"`
	Coalesced50MSDiagnostic             builderPerfWriteTrial   `json:"coalesced50msDiagnostic"`
	Coalesced100MSDiagnostic            builderPerfWriteTrial   `json:"coalesced100msDiagnostic"`
	Coalesced50MSNoCompactionDiagnostic builderPerfWriteTrial   `json:"coalesced50msNoCompactionDiagnostic"`
	CoalescedTickerDiagnostic           builderPerfWriteTrial   `json:"coalescedTickerDiagnostic"`
}

type builderPerfWriteTrial struct {
	Name                       string             `json:"name"`
	Trial                      int                `json:"trial"`
	Baseline                   builderPerfLatency `json:"baselineCombined"`
	BaselineBefore             builderPerfLatency `json:"baselineBefore"`
	BaselineAfter              builderPerfLatency `json:"baselineAfter"`
	BaselineP99DriftPercent    float64            `json:"baselineP99DriftPercent"`
	BaselineStable             bool               `json:"baselineStable"`
	SteadyTail                 builderPerfLatency `json:"steadyTail"`
	P99RegressionPercent       float64            `json:"p99RegressionPercent"`
	ProducedAudits             uint64             `json:"producedAudits"`
	PrimaryWarmupWrites        int                `json:"primaryWarmupWrites"`
	LogicalCompactionThreshold int                `json:"logicalCompactionThreshold"`
	DurabilityIntervalMS       float64            `json:"durabilityIntervalMs"`
	ProducerIntervalMS         float64            `json:"producerIntervalMs"`
	NotificationIntervalMS     float64            `json:"notificationIntervalMs"`
	UsesLogNotifications       bool               `json:"usesLogNotifications"`
	FanOutTargets              int                `json:"fanOutTargets"`
	MaintenanceEnabled         bool               `json:"maintenanceEnabled"`
	MaintenanceIntervalMS      float64            `json:"maintenanceIntervalMs"`
	MaintenancePasses          uint64             `json:"maintenancePasses"`
	MaintenanceCompactions     uint64             `json:"maintenanceCompactions"`
	RunsAtProducerStop         int                `json:"runsAtProducerStop"`
	MaxRunsAtLevelAtStop       int                `json:"maxRunsAtLevelAtStop"`
	RunsPerLevelBoundedAtStop  bool               `json:"runsPerLevelBoundedAtStop"`
	CatchupCompactions         int                `json:"catchupCompactions"`
	CatchupElapsedMS           float64            `json:"catchupElapsedMs"`
	MaxRunsAtLevelAfterCatchup int                `json:"maxRunsAtLevelAfterCatchup"`
	RunsPerLevelAfterCatchup   bool               `json:"runsPerLevelBoundedAfterCatchup"`
	ManifestVersion            uint64             `json:"manifestVersion"`
	LogicalRuns                int                `json:"logicalRunsAfterCatchup"`
	HistoryIO                  *builderPerfIO     `json:"historyIo"`
}

type builderPerfVerifierImpact struct {
	Baseline               builderPerfLatency `json:"baseline"`
	FullVerifierOverlap    builderPerfLatency `json:"fullVerifierOverlap"`
	P99RegressionPercent   float64            `json:"p99RegressionPercent"`
	VerifierElapsedMS      float64            `json:"verifierElapsedMs"`
	OverlappedAllWrites    bool               `json:"overlappedAllWrites"`
	AuthoritativeProposals int                `json:"authoritativeProposals"`
	Scope                  string             `json:"scope"`
}

type builderPerfReport struct {
	SchemaVersion int                       `json:"schemaVersion"`
	GeneratedAt   string                    `json:"generatedAt"`
	Profile       string                    `json:"profile"`
	GitCommit     string                    `json:"gitCommit"`
	GitTree       string                    `json:"gitTree"`
	WorkingTree   string                    `json:"workingTree"`
	Machine       string                    `json:"machine"`
	GoVersion     string                    `json:"goVersion"`
	GOOS          string                    `json:"goos"`
	GOARCH        string                    `json:"goarch"`
	BatchSize     int                       `json:"batchSize"`
	Backfill      builderPerfPhase          `json:"backfill"`
	Rebuild       builderPerfPhase          `json:"rebuild"`
	Restart       builderPerfLatency        `json:"restartVerification"`
	TailLag       builderPerfLatency        `json:"tailWallClockLag"`
	WriteImpact   builderPerfWriteImpact    `json:"primaryWriteImpact"`
	Verifier      builderPerfVerifierImpact `json:"fullVerifierWriteImpact"`
	Assertions    []builderPerfAssertion    `json:"assertions"`
	Pending       []string                  `json:"pending"`
}

// TestBuilderLocalPerformanceEvidence is opt-in and intentionally sequential.
// It measures the real reducer, hash verification, synchronous publications,
// logical compaction, durable restart state, and ticker-driven tail loop.
func TestBuilderLocalPerformanceEvidence(t *testing.T) {
	if os.Getenv("PIT_PERF") != "1" {
		t.Skip("set PIT_PERF=1 to run the local balance-history builder evidence harness")
	}

	profile, proposals, lagSamples := builderPerfProfile(t)
	phaseOnly := os.Getenv("PIT_PERF_PHASE_ONLY") == "1"
	if phaseOnly {
		if profile != "full" {
			t.Fatal("PIT_PERF_PHASE_ONLY=1 requires PIT_PERF_PROFILE=full")
		}
		proposals = 100_000
	}
	batchSize := DefaultBatchSize
	report := builderPerfReport{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Profile:       profile,
		GitCommit:     builderPerfValueOrUnknown(os.Getenv("PIT_PERF_GIT_COMMIT")),
		GitTree:       builderPerfValueOrUnknown(os.Getenv("PIT_PERF_GIT_TREE")),
		WorkingTree:   builderPerfValueOrUnknown(os.Getenv("PIT_PERF_WORKTREE")),
		Machine:       builderPerfValueOrUnknown(os.Getenv("PIT_PERF_MACHINE")),
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		BatchSize:     batchSize,
		Pending: []string{
			"cold archive download/decode throughput requires the composite cold source",
			"cluster-wide replica lag requires a multi-node deployment; deterministic digest convergence is covered by the store harness",
			"production lag under concurrent Raft traffic requires the deployed telemetry/k6 run",
			"deployed startup/daily HistoryVerifier overlap still requires the production workload run; the report includes a separate local lifecycle diagnostic",
		},
	}

	source := newBuilderPerfSource(t)
	source.appendCreateLedger(t)
	for range proposals - 1 {
		source.appendTransaction(t)
	}
	store := openBuilderPerfStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing builder performance store: %v", err)
		}
	}()
	builder := NewBuilder(
		source.mock,
		store,
		nil,
		nil,
		builderPerfClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-builder-performance"),
		batchSize,
		balancehistorystore.DefaultRunCompactionThreshold,
		DefaultBackfillYield,
		DefaultDurabilityInterval,
	)

	backfillRuntimeBefore := captureBuilderPerfRuntime()
	backfillIOBefore := captureBuilderPerfHistoryIO(store)
	started := time.Now()
	if err := builder.boot(context.Background()); err != nil {
		t.Fatalf("backfilling builder performance store: %v", err)
	}
	backfillElapsed := time.Since(started)
	report.Backfill = builderPerfPhaseResult(
		t,
		"backfill",
		store,
		proposals,
		backfillElapsed,
		backfillRuntimeBefore,
		captureBuilderPerfRuntime(),
		backfillIOBefore,
		captureBuilderPerfHistoryIO(store),
	)

	if !phaseOnly {
		restartSamples := make([]int64, max(10, min(50, lagSamples)))
		for index := range restartSamples {
			started = time.Now()
			if err := builder.boot(context.Background()); err != nil {
				t.Fatalf("verifying builder restart sample %d: %v", index, err)
			}
			restartSamples[index] = time.Since(started).Nanoseconds()
		}
		report.Restart = summarizeBuilderPerfLatency("restart_verify_and_resume", restartSamples)
	}

	if err := store.Reset(); err != nil {
		t.Fatalf("resetting builder performance store for rebuild: %v", err)
	}
	rebuildRuntimeBefore := captureBuilderPerfRuntime()
	rebuildIOBefore := captureBuilderPerfHistoryIO(store)
	started = time.Now()
	if err := builder.boot(context.Background()); err != nil {
		t.Fatalf("rebuilding builder performance store: %v", err)
	}
	rebuildElapsed := time.Since(started)
	report.Rebuild = builderPerfPhaseResult(
		t,
		"rebuild",
		store,
		proposals,
		rebuildElapsed,
		rebuildRuntimeBefore,
		captureBuilderPerfRuntime(),
		rebuildIOBefore,
		captureBuilderPerfHistoryIO(store),
	)
	if phaseOnly {
		report.Profile = "full-phase-only"
		report.Pending = []string{
			"phase-only evidence intentionally excludes restart, tail lag, primary-write, and verifier measurements",
			"the frozen local primary-write acceptance gate remains in its separate artifact and was not recalculated",
		}
		writeBuilderPerfEvidence(t, report)

		return
	}

	report.TailLag = measureBuilderPerfTailLag(t, lagSamples)
	report.WriteImpact = measureBuilderPerfPrimaryWriteImpact(t, profile, lagSamples)
	report.Verifier = measureBuilderPerfVerifierWriteImpact(t, source.mock, store, proposals, profile, lagSamples)
	report.Assertions = append(report.Assertions, builderPerfAssertion{
		Name:      "builder_tail_wall_clock_lag_p99",
		Target:    "< 500 ms",
		Observed:  report.TailLag.P99,
		Unit:      "milliseconds",
		Satisfied: report.TailLag.P99 < 500,
		Scope:     "local ticker-driven builder; one proposal appended at a time; asynchronous history publication",
	})
	report.Assertions = append(report.Assertions, builderPerfAssertion{
		Name:      "primary_durable_write_p99_regression_with_builder_tail",
		Target:    "< 5%",
		Observed:  report.WriteImpact.SteadyP99RegressionPercent,
		Unit:      "percent",
		Satisfied: profile == "smoke" || (report.WriteImpact.SteadyP99RegressionPercent < 5 && report.WriteImpact.BaselineBracketStable),
		Scope:     "pooled primary Commit+SyncWAL across interleaved A/B/A baseline/final-runtime trials; original four-target production FanOut, ticker-only 200 ms Builder without inline compaction, one source proposal every 5 ms, and the 1 s bounded maintenance worker; history WAL sync interval 5 s; HistoryVerifier excluded",
	})
	report.Assertions = append(report.Assertions, builderPerfAssertion{
		Name:      "primary_write_baseline_bracket_stability",
		Target:    fmt.Sprintf("<= %.0f%% p99 drift between pooled before/after A brackets", builderPerfBaselineDriftLimitPercent),
		Observed:  report.WriteImpact.PooledBaselineP99DriftPercent,
		Unit:      "percent",
		Satisfied: profile == "smoke" || report.WriteImpact.BaselineBracketStable,
		Scope:     "five interleaved A/B/A trials; before and after baseline pools each contain at least 1500 samples in local/full; per-trial median/min/max drift remains diagnostic; smoke is diagnostic only",
	})
	maxRunsAtLevel := 0
	catchupComplete := true
	maxRunsAfterCatchup := 0
	for _, trial := range report.WriteImpact.InterleavedTrials {
		maxRunsAtLevel = max(maxRunsAtLevel, trial.MaxRunsAtLevelAtStop)
		maxRunsAfterCatchup = max(maxRunsAfterCatchup, trial.MaxRunsAtLevelAfterCatchup)
		catchupComplete = catchupComplete && trial.RunsPerLevelAfterCatchup
	}
	publicationsPerMaintenanceInterval := int(math.Ceil(
		float64(builderPerfMaintenanceInterval) / float64(TickInterval),
	))
	burstBound := balancehistorystore.DefaultRunCompactionThreshold - 1 + publicationsPerMaintenanceInterval
	report.Assertions = append(report.Assertions, builderPerfAssertion{
		Name:      "production_history_run_burst_is_bounded",
		Target:    fmt.Sprintf("<= %d runs at any level before the next maintenance tick", burstBound),
		Observed:  float64(maxRunsAtLevel),
		Unit:      "runs",
		Satisfied: maxRunsAtLevel <= burstBound,
		Scope:     "threshold-1 settled runs plus at most ceil(maintenanceInterval/builderTickInterval) newly published runs; measured immediately after stopping the producer at 200 audits/s",
	})
	report.Assertions = append(report.Assertions, builderPerfAssertion{
		Name:      "production_history_reconverges_after_producer_stop",
		Target:    fmt.Sprintf("< %d runs at every level", balancehistorystore.DefaultRunCompactionThreshold),
		Observed:  float64(maxRunsAfterCatchup),
		Unit:      "runs",
		Satisfied: catchupComplete,
		Scope:     "ordinary CompactContext calls after source catch-up until no threshold-eligible level remains; no special rebuild or store reset",
	})

	writeBuilderPerfEvidence(t, report)
	if os.Getenv("PIT_PERF_ENFORCE") == "1" {
		for _, assertion := range report.Assertions {
			if !assertion.Satisfied {
				t.Errorf("performance assertion %s failed: observed %.4f %s, target %s", assertion.Name, assertion.Observed, assertion.Unit, assertion.Target)
			}
		}
	}
}

func builderPerfProfile(t *testing.T) (string, int, int) {
	t.Helper()

	switch profile := os.Getenv("PIT_PERF_PROFILE"); profile {
	case "", "local":
		return "local", 2_000, 60
	case "smoke":
		return "smoke", 250, 15
	case "full":
		return "full", 10_000, 300
	default:
		t.Fatalf("unknown PIT_PERF_PROFILE %q; expected smoke, local, or full", profile)

		return "", 0, 0
	}
}

func openBuilderPerfStore(t *testing.T) *balancehistorystore.Store {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	if err != nil {
		t.Fatalf("opening builder performance store: %v", err)
	}

	return store
}

func builderPerfPhaseResult(
	t *testing.T,
	name string,
	store *balancehistorystore.Store,
	proposals int,
	elapsed time.Duration,
	runtimeBefore, runtimeAfter builderPerfRuntimeSnapshot,
	ioBefore, ioAfter builderPerfHistoryIOSnapshot,
) builderPerfPhase {
	t.Helper()

	if err := store.DB().Flush(); err != nil {
		t.Fatalf("flushing builder performance %s store for capacity measurement: %v", name, err)
	}
	ioAfter = captureBuilderPerfHistoryIO(store)
	manifest, err := store.Manifest()
	if err != nil {
		t.Fatalf("reading builder performance %s manifest: %v", name, err)
	}
	effects := (proposals - 1) * 2
	diskBytes := store.DB().Metrics().DiskSpaceUsage()
	effectCount := float64(effects)
	cpuSeconds := runtimeAfter.GoCPUSeconds - runtimeBefore.GoCPUSeconds

	return builderPerfPhase{
		Name: name, AuditProposals: proposals, Postings: proposals - 1, Effects: effects,
		ElapsedMS:       float64(elapsed) / float64(time.Millisecond),
		AuditsPerSecond: float64(proposals) / elapsed.Seconds(), EffectsPerSecond: float64(effects) / elapsed.Seconds(),
		AllocatedBytesPerEffect: float64(runtimeAfter.AllocatedBytes-runtimeBefore.AllocatedBytes) / effectCount,
		AllocationsPerEffect:    float64(runtimeAfter.Allocations-runtimeBefore.Allocations) / effectCount,
		GoCPUSeconds:            cpuSeconds,
		GoCPUMicrosPerEffect:    cpuSeconds * float64(time.Second/time.Microsecond) / effectCount,
		DiskBytes:               diskBytes,
		BytesPerEffect:          float64(diskBytes) / effectCount,
		Runs:                    len(manifest.Runs),
		IO: &builderPerfIO{
			WALBytesIn:      ioAfter.WALBytesIn - ioBefore.WALBytesIn,
			WALBytesWritten: ioAfter.WALBytesWritten - ioBefore.WALBytesWritten,
			Flushes:         ioAfter.Flushes - ioBefore.Flushes,
			Compactions:     ioAfter.Compactions - ioBefore.Compactions,
			DiskBytesBefore: ioBefore.DiskBytes,
			DiskBytesAfter:  ioAfter.DiskBytes,
		},
	}
}

func captureBuilderPerfHistoryIO(store *balancehistorystore.Store) builderPerfHistoryIOSnapshot {
	metrics := store.DB().Metrics()

	return builderPerfHistoryIOSnapshot{
		WALBytesIn:      metrics.WAL.BytesIn,
		WALBytesWritten: metrics.WAL.BytesWritten,
		Flushes:         metrics.Flush.Count,
		Compactions:     metrics.Compact.Count,
		DiskBytes:       metrics.DiskSpaceUsage(),
	}
}

type builderPerfSteadyTrialConfig struct {
	name                       string
	trial                      int
	logicalCompactionThreshold int
	durabilityInterval         time.Duration
	producerInterval           time.Duration
	notificationInterval       time.Duration
	useNotifications           bool
	maintenanceEnabled         bool
	fanOutTargets              int
}

const (
	builderPerfMaintenanceInterval        = time.Second
	builderPerfMaxCompactionsPerPass      = 2
	builderPerfMinimumSteadyTrialDuration = 1200 * time.Millisecond
	builderPerfBaselineDriftLimitPercent  = 10.0
)

type builderPerfMaintenance struct {
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
	err      error

	passes      atomic.Uint64
	compactions atomic.Uint64
}

func startBuilderPerfMaintenance(
	store *balancehistorystore.Store,
	threshold int,
) *builderPerfMaintenance {
	ctx, cancel := context.WithCancel(context.Background())
	maintenance := &builderPerfMaintenance{
		cancel: cancel,
		done:   make(chan error, 1),
	}
	go func() {
		runPass := func() error {
			maintenance.passes.Add(1)
			for range builderPerfMaxCompactionsPerPass {
				compacted, err := store.CompactContext(ctx, threshold)
				if err != nil {
					return err
				}
				if !compacted {
					return nil
				}
				maintenance.compactions.Add(1)
			}

			return nil
		}

		// The production worker also performs one bounded pass immediately.
		if err := runPass(); err != nil {
			maintenance.done <- err

			return
		}
		ticker := time.NewTicker(builderPerfMaintenanceInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				maintenance.done <- nil

				return
			case <-ticker.C:
				if err := runPass(); err != nil {
					maintenance.done <- err

					return
				}
			}
		}
	}()

	return maintenance
}

func (m *builderPerfMaintenance) stop() error {
	if m == nil {
		return nil
	}
	m.stopOnce.Do(func() {
		m.cancel()
		m.err = <-m.done
		if errors.Is(m.err, context.Canceled) {
			m.err = nil
		}
	})

	return m.err
}

func builderPerfRunLevels(manifest balancehistorystore.Manifest) (int, bool) {
	byLevel := make(map[uint32]int)
	maxRuns := 0
	bounded := true
	for _, run := range manifest.Runs {
		byLevel[run.Level]++
		maxRuns = max(maxRuns, byLevel[run.Level])
		if byLevel[run.Level] >= balancehistorystore.DefaultRunCompactionThreshold {
			bounded = false
		}
	}

	return maxRuns, bounded
}

type builderPerfBackfillImpact struct {
	baseline            builderPerfLatency
	measurement         builderPerfLatency
	overlappedAllWrites bool
	manifest            balancehistorystore.Manifest
	historyIO           *builderPerfIO
}

func measureBuilderPerfPrimaryWriteImpact(
	t *testing.T,
	profile string,
	lagSamples int,
) builderPerfWriteImpact {
	t.Helper()

	writeSamples := 1_000
	diagnosticSamples := 300
	trialCount := 5
	backfillProposals := 30_000
	switch profile {
	case "smoke":
		writeSamples = max(60, lagSamples)
		diagnosticSamples = writeSamples
		trialCount = 1
		backfillProposals = 3_000
	case "full":
		backfillProposals = 100_000
	}

	root := t.TempDir()
	const productionCadence = 5 * time.Millisecond
	trials := make([]builderPerfWriteTrial, 0, trialCount)
	baselines := make([]builderPerfLatency, 0, trialCount)
	baselinesBefore := make([]builderPerfLatency, 0, trialCount)
	baselinesAfter := make([]builderPerfLatency, 0, trialCount)
	steadyMeasurements := make([]builderPerfLatency, 0, trialCount)
	regressions := make([]float64, 0, trialCount)
	baselineDrifts := make([]float64, 0, trialCount)
	var steadyProduced uint64
	for trial := 1; trial <= trialCount; trial++ {
		result := measureBuilderPerfSteadyTrial(t, root, writeSamples, builderPerfSteadyTrialConfig{
			name:                       "production_ticker_200ms_four_target_fanout",
			trial:                      trial,
			logicalCompactionThreshold: balancehistorystore.DefaultRunCompactionThreshold,
			durabilityInterval:         DefaultDurabilityInterval,
			producerInterval:           productionCadence,
			useNotifications:           false,
			maintenanceEnabled:         true,
			fanOutTargets:              4,
		})
		trials = append(trials, result)
		baselines = append(baselines, result.Baseline)
		baselinesBefore = append(baselinesBefore, result.BaselineBefore)
		baselinesAfter = append(baselinesAfter, result.BaselineAfter)
		steadyMeasurements = append(steadyMeasurements, result.SteadyTail)
		regressions = append(regressions, result.P99RegressionPercent)
		baselineDrifts = append(baselineDrifts, result.BaselineP99DriftPercent)
		steadyProduced += result.ProducedAudits
	}

	baseline := combineBuilderPerfLatencies("primary_commit_syncwal_baseline_interleaved", baselines)
	baselineBefore := combineBuilderPerfLatencies("primary_commit_syncwal_baseline_before_pooled", baselinesBefore)
	baselineAfter := combineBuilderPerfLatencies("primary_commit_syncwal_baseline_after_pooled", baselinesAfter)
	steady := combineBuilderPerfLatencies("primary_commit_syncwal_with_final_ticker_builder_and_maintenance", steadyMeasurements)
	pooledBaselineDrift := math.Abs(builderPerfP99Regression(baselineAfter, baselineBefore))
	baselineStable := pooledBaselineDrift <= builderPerfBaselineDriftLimitPercent
	slices.Sort(regressions)
	slices.Sort(baselineDrifts)
	conclusion := "pass"
	if !baselineStable {
		conclusion = "inconclusive: pooled A/B/A baseline p99 brackets drifted by more than 10%"
	} else if builderPerfP99Regression(steady, baseline) >= 5 {
		conclusion = "fail: final-runtime pooled p99 regression is at least 5%"
	}

	noCompaction := measureBuilderPerfSteadyTrial(t, root, diagnosticSamples, builderPerfSteadyTrialConfig{
		name:                       "production_notifications_without_logical_run_merges",
		trial:                      trialCount + 1,
		logicalCompactionThreshold: 1_000_000_000,
		durabilityInterval:         DefaultDurabilityInterval,
		producerInterval:           productionCadence,
		useNotifications:           true,
		maintenanceEnabled:         true,
		fanOutTargets:              5,
	})
	synchronousDurability := measureBuilderPerfSteadyTrial(t, root, diagnosticSamples, builderPerfSteadyTrialConfig{
		name:                       "production_notifications_sync_every_publication",
		trial:                      trialCount + 2,
		logicalCompactionThreshold: balancehistorystore.DefaultRunCompactionThreshold,
		durabilityInterval:         time.Nanosecond,
		producerInterval:           productionCadence,
		useNotifications:           true,
		maintenanceEnabled:         true,
		fanOutTargets:              5,
	})
	coalesced50MS := measureBuilderPerfSteadyTrial(t, root, diagnosticSamples, builderPerfSteadyTrialConfig{
		name:                       "production_notifications_coalesced_50ms",
		trial:                      trialCount + 3,
		logicalCompactionThreshold: balancehistorystore.DefaultRunCompactionThreshold,
		durabilityInterval:         DefaultDurabilityInterval,
		producerInterval:           productionCadence,
		notificationInterval:       50 * time.Millisecond,
		useNotifications:           true,
		maintenanceEnabled:         true,
		fanOutTargets:              5,
	})
	coalesced100MS := measureBuilderPerfSteadyTrial(t, root, diagnosticSamples, builderPerfSteadyTrialConfig{
		name:                       "production_notifications_coalesced_100ms",
		trial:                      trialCount + 4,
		logicalCompactionThreshold: balancehistorystore.DefaultRunCompactionThreshold,
		durabilityInterval:         DefaultDurabilityInterval,
		producerInterval:           productionCadence,
		notificationInterval:       100 * time.Millisecond,
		useNotifications:           true,
		maintenanceEnabled:         true,
		fanOutTargets:              5,
	})
	coalesced50MSNoCompaction := measureBuilderPerfSteadyTrial(t, root, diagnosticSamples, builderPerfSteadyTrialConfig{
		name:                       "production_notifications_coalesced_50ms_without_logical_run_merges",
		trial:                      trialCount + 5,
		logicalCompactionThreshold: 1_000_000_000,
		durabilityInterval:         DefaultDurabilityInterval,
		producerInterval:           productionCadence,
		notificationInterval:       50 * time.Millisecond,
		useNotifications:           true,
		maintenanceEnabled:         true,
		fanOutTargets:              5,
	})
	coalescedTicker := measureBuilderPerfSteadyTrial(t, root, diagnosticSamples, builderPerfSteadyTrialConfig{
		name:                       "coalesced_ticker_only",
		trial:                      trialCount + 6,
		logicalCompactionThreshold: balancehistorystore.DefaultRunCompactionThreshold,
		durabilityInterval:         DefaultDurabilityInterval,
		producerInterval:           productionCadence,
		useNotifications:           false,
		maintenanceEnabled:         true,
		fanOutTargets:              4,
	})
	backfill := measureBuilderPerfBackfillImpact(t, root, diagnosticSamples, backfillProposals)

	return builderPerfWriteImpact{
		Baseline:                            baseline,
		BaselineBefore:                      baselineBefore,
		BaselineAfter:                       baselineAfter,
		SteadyTail:                          steady,
		SteadyWithoutCompaction:             noCompaction.SteadyTail,
		SteadyWithSynchronousDurability:     synchronousDurability.SteadyTail,
		Coalesced50MSTail:                   coalesced50MS.SteadyTail,
		Coalesced100MSTail:                  coalesced100MS.SteadyTail,
		Coalesced50MSWithoutCompactionTail:  coalesced50MSNoCompaction.SteadyTail,
		CoalescedTickerTail:                 coalescedTicker.SteadyTail,
		RegulatedBackfill:                   backfill.measurement,
		BackfillBaseline:                    backfill.baseline,
		SteadyP99RegressionPercent:          builderPerfP99Regression(steady, baseline),
		MedianTrialP99RegressionPercent:     builderPerfMedian(regressions),
		MinTrialP99RegressionPercent:        regressions[0],
		MaxTrialP99RegressionPercent:        regressions[len(regressions)-1],
		MedianBaselineP99DriftPercent:       builderPerfMedian(baselineDrifts),
		MaxBaselineP99DriftPercent:          baselineDrifts[len(baselineDrifts)-1],
		PooledBaselineP99DriftPercent:       pooledBaselineDrift,
		BaselineBracketStable:               baselineStable,
		Conclusion:                          conclusion,
		NoCompactionP99Regression:           noCompaction.P99RegressionPercent,
		SynchronousDurabilityP99Regression:  synchronousDurability.P99RegressionPercent,
		Coalesced50MSP99Regression:          coalesced50MS.P99RegressionPercent,
		Coalesced100MSP99Regression:         coalesced100MS.P99RegressionPercent,
		Coalesced50MSNoCompactionP99:        coalesced50MSNoCompaction.P99RegressionPercent,
		CoalescedTickerP99Regression:        coalescedTicker.P99RegressionPercent,
		BackfillP99RegressionPercent:        builderPerfP99Regression(backfill.measurement, backfill.baseline),
		SteadyProducedAudits:                steadyProduced,
		PrimaryWarmupWrites:                 builderPerfPrimaryWarmupWrites,
		BackfillAuditProposals:              backfillProposals,
		BackfillOverlappedAllWrites:         backfill.overlappedAllWrites,
		BackfillManifestVersion:             backfill.manifest.Version,
		BackfillLogicalRuns:                 len(backfill.manifest.Runs),
		BackfillHistoryIO:                   backfill.historyIO,
		BackfillYieldMS:                     float64(DefaultBackfillYield) / float64(time.Millisecond),
		DurabilityIntervalMS:                float64(DefaultDurabilityInterval) / float64(time.Millisecond),
		InterleavedTrials:                   trials,
		LogicalCompactionDiagnostic:         noCompaction,
		SynchronousDurabilityDiagnostic:     synchronousDurability,
		Coalesced50MSDiagnostic:             coalesced50MS,
		Coalesced100MSDiagnostic:            coalesced100MS,
		Coalesced50MSNoCompactionDiagnostic: coalesced50MSNoCompaction,
		CoalescedTickerDiagnostic:           coalescedTicker,
	}
}

func measureBuilderPerfSteadyTrial(
	t *testing.T,
	root string,
	writeSamples int,
	config builderPerfSteadyTrialConfig,
) builderPerfWriteTrial {
	t.Helper()

	trialPath := fmt.Sprintf("%02d-%s", config.trial, config.name)
	writeCadence := config.producerInterval
	if minimum := builderPerfMinimumSteadyTrialDuration / time.Duration(writeSamples); writeCadence < minimum {
		writeCadence = minimum
	}
	baselineBeforeStore := openBuilderPerfPrimaryStore(t, filepath.Join(root, trialPath+"-primary-before"))
	baselineBeforeAttrs := attributes.New()
	_ = measureBuilderPerfPrimaryWrites(t, baselineBeforeStore, baselineBeforeAttrs, 0, builderPerfPrimaryWarmupWrites)
	baselineBefore := measureBuilderPerfPrimaryWritesCadenced(
		t,
		baselineBeforeStore,
		baselineBeforeAttrs,
		builderPerfPrimaryWarmupWrites,
		writeSamples,
		writeCadence,
	)
	baselineBefore.Name = config.name + "_baseline_before"
	if err := baselineBeforeStore.Close(); err != nil {
		t.Fatalf("closing primary before-baseline store for Builder trial %s: %v", config.name, err)
	}

	primary := openBuilderPerfPrimaryStore(t, filepath.Join(root, trialPath+"-primary-steady"))
	defer func() {
		if closeErr := primary.Close(); closeErr != nil {
			t.Errorf("closing steady primary store for Builder trial %s: %v", config.name, closeErr)
		}
	}()
	attrs := attributes.New()
	_ = measureBuilderPerfPrimaryWrites(t, primary, attrs, 0, builderPerfPrimaryWarmupWrites)

	source := newBuilderPerfSource(t)
	initial := source.appendCreateLedger(t)
	history, err := balancehistorystore.New(
		filepath.Join(root, trialPath+"-history"),
		logging.NopZap(),
		balancehistorystore.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("opening history store for Builder trial %s: %v", config.name, err)
	}
	defer func() {
		if closeErr := history.Close(); closeErr != nil {
			t.Errorf("closing history store for Builder trial %s: %v", config.name, closeErr)
		}
	}()

	var (
		notifications *signal.Notifications
		fanOut        *signal.FanOut
	)
	targets := make([]*signal.Notifications, 0, config.fanOutTargets)
	if config.useNotifications {
		notifications = signal.NewNotifications()
		for range max(0, config.fanOutTargets-1) {
			targets = append(targets, signal.NewNotifications())
		}
		targets = append(targets, notifications)
	} else {
		for range config.fanOutTargets {
			targets = append(targets, signal.NewNotifications())
		}
	}
	if len(targets) > 0 {
		fanOut = signal.NewFanOut(targets...)
	}
	historyIOBefore := captureBuilderPerfHistoryIO(history)
	builder := NewBuilder(
		source.mock,
		history,
		nil,
		notifications,
		builderPerfClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-steady-write-performance"),
		DefaultBatchSize,
		config.logicalCompactionThreshold,
		DefaultBackfillYield,
		config.durabilityInterval,
	)
	builder.Start()
	builderRunning := true
	var maintenance *builderPerfMaintenance
	if config.maintenanceEnabled {
		maintenance = startBuilderPerfMaintenance(history, config.logicalCompactionThreshold)
	}
	defer func() {
		if maintenance != nil {
			_ = maintenance.stop()
		}
		if builderRunning {
			_ = builder.Stop()
		}
	}()

	warmContext, cancelWarm := context.WithTimeout(context.Background(), 10*time.Second)
	if err := history.WaitForLogWatermark(warmContext, initial.LogSequence); err != nil {
		cancelWarm()
		t.Fatalf("waiting for Builder trial %s warm-up: %v", config.name, err)
	}
	cancelWarm()

	producerStop := make(chan struct{})
	producerDone := make(chan struct{})
	firstProduced := make(chan Position, 1)
	var latestProduced atomic.Uint64
	var producedAudits atomic.Uint64
	go func() {
		defer close(producerDone)
		publish := func(notify bool) Position {
			position := source.appendTransaction(t)
			latestProduced.Store(position.LogSequence)
			producedAudits.Add(1)
			if notify && fanOut != nil {
				fanOut.NotifyLogsCommitted(position.LogSequence)
			}

			return position
		}

		first := publish(false)
		if fanOut != nil {
			fanOut.NotifyLogsCommitted(first.LogSequence)
		}
		firstProduced <- first
		ticker := time.NewTicker(config.producerInterval)
		defer ticker.Stop()
		var notificationTicker *time.Ticker
		var notificationC <-chan time.Time
		if notifications != nil && config.notificationInterval > 0 {
			notificationTicker = time.NewTicker(config.notificationInterval)
			notificationC = notificationTicker.C
			defer notificationTicker.Stop()
		}
		for {
			select {
			case <-producerStop:
				return
			case <-ticker.C:
				publish(config.notificationInterval <= 0)
			case <-notificationC:
				fanOut.NotifyLogsCommitted(latestProduced.Load())
			}
		}
	}()
	producerRunning := true
	defer func() {
		if producerRunning {
			close(producerStop)
			<-producerDone
		}
	}()

	first := <-firstProduced
	firstContext, cancelFirst := context.WithTimeout(context.Background(), 10*time.Second)
	if err := history.WaitForLogWatermark(firstContext, first.LogSequence); err != nil {
		cancelFirst()
		t.Fatalf("waiting for first Builder trial %s publication: %v", config.name, err)
	}
	cancelFirst()

	steady := measureBuilderPerfPrimaryWritesCadenced(
		t,
		primary,
		attrs,
		builderPerfPrimaryWarmupWrites,
		writeSamples,
		writeCadence,
	)
	steady.Name = config.name + "_steady_tail"
	close(producerStop)
	<-producerDone
	producerRunning = false

	latestLog := latestProduced.Load()
	steadyContext, cancelSteady := context.WithTimeout(context.Background(), 30*time.Second)
	if err := history.WaitForLogWatermark(steadyContext, latestLog); err != nil {
		cancelSteady()
		t.Fatalf("waiting for Builder trial %s to catch up through log %d: %v", config.name, latestLog, err)
	}
	cancelSteady()
	var (
		maintenancePasses      uint64
		maintenanceCompactions uint64
	)
	if err := maintenance.stop(); err != nil {
		t.Fatalf("stopping Builder trial %s maintenance: %v", config.name, err)
	}
	if maintenance != nil {
		maintenancePasses = maintenance.passes.Load()
		maintenanceCompactions = maintenance.compactions.Load()
	}
	maintenance = nil
	if err := builder.Stop(); err != nil {
		t.Fatalf("stopping Builder trial %s: %v", config.name, err)
	}
	builderRunning = false

	atProducerStop, err := history.Manifest()
	if err != nil {
		t.Fatalf("reading Builder trial %s manifest before maintenance catch-up: %v", config.name, err)
	}
	catchupStarted := time.Now()
	catchupCompactions := 0
	for {
		compacted, compactErr := history.CompactContext(context.Background(), config.logicalCompactionThreshold)
		if compactErr != nil {
			t.Fatalf("catching up Builder trial %s maintenance: %v", config.name, compactErr)
		}
		if !compacted {
			break
		}
		catchupCompactions++
	}
	catchupElapsed := time.Since(catchupStarted)

	manifest, err := history.Manifest()
	if err != nil {
		t.Fatalf("reading Builder trial %s manifest: %v", config.name, err)
	}
	historyIOAfter := captureBuilderPerfHistoryIO(history)
	maxRunsAtLevel, runsPerLevelBounded := builderPerfRunLevels(atProducerStop)
	maxRunsAfterCatchup, runsPerLevelAfterCatchup := builderPerfRunLevels(manifest)
	baselineAfterStore := openBuilderPerfPrimaryStore(t, filepath.Join(root, trialPath+"-primary-after"))
	baselineAfterAttrs := attributes.New()
	_ = measureBuilderPerfPrimaryWrites(t, baselineAfterStore, baselineAfterAttrs, 0, builderPerfPrimaryWarmupWrites)
	baselineAfter := measureBuilderPerfPrimaryWritesCadenced(
		t,
		baselineAfterStore,
		baselineAfterAttrs,
		builderPerfPrimaryWarmupWrites,
		writeSamples,
		writeCadence,
	)
	baselineAfter.Name = config.name + "_baseline_after"
	if err := baselineAfterStore.Close(); err != nil {
		t.Fatalf("closing primary after-baseline store for Builder trial %s: %v", config.name, err)
	}
	baseline := combineBuilderPerfLatencies(
		config.name+"_baseline_bracket",
		[]builderPerfLatency{baselineBefore, baselineAfter},
	)
	baselineDrift := math.Abs(builderPerfP99Regression(baselineAfter, baselineBefore))

	return builderPerfWriteTrial{
		Name:                       config.name,
		Trial:                      config.trial,
		Baseline:                   baseline,
		BaselineBefore:             baselineBefore,
		BaselineAfter:              baselineAfter,
		BaselineP99DriftPercent:    baselineDrift,
		BaselineStable:             baselineDrift <= builderPerfBaselineDriftLimitPercent,
		SteadyTail:                 steady,
		P99RegressionPercent:       builderPerfP99Regression(steady, baseline),
		ProducedAudits:             producedAudits.Load(),
		PrimaryWarmupWrites:        builderPerfPrimaryWarmupWrites,
		LogicalCompactionThreshold: config.logicalCompactionThreshold,
		DurabilityIntervalMS:       float64(config.durabilityInterval) / float64(time.Millisecond),
		ProducerIntervalMS:         float64(config.producerInterval) / float64(time.Millisecond),
		NotificationIntervalMS:     float64(config.notificationInterval) / float64(time.Millisecond),
		UsesLogNotifications:       config.useNotifications,
		FanOutTargets:              config.fanOutTargets,
		MaintenanceEnabled:         config.maintenanceEnabled,
		MaintenanceIntervalMS:      float64(builderPerfMaintenanceInterval) / float64(time.Millisecond),
		MaintenancePasses:          maintenancePasses,
		MaintenanceCompactions:     maintenanceCompactions,
		RunsAtProducerStop:         len(atProducerStop.Runs),
		MaxRunsAtLevelAtStop:       maxRunsAtLevel,
		RunsPerLevelBoundedAtStop:  runsPerLevelBounded,
		CatchupCompactions:         catchupCompactions,
		CatchupElapsedMS:           float64(catchupElapsed) / float64(time.Millisecond),
		MaxRunsAtLevelAfterCatchup: maxRunsAfterCatchup,
		RunsPerLevelAfterCatchup:   runsPerLevelAfterCatchup,
		ManifestVersion:            manifest.Version,
		LogicalRuns:                len(manifest.Runs),
		HistoryIO:                  diffBuilderPerfHistoryIO(historyIOBefore, historyIOAfter),
	}
}

func measureBuilderPerfBackfillImpact(
	t *testing.T,
	root string,
	writeSamples int,
	backfillProposals int,
) builderPerfBackfillImpact {
	t.Helper()

	primary, err := dal.NewStore(
		filepath.Join(root, "backfill-primary"),
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-backfill-primary-write-performance"),
		dal.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("opening primary store for Builder backfill measurement: %v", err)
	}
	defer func() {
		if closeErr := primary.Close(); closeErr != nil {
			t.Errorf("closing primary Builder backfill store: %v", closeErr)
		}
	}()
	attrs := attributes.New()
	_ = measureBuilderPerfPrimaryWrites(t, primary, attrs, 0, builderPerfPrimaryWarmupWrites)
	baseline := measureBuilderPerfPrimaryWrites(t, primary, attrs, builderPerfPrimaryWarmupWrites, writeSamples)
	baseline.Name = "regulated_backfill_paired_baseline"

	source := newBuilderPerfSource(t)
	source.appendCreateLedger(t)
	for range backfillProposals - 1 {
		source.appendTransaction(t)
	}
	history, err := balancehistorystore.New(
		filepath.Join(root, "backfill-history"),
		logging.NopZap(),
		balancehistorystore.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("opening regulated-backfill Builder store: %v", err)
	}
	defer func() {
		if closeErr := history.Close(); closeErr != nil {
			t.Errorf("closing regulated-backfill Builder store: %v", closeErr)
		}
	}()
	historyIOBefore := captureBuilderPerfHistoryIO(history)
	builder := NewBuilder(
		source.mock,
		history,
		nil,
		nil,
		builderPerfClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-backfill-write-performance"),
		DefaultBatchSize,
		balancehistorystore.DefaultRunCompactionThreshold,
		DefaultBackfillYield,
		DefaultDurabilityInterval,
	)
	backfillDone := make(chan error, 1)
	var backfillCompleted atomic.Bool
	go func() {
		bootErr := builder.boot(context.Background())
		backfillCompleted.Store(true)
		backfillDone <- bootErr
	}()

	firstBackfillLog := uint64(min(DefaultBatchSize, backfillProposals))
	backfillContext, cancelBackfill := context.WithTimeout(context.Background(), 30*time.Second)
	if err := history.WaitForLogWatermark(backfillContext, firstBackfillLog); err != nil {
		cancelBackfill()
		t.Fatalf("waiting for regulated Builder backfill to start: %v", err)
	}
	cancelBackfill()
	measurement := measureBuilderPerfPrimaryWrites(
		t,
		primary,
		attrs,
		builderPerfPrimaryWarmupWrites+writeSamples,
		writeSamples,
	)
	measurement.Name = "primary_commit_syncwal_with_real_builder_regulated_backfill"
	overlappedAllWrites := !backfillCompleted.Load()
	select {
	case bootErr := <-backfillDone:
		if bootErr != nil {
			t.Fatalf("running regulated Builder backfill: %v", bootErr)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("timed out waiting for regulated Builder backfill")
	}
	manifest, err := history.Manifest()
	if err != nil {
		t.Fatalf("reading regulated Builder backfill manifest: %v", err)
	}
	if manifest.AuditWatermark != uint64(backfillProposals) {
		t.Fatalf("regulated Builder stopped at audit %d, want %d", manifest.AuditWatermark, backfillProposals)
	}
	if err := builder.Stop(); err != nil {
		t.Fatalf("stopping regulated-backfill Builder: %v", err)
	}

	return builderPerfBackfillImpact{
		baseline:            baseline,
		measurement:         measurement,
		overlappedAllWrites: overlappedAllWrites,
		manifest:            manifest,
		historyIO:           diffBuilderPerfHistoryIO(historyIOBefore, captureBuilderPerfHistoryIO(history)),
	}
}

func diffBuilderPerfHistoryIO(before, after builderPerfHistoryIOSnapshot) *builderPerfIO {
	return &builderPerfIO{
		WALBytesIn:      after.WALBytesIn - before.WALBytesIn,
		WALBytesWritten: after.WALBytesWritten - before.WALBytesWritten,
		Flushes:         after.Flushes - before.Flushes,
		Compactions:     after.Compactions - before.Compactions,
		DiskBytesBefore: before.DiskBytes,
		DiskBytesAfter:  after.DiskBytes,
	}
}

func combineBuilderPerfLatencies(name string, measurements []builderPerfLatency) builderPerfLatency {
	samples := make([]int64, 0)
	var (
		allocatedBytes float64
		allocations    float64
		goCPUSeconds   float64
		io             builderPerfIO
		hasIO          bool
	)
	for _, measurement := range measurements {
		samples = append(samples, measurement.SamplesNS...)
		allocatedBytes += measurement.AllocatedBytesPerOp * float64(measurement.Samples)
		allocations += measurement.AllocationsPerOp * float64(measurement.Samples)
		goCPUSeconds += measurement.GoCPUSeconds
		if measurement.IO != nil {
			hasIO = true
			io.WALBytesIn += measurement.IO.WALBytesIn
			io.WALBytesWritten += measurement.IO.WALBytesWritten
			io.Flushes += measurement.IO.Flushes
			io.Compactions += measurement.IO.Compactions
			io.DiskBytesBefore += measurement.IO.DiskBytesBefore
			io.DiskBytesAfter += measurement.IO.DiskBytesAfter
		}
	}
	combined := summarizeBuilderPerfLatency(name, samples)
	combined.AllocatedBytesPerOp = allocatedBytes / float64(combined.Samples)
	combined.AllocationsPerOp = allocations / float64(combined.Samples)
	combined.GoCPUSeconds = goCPUSeconds
	combined.GoCPUMicrosecondsPer = goCPUSeconds * float64(time.Second/time.Microsecond) / float64(combined.Samples)
	if hasIO {
		combined.IO = &io
	}

	return combined
}

func builderPerfP99Regression(measurement, baseline builderPerfLatency) float64 {
	if baseline.P99 == 0 {
		return 0
	}

	return (measurement.P99/baseline.P99 - 1) * 100
}

func builderPerfMedian(ordered []float64) float64 {
	if len(ordered) == 0 {
		return 0
	}
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		return ordered[middle]
	}

	return (ordered[middle-1] + ordered[middle]) / 2
}

func measureBuilderPerfVerifierWriteImpact(
	t *testing.T,
	source Source,
	history *balancehistorystore.Store,
	proposals int,
	profile string,
	lagSamples int,
) builderPerfVerifierImpact {
	t.Helper()

	writeSamples := 300
	switch profile {
	case "smoke":
		writeSamples = max(60, lagSamples)
	case "full":
		writeSamples = 1_000
	}
	primary, err := dal.NewStore(
		filepath.Join(t.TempDir(), "verifier-primary"),
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-verifier-primary-write-performance"),
		dal.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("opening primary store for full-verifier overlap measurement: %v", err)
	}
	defer func() {
		if closeErr := primary.Close(); closeErr != nil {
			t.Errorf("closing primary full-verifier overlap store: %v", closeErr)
		}
	}()
	attrs := attributes.New()
	_ = measureBuilderPerfPrimaryWrites(t, primary, attrs, 0, builderPerfPrimaryWarmupWrites)
	baseline := measureBuilderPerfPrimaryWrites(t, primary, attrs, builderPerfPrimaryWarmupWrites, writeSamples)
	baseline.Name = "full_verifier_paired_baseline"

	verifierConfig := DefaultVerifierConfig()
	verifierConfig.ScratchParent = filepath.Join(history.Path(), "verifier-scratch")
	verifier, err := NewHistoryVerifier(
		source,
		history,
		builderPerfClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-full-verifier-write-performance"),
		verifierConfig,
	)
	if err != nil {
		t.Fatalf("creating full verifier for write-impact measurement: %v", err)
	}
	type verifierResult struct {
		elapsed time.Duration
		err     error
	}
	ready := make(chan struct{})
	done := make(chan verifierResult, 1)
	var completed atomic.Bool
	go func() {
		close(ready)
		started := time.Now()
		verifyErr := verifier.Verify(context.Background())
		completed.Store(true)
		done <- verifierResult{elapsed: time.Since(started), err: verifyErr}
	}()
	<-ready
	overlap := measureBuilderPerfPrimaryWrites(
		t,
		primary,
		attrs,
		builderPerfPrimaryWarmupWrites+writeSamples,
		writeSamples,
	)
	overlap.Name = "primary_commit_syncwal_with_full_history_verifier"
	overlappedAllWrites := !completed.Load()
	result := <-done
	if result.err != nil {
		t.Fatalf("running full-verifier write-impact measurement: %v", result.err)
	}

	return builderPerfVerifierImpact{
		Baseline:               baseline,
		FullVerifierOverlap:    overlap,
		P99RegressionPercent:   builderPerfP99Regression(overlap, baseline),
		VerifierElapsedMS:      float64(result.elapsed) / float64(time.Millisecond),
		OverlappedAllWrites:    overlappedAllWrites,
		AuthoritativeProposals: proposals,
		Scope:                  "separate local startup/daily full semantic replay diagnostic; excluded from the steady-tail write gate",
	}
}

func measureBuilderPerfPrimaryWrites(
	t *testing.T,
	store *dal.Store,
	attrs *attributes.Attributes,
	start, samples int,
) builderPerfLatency {
	return measureBuilderPerfPrimaryWritesCadenced(t, store, attrs, start, samples, 0)
}

func openBuilderPerfPrimaryStore(t *testing.T, path string) *dal.Store {
	t.Helper()

	store, err := dal.NewStore(
		path,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-primary-write-performance"),
		dal.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("opening Builder primary performance store %s: %v", path, err)
	}

	return store
}

func measureBuilderPerfPrimaryWritesCadenced(
	t *testing.T,
	store *dal.Store,
	attrs *attributes.Attributes,
	start, samples int,
	interval time.Duration,
) builderPerfLatency {
	t.Helper()

	ioBefore := captureBuilderPerfPrimaryIO(t, store)
	runtimeBefore := captureBuilderPerfRuntime()
	durations := make([]int64, samples)
	for index := range samples {
		if interval > 0 {
			time.Sleep(interval)
		}
		sequence := start + index
		batch := store.OpenWriteSession()
		key := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "pit-builder-performance", Account: fmt.Sprintf("write:%08d", sequence)},
			Asset:      "USD/2", AssetBase: "USD", AssetPrecision: 2,
		}
		started := time.Now()
		_, setErr := attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(uint64(sequence + 1)),
		})
		if setErr != nil {
			_ = batch.Cancel()
			t.Fatalf("staging Builder primary write %d: %v", sequence, setErr)
		}
		if err := batch.Commit(); err != nil {
			_ = batch.Cancel()
			t.Fatalf("committing Builder primary write %d: %v", sequence, err)
		}
		if err := store.SyncWAL(); err != nil {
			t.Fatalf("syncing Builder primary write %d: %v", sequence, err)
		}
		durations[index] = time.Since(started).Nanoseconds()
	}
	runtimeAfter := captureBuilderPerfRuntime()
	measurement := summarizeBuilderPerfLatency("primary_commit_syncwal", durations)
	attachBuilderPerfRuntime(&measurement, runtimeBefore, runtimeAfter)
	measurement.IO = diffBuilderPerfPrimaryIO(ioBefore, captureBuilderPerfPrimaryIO(t, store))

	return measurement
}

func captureBuilderPerfPrimaryIO(t *testing.T, store *dal.Store) *servicepb.PebbleMetrics {
	t.Helper()

	metrics, ok := store.GetMetrics().(*servicepb.PebbleMetrics)
	if !ok || metrics == nil {
		t.Fatal("primary store did not expose Pebble metrics")
	}

	return metrics
}

func diffBuilderPerfPrimaryIO(before, after *servicepb.PebbleMetrics) *builderPerfIO {
	return &builderPerfIO{
		WALBytesIn:      after.GetWal().GetBytesIn() - before.GetWal().GetBytesIn(),
		WALBytesWritten: after.GetWal().GetBytesWritten() - before.GetWal().GetBytesWritten(),
		Flushes:         after.GetFlush().GetCount() - before.GetFlush().GetCount(),
		Compactions:     after.GetCompact().GetCount() - before.GetCompact().GetCount(),
		DiskBytesBefore: before.GetDiskSpaceUsage(),
		DiskBytesAfter:  after.GetDiskSpaceUsage(),
	}
}

func measureBuilderPerfTailLag(t *testing.T, samples int) builderPerfLatency {
	t.Helper()

	source := newBuilderPerfSource(t)
	initial := source.appendCreateLedger(t)
	store := openBuilderPerfStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing builder lag store: %v", err)
		}
	}()
	builder := NewBuilder(
		source.mock,
		store,
		nil,
		nil,
		builderPerfClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-builder-lag-performance"),
		DefaultBatchSize,
		balancehistorystore.DefaultRunCompactionThreshold,
		DefaultBackfillYield,
		DefaultDurabilityInterval,
	)
	builder.Start()
	defer func() {
		if err := builder.Stop(); err != nil {
			t.Errorf("stopping builder lag measurement: %v", err)
		}
	}()
	warmContext, cancelWarm := context.WithTimeout(context.Background(), 10*time.Second)
	if err := store.WaitForLogWatermark(warmContext, initial.LogSequence); err != nil {
		cancelWarm()
		t.Fatalf("waiting for builder lag warm-up: %v", err)
	}
	cancelWarm()

	runtimeBefore := captureBuilderPerfRuntime()
	durations := make([]int64, samples)
	for index := range samples {
		started := time.Now()
		position := source.appendTransaction(t)
		waitContext, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
		err := store.WaitForLogWatermark(waitContext, position.LogSequence)
		cancelWait()
		if err != nil {
			t.Fatalf("waiting for builder lag sample %d through log %d: %v", index, position.LogSequence, err)
		}
		durations[index] = time.Since(started).Nanoseconds()
	}
	runtimeAfter := captureBuilderPerfRuntime()

	measurement := summarizeBuilderPerfLatency("append_to_published_manifest", durations)
	attachBuilderPerfRuntime(&measurement, runtimeBefore, runtimeAfter)

	return measurement
}

func summarizeBuilderPerfLatency(name string, samples []int64) builderPerfLatency {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	total := int64(0)
	for _, sample := range samples {
		total += sample
	}
	toMilliseconds := func(nanoseconds int64) float64 {
		return float64(nanoseconds) / float64(time.Millisecond)
	}

	elapsedSeconds := float64(total) / float64(time.Second)

	return builderPerfLatency{
		Name: name, Unit: "milliseconds", Samples: len(samples),
		Min:                 toMilliseconds(ordered[0]),
		P50:                 toMilliseconds(builderPerfPercentile(ordered, 0.50)),
		P95:                 toMilliseconds(builderPerfPercentile(ordered, 0.95)),
		P99:                 toMilliseconds(builderPerfPercentile(ordered, 0.99)),
		Max:                 toMilliseconds(ordered[len(ordered)-1]),
		Mean:                toMilliseconds(total / int64(len(samples))),
		OperationsPerSecond: float64(len(samples)) / elapsedSeconds,
		SamplesNS:           slices.Clone(samples),
	}
}

func captureBuilderPerfRuntime() builderPerfRuntimeSnapshot {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	cpuSamples := []runtimemetrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
	}
	runtimemetrics.Read(cpuSamples)
	cpuSeconds := float64(0)
	for _, sample := range cpuSamples {
		cpuSeconds += sample.Value.Float64()
	}

	return builderPerfRuntimeSnapshot{
		AllocatedBytes: memory.TotalAlloc,
		Allocations:    memory.Mallocs,
		GoCPUSeconds:   cpuSeconds,
	}
}

func attachBuilderPerfRuntime(
	measurement *builderPerfLatency,
	before, after builderPerfRuntimeSnapshot,
) {
	if measurement == nil || measurement.Samples == 0 {
		return
	}
	operations := float64(measurement.Samples)
	measurement.AllocatedBytesPerOp = float64(after.AllocatedBytes-before.AllocatedBytes) / operations
	measurement.AllocationsPerOp = float64(after.Allocations-before.Allocations) / operations
	measurement.GoCPUSeconds = after.GoCPUSeconds - before.GoCPUSeconds
	measurement.GoCPUMicrosecondsPer = measurement.GoCPUSeconds * float64(time.Second/time.Microsecond) / operations
}

func builderPerfPercentile(ordered []int64, quantile float64) int64 {
	index := int(math.Ceil(quantile*float64(len(ordered)))) - 1
	index = max(0, min(index, len(ordered)-1))

	return ordered[index]
}

func writeBuilderPerfEvidence(t *testing.T, report builderPerfReport) {
	t.Helper()

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshaling builder performance evidence: %v", err)
	}
	output := os.Getenv("PIT_BUILDER_PERF_OUTPUT")
	if output == "" {
		t.Logf("PIT_BUILDER_PERF_OUTPUT is unset; raw evidence follows:\n%s", encoded)

		return
	}
	if !filepath.IsAbs(output) {
		cwd, cwdErr := os.Getwd()
		if cwdErr != nil {
			t.Fatalf("resolving builder performance output path: %v", cwdErr)
		}
		output = filepath.Join(cwd, output)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		t.Fatalf("creating builder performance output directory: %v", err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("writing builder performance evidence %s: %v", output, err)
	}
	t.Logf("wrote builder performance evidence to %s", output)
}

func builderPerfValueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}

	return value
}

func TestBuilderPerfPercentileUsesNearestRank(t *testing.T) {
	t.Parallel()

	samples := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	if got := builderPerfPercentile(samples, 0.50); got != 10 {
		t.Fatalf("p50 = %d, want 10", got)
	}
	if got := builderPerfPercentile(samples, 0.95); got != 19 {
		t.Fatalf("p95 = %d, want 19", got)
	}
	if got := builderPerfPercentile(samples, 0.99); got != 20 {
		t.Fatalf("p99 = %d, want 20", got)
	}
}
