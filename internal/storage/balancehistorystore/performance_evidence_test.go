package balancehistorystore_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	perfDayMicros       = uint64(24 * time.Hour / time.Microsecond)
	perfLedgerID        = uint32(1)
	perfLedger          = "pit-performance"
	perfRunCountFormula = "(threshold-1)*(1+ceil(log_threshold(publications)))"
)

var perfResultSink any

type perfProfile struct {
	Name                  string
	Samples               int
	ShapeSamples          int
	Days                  int
	Accounts              int
	AssetBuckets          int
	Colors                int
	PostingsPerDay        int
	Runs                  int
	CardinalitySamples    int
	InterferenceSamples   int
	InterferenceBatchSize int
}

type perfDatasetConfig struct {
	Name                   string `json:"name"`
	Days                   int    `json:"days"`
	Accounts               int    `json:"accounts"`
	AssetBuckets           int    `json:"assetBuckets"`
	Colors                 int    `json:"colors"`
	PostingsPerDay         int    `json:"postingsPerDay"`
	Runs                   int    `json:"publications"`
	BackdatedRate          int    `json:"backdatedPercent"`
	CompactEachPublication bool   `json:"compactEachPublication"`
}

type perfLatency struct {
	Name                 string         `json:"name"`
	Category             string         `json:"category"`
	Unit                 string         `json:"unit"`
	Parameters           map[string]any `json:"parameters,omitempty"`
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
	IO                   *perfReadIO    `json:"io,omitempty"`
	ObjectIO             *perfObjectIO  `json:"objectIo,omitempty"`
	SamplesNS            []int64        `json:"samplesNs"`
}

type perfReadIO struct {
	BlockCacheHits   int64  `json:"blockCacheHits"`
	BlockCacheMisses int64  `json:"blockCacheMisses"`
	WALBytesIn       uint64 `json:"walBytesIn"`
	WALBytesWritten  uint64 `json:"walBytesWritten"`
	Flushes          int64  `json:"flushes"`
	Compactions      int64  `json:"compactions"`
	DiskBytesBefore  uint64 `json:"diskBytesBefore"`
	DiskBytesAfter   uint64 `json:"diskBytesAfter"`
}

type perfObjectIO struct {
	Backend            string  `json:"backend"`
	Fetches            int     `json:"fetches"`
	FetchesPerSample   float64 `json:"fetchesPerSample"`
	BytesRead          uint64  `json:"bytesRead"`
	SimulatedLatencyMS float64 `json:"simulatedLatencyMs"`
	ObservedFetchP50MS float64 `json:"observedFetchP50Ms"`
	ObservedFetchP95MS float64 `json:"observedFetchP95Ms"`
	ObservedFetchP99MS float64 `json:"observedFetchP99Ms"`
	ObservedFetchMaxMS float64 `json:"observedFetchMaxMs"`
	SamplesNS          []int64 `json:"samplesNs"`
}

type perfRuntimeSnapshot struct {
	AllocatedBytes uint64
	Allocations    uint64
	GoCPUSeconds   float64
}

type perfObjectSnapshot struct {
	bytesRead uint64
	durations []int64
}

type perfObservedColdStorage struct {
	delegate         coldstorage.ColdStorage
	simulatedLatency time.Duration

	mu        sync.Mutex
	bytesRead uint64
	durations []int64
}

func newPerfObservedColdStorage(
	delegate coldstorage.ColdStorage,
	simulatedLatency time.Duration,
) *perfObservedColdStorage {
	return &perfObservedColdStorage{delegate: delegate, simulatedLatency: simulatedLatency}
}

func (s *perfObservedColdStorage) Archive(
	ctx context.Context,
	bucketID string,
	chapterID uint64,
	data io.Reader,
	checksum []byte,
) error {
	return s.delegate.Archive(ctx, bucketID, chapterID, data, checksum)
}

func (s *perfObservedColdStorage) Exists(
	ctx context.Context,
	bucketID string,
	chapterID uint64,
) (bool, error) {
	return s.delegate.Exists(ctx, bucketID, chapterID)
}

func (s *perfObservedColdStorage) ExpectedChecksum(
	ctx context.Context,
	bucketID string,
	chapterID uint64,
) ([]byte, error) {
	return s.delegate.ExpectedChecksum(ctx, bucketID, chapterID)
}

func (s *perfObservedColdStorage) Checksum(
	ctx context.Context,
	bucketID string,
	chapterID uint64,
) ([]byte, error) {
	return s.delegate.Checksum(ctx, bucketID, chapterID)
}

func (s *perfObservedColdStorage) DestinationIdentity() (string, error) {
	identified, ok := s.delegate.(coldstorage.DestinationIdentified)
	if !ok {
		return "", errors.New("performance cold storage delegate has no destination identity")
	}

	return identified.DestinationIdentity()
}

func (s *perfObservedColdStorage) Fetch(
	ctx context.Context,
	bucketID string,
	chapterID uint64,
) (io.ReadCloser, error) {
	started := time.Now()
	if s.simulatedLatency > 0 {
		timer := time.NewTimer(s.simulatedLatency)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			s.recordFetch(time.Since(started), 0)

			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	reader, err := s.delegate.Fetch(ctx, bucketID, chapterID)
	if err != nil {
		s.recordFetch(time.Since(started), 0)

		return nil, err
	}

	return &perfObservedReadCloser{
		ReadCloser: reader,
		started:    started,
		finish:     s.recordFetch,
	}, nil
}

func (s *perfObservedColdStorage) recordFetch(elapsed time.Duration, bytesRead uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.bytesRead += bytesRead
	s.durations = append(s.durations, elapsed.Nanoseconds())
}

func (s *perfObservedColdStorage) snapshot() perfObjectSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	return perfObjectSnapshot{
		bytesRead: s.bytesRead,
		durations: slices.Clone(s.durations),
	}
}

type perfObservedReadCloser struct {
	io.ReadCloser

	started time.Time
	bytes   uint64
	finish  func(time.Duration, uint64)
	once    sync.Once
}

func (r *perfObservedReadCloser) Read(buffer []byte) (int, error) {
	read, err := r.ReadCloser.Read(buffer)
	r.bytes += uint64(read)
	if errors.Is(err, io.EOF) {
		r.record()
	}

	return read, err
}

func (r *perfObservedReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.record()

	return err
}

func (r *perfObservedReadCloser) record() {
	r.once.Do(func() {
		r.finish(time.Since(r.started), r.bytes)
	})
}

func diffPerfObjectIO(
	before, after perfObjectSnapshot,
	querySamples int,
	simulatedLatency time.Duration,
) *perfObjectIO {
	durations := slices.Clone(after.durations[len(before.durations):])
	slices.Sort(durations)
	fetches := len(durations)
	result := &perfObjectIO{
		Backend:            "local-filesystem",
		Fetches:            fetches,
		BytesRead:          after.bytesRead - before.bytesRead,
		SimulatedLatencyMS: float64(simulatedLatency) / float64(time.Millisecond),
		SamplesNS:          durations,
	}
	if querySamples > 0 {
		result.FetchesPerSample = float64(fetches) / float64(querySamples)
	}
	if fetches == 0 {
		return result
	}
	toMilliseconds := func(nanoseconds int64) float64 {
		return float64(nanoseconds) / float64(time.Millisecond)
	}
	result.ObservedFetchP50MS = toMilliseconds(perfPercentile(durations, 0.50))
	result.ObservedFetchP95MS = toMilliseconds(perfPercentile(durations, 0.95))
	result.ObservedFetchP99MS = toMilliseconds(perfPercentile(durations, 0.99))
	result.ObservedFetchMaxMS = toMilliseconds(durations[len(durations)-1])

	return result
}

func mergePerfObjectIO(
	measurements []perfLatency,
	category string,
	simulatedLatency time.Duration,
) perfObjectIO {
	after := perfObjectSnapshot{}
	querySamples := 0
	for _, measurement := range measurements {
		if measurement.Category != category || measurement.ObjectIO == nil {
			continue
		}
		querySamples += measurement.Samples
		after.bytesRead += measurement.ObjectIO.BytesRead
		after.durations = append(after.durations, measurement.ObjectIO.SamplesNS...)
	}

	return *diffPerfObjectIO(perfObjectSnapshot{}, after, querySamples, simulatedLatency)
}

type perfDatasetResult struct {
	Config               perfDatasetConfig `json:"config"`
	Postings             int               `json:"postings"`
	Effects              int               `json:"effects"`
	PublishedRuns        int               `json:"publishedLevelZeroRuns"`
	LogicalCompactions   int               `json:"logicalCompactions"`
	TierPasses           int               `json:"tierPasses"`
	TieredRuns           int               `json:"tieredRuns"`
	FinalLogicalRuns     int               `json:"finalLogicalRuns"`
	FinalHotRuns         int               `json:"finalHotRuns"`
	FinalColdRuns        int               `json:"finalColdRuns"`
	RunCountFormula      string            `json:"runCountFormula"`
	RunCountLimit        int               `json:"runCountLimit"`
	RunCountBounded      bool              `json:"runCountBounded"`
	RunsPerLevelBounded  bool              `json:"runsPerLevelBounded"`
	RunLevels            []perfRunLevel    `json:"runLevels"`
	AgingElapsedMS       float64           `json:"agingElapsedMs"`
	Publication          perfLatency       `json:"publication"`
	PublicationEffectsS  float64           `json:"publicationEffectsPerSecond"`
	DiskBytes            uint64            `json:"diskBytes"`
	PebbleCompactionDebt uint64            `json:"pebbleCompactionDebtBytes"`
	WALBytesWritten      uint64            `json:"walBytesWritten"`
	BytesPerPosting      float64           `json:"bytesPerPosting"`
	BytesPerEffect       float64           `json:"bytesPerEffect"`
}

type perfRunLevel struct {
	Level     uint32 `json:"level"`
	HotRuns   int    `json:"hotRuns"`
	ColdRuns  int    `json:"coldRuns"`
	TotalRuns int    `json:"totalRuns"`
	Limit     int    `json:"exclusiveLimit"`
	Bounded   bool   `json:"bounded"`
}

type perfAssertion struct {
	Name      string  `json:"name"`
	Target    string  `json:"target"`
	Observed  float64 `json:"observed"`
	Unit      string  `json:"unit"`
	Satisfied bool    `json:"satisfied"`
	Scope     string  `json:"scope"`
}

type perfReplicaDigest struct {
	Days                   int     `json:"days"`
	ReplicaAPublications   int     `json:"replicaAPublications"`
	ReplicaBPublications   int     `json:"replicaBPublications"`
	ReplicaARuns           int     `json:"replicaARuns"`
	ReplicaBRuns           int     `json:"replicaBRuns"`
	LogicalDigestA         string  `json:"logicalDigestA"`
	LogicalDigestB         string  `json:"logicalDigestB"`
	SemanticDigestA        string  `json:"semanticDigestA"`
	SemanticDigestB        string  `json:"semanticDigestB"`
	LogicalDigestEqual     bool    `json:"logicalDigestEqual"`
	SemanticDigestEqual    bool    `json:"semanticDigestEqual"`
	DifferentPhysicalShape bool    `json:"differentPhysicalShape"`
	ElapsedMS              float64 `json:"elapsedMs"`
}

type perfCompaction struct {
	Operations                 int         `json:"operations"`
	ElapsedMS                  float64     `json:"elapsedMs"`
	RunsBefore                 int         `json:"runsBefore"`
	RunsAfter                  int         `json:"runsAfter"`
	DiskBytesBefore            uint64      `json:"diskBytesBefore"`
	DiskBytesAfterImmediate    uint64      `json:"diskBytesAfterImmediate"`
	DiskBytesAfterFlush        uint64      `json:"diskBytesAfterFlush"`
	DebtBytesBefore            uint64      `json:"debtBytesBefore"`
	DebtBytesAfterImmediate    uint64      `json:"debtBytesAfterImmediate"`
	DebtBytesAfterFlush        uint64      `json:"debtBytesAfterFlush"`
	WALBytesWrittenBefore      uint64      `json:"walBytesWrittenBefore"`
	WALBytesWrittenAfter       uint64      `json:"walBytesWrittenAfter"`
	PebbleCumulativeWriteAmp   float64     `json:"pebbleCumulativeWriteAmp"`
	UnfilteredBeforeCompaction perfLatency `json:"unfilteredBeforeCompaction"`
	UnfilteredAfterCompaction  perfLatency `json:"unfilteredAfterCompaction"`
}

type perfColdSummary struct {
	TieredRuns          int            `json:"tieredRuns"`
	TierPasses          int            `json:"tierPasses"`
	TierElapsedMS       float64        `json:"tierElapsedMs"`
	HotDiskBytes        uint64         `json:"equivalentHotDiskBytes"`
	PostTierDiskBytes   uint64         `json:"postTierDiskBytes"`
	ColdObjectBytes     uint64         `json:"coldObjectBytes"`
	FinalLogicalRuns    int            `json:"finalLogicalRuns"`
	FinalHotRuns        int            `json:"finalHotRuns"`
	FinalColdRuns       int            `json:"finalColdRuns"`
	ColdParts           int            `json:"coldParts"`
	ArchivedParts       int            `json:"archivedParts"`
	RunCountFormula     string         `json:"runCountFormula"`
	RunCountBound       int            `json:"runCountLimit"`
	RunCountBounded     bool           `json:"runCountBounded"`
	RunsPerLevelBounded bool           `json:"runsPerLevelBounded"`
	RunLevels           []perfRunLevel `json:"runLevels"`
	ColdMissObjectIO    perfObjectIO   `json:"coldMissObjectIo"`
	CacheHitObjectIO    perfObjectIO   `json:"cacheHitObjectIo"`
	ObjectLatencyMode   string         `json:"objectLatencyMode"`
	ColdMissSamples     int            `json:"coldMissSamplesPerShape"`
	CacheHitSamples     int            `json:"cacheHitSamplesPerShape"`
	Backend             string         `json:"backend"`
	CacheMode           string         `json:"cacheMode"`
	Lifecycle           string         `json:"lifecycle"`
}

type perfEvidenceReport struct {
	SchemaVersion    int                 `json:"schemaVersion"`
	GeneratedAt      string              `json:"generatedAt"`
	Profile          string              `json:"profile"`
	Complete         bool                `json:"complete"`
	SelectedPhases   []string            `json:"selectedPhases"`
	HarnessElapsedMS float64             `json:"harnessElapsedMs"`
	GitCommit        string              `json:"gitCommit"`
	GitTree          string              `json:"gitTree"`
	WorkingTree      string              `json:"workingTree"`
	Machine          string              `json:"machine"`
	GoVersion        string              `json:"goVersion"`
	GOOS             string              `json:"goos"`
	GOARCH           string              `json:"goarch"`
	Datasets         []perfDatasetResult `json:"datasets"`
	Measurements     []perfLatency       `json:"measurements"`
	Compaction       perfCompaction      `json:"compaction"`
	Cold             perfColdSummary     `json:"cold"`
	ReplicaDigest    perfReplicaDigest   `json:"simulatedReplicaDigest"`
	Assertions       []perfAssertion     `json:"assertions"`
	Pending          []string            `json:"pending"`
}

type seededHistory struct {
	store      *balancehistorystore.Store
	result     perfDatasetResult
	head       uint64
	accountIDs []string
}

// TestPITLocalPerformanceEvidence is deliberately opt-in and sequential:
// parallel execution would invalidate the latency and disk-contention samples.
// Set PIT_PERF=1 and PIT_PERF_OUTPUT to a path under build/ to capture the raw
// nanosecond samples and dataset counters as JSON.
func TestPITLocalPerformanceEvidence(t *testing.T) {
	if os.Getenv("PIT_PERF") != "1" {
		t.Skip("set PIT_PERF=1 to run the local PIT performance evidence harness")
	}

	harnessStarted := time.Now()
	phaseSelection, err := parsePerfPhaseSelection(os.Getenv("PIT_PERF_PHASES"))
	if err != nil {
		t.Fatal(err)
	}
	profile := selectedPerfProfile(t)
	report := perfEvidenceReport{
		SchemaVersion:  2,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Profile:        profile.Name,
		Complete:       phaseSelection.all,
		SelectedPhases: phaseSelection.Names(),
		GitCommit:      valueOrUnknown(os.Getenv("PIT_PERF_GIT_COMMIT")),
		GitTree:        valueOrUnknown(os.Getenv("PIT_PERF_GIT_TREE")),
		WorkingTree:    valueOrUnknown(os.Getenv("PIT_PERF_WORKTREE")),
		Machine:        valueOrUnknown(os.Getenv("PIT_PERF_MACHINE")),
		GoVersion:      runtime.Version(),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		Pending: []string{
			"production object-store network latency remains deployment-specific; the cold matrix uses the real composite view over a local-filesystem backend",
			"real multi-node lag and recovery convergence require a cluster harness; deterministic two-store digest convergence is measured locally",
			"end-to-end HTTP/gRPC and Raft admission latency require the deployed k6 suite",
		},
	}
	if !report.Complete {
		report.Pending = append(report.Pending, fmt.Sprintf(
			"partial performance artifact: only phases %s were executed; combine phase artifacts only when profile, dataset parameters, code provenance, and machine identity match",
			strings.Join(report.SelectedPhases, ","),
		))
	}

	mainConfig := perfDatasetConfig{
		Name:                   "two-year-hot-history",
		Days:                   profile.Days,
		Accounts:               profile.Accounts,
		AssetBuckets:           profile.AssetBuckets,
		Colors:                 profile.Colors,
		PostingsPerDay:         profile.PostingsPerDay,
		Runs:                   profile.Days,
		BackdatedRate:          1,
		CompactEachPublication: true,
	}
	needsMainDataset := phaseSelection.IncludesAny(
		perfPhaseHotUnfiltered,
		perfPhaseHotFiltered,
		perfPhaseHotGrouped,
		perfPhaseHotShapes,
		perfPhaseCompaction,
		perfPhaseColdUnfiltered,
		perfPhaseColdFiltered,
		perfPhaseColdGrouped,
	)
	var main *seededHistory
	if needsMainDataset {
		seeded := seedPerfHistory(t, mainConfig)
		main = &seeded
		defer func() {
			if err := main.store.Close(); err != nil {
				t.Errorf("closing main PIT performance store: %v", err)
			}
		}()
		report.Datasets = append(report.Datasets, main.result)
	}

	if phaseSelection.IncludesAny(
		perfPhaseHotUnfiltered,
		perfPhaseHotFiltered,
		perfPhaseHotGrouped,
		perfPhaseHotShapes,
	) {
		view, openErr := main.store.OpenView(uint64(main.result.Config.Days + 1))
		if openErr != nil {
			t.Fatalf("opening main PIT performance view: %v", openErr)
		}

		ages := []struct {
			name string
			days int
		}{
			{name: "1d", days: 1},
			{name: "6mo", days: min(180, profile.Days-1)},
			{name: "2y", days: min(730, profile.Days-1)},
		}
		filteredAccounts := main.accountIDs[:min(16, len(main.accountIDs))]
		coreShapes := []struct {
			phase    string
			name     string
			accounts []string
			opts     query.AggregateOptions
		}{
			{phase: perfPhaseHotFiltered, name: "filtered_16", accounts: filteredAccounts},
			{phase: perfPhaseHotGrouped, name: "grouped", opts: query.AggregateOptions{GroupByPrefixes: []string{"users:", "merchants:"}}},
		}
		for _, axis := range []struct {
			name  string
			value balancehistorystore.Axis
		}{
			{name: "effective", value: balancehistorystore.AxisEffective},
			{name: "insertion", value: balancehistorystore.AxisInsertion},
		} {
			for _, age := range ages {
				at := main.head - uint64(age.days)*perfDayMicros
				if phaseSelection.Includes(perfPhaseHotUnfiltered) {
					measurement := measurePerfHistoryLatency(
						t,
						main.store,
						"pit_hot_"+axis.name+"_age_"+age.name+"_unfiltered",
						"pit-read",
						profile.Samples,
						map[string]any{
							"axis": axis.name, "ageDays": age.days, "shape": "unfiltered",
							"accounts": profile.Accounts, "assetBuckets": profile.AssetBuckets, "colors": profile.Colors,
						},
						func() error {
							result, queryErr := query.AggregateHistoricalVolumes(view, perfLedgerID, axis.value, at, nil, query.AggregateOptions{})
							perfResultSink = result

							return queryErr
						},
					)
					report.Measurements = append(report.Measurements, measurement)
				}
				for _, shape := range coreShapes {
					if !phaseSelection.Includes(shape.phase) {
						continue
					}
					shapeMeasurement := measurePerfHistoryLatency(
						t,
						main.store,
						"pit_hot_"+axis.name+"_age_"+age.name+"_"+shape.name,
						"pit-read-matrix",
						profile.ShapeSamples,
						map[string]any{
							"axis": axis.name, "ageDays": age.days, "shape": shape.name,
							"accounts": profile.Accounts, "assetBuckets": profile.AssetBuckets, "colors": profile.Colors,
						},
						func() error {
							result, queryErr := query.AggregateHistoricalVolumes(
								view, perfLedgerID, axis.value, at, shape.accounts, shape.opts,
							)
							perfResultSink = result

							return queryErr
						},
					)
					report.Measurements = append(report.Measurements, shapeMeasurement)
				}
			}
		}

		if phaseSelection.Includes(perfPhaseHotShapes) {
			recentAt := main.head - perfDayMicros
			shapeCases := []struct {
				name     string
				accounts []string
				opts     query.AggregateOptions
			}{
				{name: "grouped_max_precision_collapsed_colors", opts: query.AggregateOptions{
					GroupByPrefixes: []string{"users:", "merchants:"}, UseMaxPrecision: true, CollapseColors: true,
				}},
				{name: "unfiltered_collapsed_colors", opts: query.AggregateOptions{CollapseColors: true}},
			}
			for _, shape := range shapeCases {
				measurement := measurePerfHistoryLatency(
					t,
					main.store,
					"pit_hot_effective_shape_"+shape.name,
					"pit-read-shape",
					profile.ShapeSamples,
					map[string]any{"axis": "effective", "ageDays": 1, "shape": shape.name},
					func() error {
						result, queryErr := query.AggregateHistoricalVolumes(
							view, perfLedgerID, balancehistorystore.AxisEffective, recentAt, shape.accounts, shape.opts,
						)
						perfResultSink = result

						return queryErr
					},
				)
				report.Measurements = append(report.Measurements, measurement)
			}
		}

		if err := view.Close(); err != nil {
			t.Fatalf("closing pre-compaction view: %v", err)
		}
	}

	if phaseSelection.Includes(perfPhaseCompaction) {
		report.Compaction = measurePerfCompaction(t, main.store, main.head-perfDayMicros, profile.ShapeSamples)
	}
	if phaseSelection.Includes(perfPhaseReplicaDigest) {
		report.ReplicaDigest = measurePerfReplicaDigest(t, profile)
	}
	if phaseSelection.IncludesAny(perfPhaseColdUnfiltered, perfPhaseColdFiltered, perfPhaseColdGrouped) {
		coldConfig := mainConfig
		coldConfig.Name = "two-year-cold-history"
		coldDataset, coldMeasurements, coldSummary := measurePerfColdMatrix(
			t,
			profile,
			coldConfig,
			main.result.DiskBytes,
			phaseSelection,
		)
		report.Datasets = append(report.Datasets, coldDataset)
		report.Measurements = append(report.Measurements, coldMeasurements...)
		report.Cold = coldSummary
	}

	if phaseSelection.Includes(perfPhaseCardinality) {
		for _, cardinality := range []struct {
			accounts int
			assets   int
			colors   int
		}{
			{accounts: 64, assets: 1, colors: 1},
			{accounts: 256, assets: 8, colors: 1},
			{accounts: 256, assets: 8, colors: 4},
		} {
			config := perfDatasetConfig{
				Name:           fmt.Sprintf("cardinality-a%d-s%d-c%d", cardinality.accounts, cardinality.assets, cardinality.colors),
				Days:           2,
				Accounts:       cardinality.accounts,
				AssetBuckets:   cardinality.assets,
				Colors:         cardinality.colors,
				PostingsPerDay: 1,
				Runs:           1,
			}
			seeded := seedPerfHistory(t, config)
			view, openErr := seeded.store.OpenView(uint64(config.Days + 1))
			if openErr != nil {
				t.Fatalf("opening cardinality view %s: %v", config.Name, openErr)
			}
			measurement := measurePerfHistoryLatency(
				t,
				seeded.store,
				"pit_hot_cardinality_"+config.Name,
				"pit-cardinality",
				profile.CardinalitySamples,
				map[string]any{"accounts": config.Accounts, "assetBuckets": config.AssetBuckets, "colors": config.Colors},
				func() error {
					result, queryErr := query.AggregateHistoricalVolumes(
						view, perfLedgerID, balancehistorystore.AxisEffective, seeded.head, nil, query.AggregateOptions{},
					)
					perfResultSink = result

					return queryErr
				},
			)
			report.Measurements = append(report.Measurements, measurement)
			report.Datasets = append(report.Datasets, seeded.result)
			if err := view.Close(); err != nil {
				t.Fatalf("closing cardinality view %s: %v", config.Name, err)
			}
			if err := seeded.store.Close(); err != nil {
				t.Fatalf("closing cardinality store %s: %v", config.Name, err)
			}
		}
	}

	if phaseSelection.Includes(perfPhaseBackdating) {
		for _, backdatedRate := range []int{0, 1, 50} {
			config := perfDatasetConfig{
				Name:           fmt.Sprintf("backdated-%d-percent", backdatedRate),
				Days:           min(180, profile.Days),
				Accounts:       min(64, profile.Accounts),
				AssetBuckets:   min(4, profile.AssetBuckets),
				Colors:         min(2, profile.Colors),
				PostingsPerDay: min(16, profile.PostingsPerDay),
				Runs:           min(6, profile.Runs),
				BackdatedRate:  backdatedRate,
			}
			seeded := seedPerfHistory(t, config)
			report.Datasets = append(report.Datasets, seeded.result)
			view, openErr := seeded.store.OpenView(uint64(config.Days + 1))
			if openErr != nil {
				t.Fatalf("opening backdating view %s: %v", config.Name, openErr)
			}
			for _, axis := range []struct {
				name  string
				value balancehistorystore.Axis
			}{
				{name: "effective", value: balancehistorystore.AxisEffective},
				{name: "insertion", value: balancehistorystore.AxisInsertion},
			} {
				measurement := measurePerfHistoryLatency(
					t,
					seeded.store,
					"pit_hot_"+axis.name+"_"+config.Name,
					"pit-backdating",
					profile.CardinalitySamples,
					map[string]any{"axis": axis.name, "backdatedPercent": backdatedRate},
					func() error {
						result, queryErr := query.AggregateHistoricalVolumes(
							view, perfLedgerID, axis.value, seeded.head-perfDayMicros, nil, query.AggregateOptions{},
						)
						perfResultSink = result

						return queryErr
					},
				)
				report.Measurements = append(report.Measurements, measurement)
			}
			if err := view.Close(); err != nil {
				t.Fatalf("closing backdating view %s: %v", config.Name, err)
			}
			if err := seeded.store.Close(); err != nil {
				t.Fatalf("closing backdating store %s: %v", config.Name, err)
			}
		}
	}

	if phaseSelection.Includes(perfPhaseWrite) {
		live, writeBaseline, writeSteadyState, writeBackfillSaturation := measureLiveAndWriteInterference(t, profile)
		report.Measurements = append(
			report.Measurements,
			live,
			writeBaseline,
			writeSteadyState,
			writeBackfillSaturation,
		)
	}

	if phaseSelection.Includes(perfPhaseHotUnfiltered) {
		oneDay := findPerfMeasurement(t, report.Measurements, "pit_hot_effective_age_1d_unfiltered")
		sixMonths := findPerfMeasurement(t, report.Measurements, "pit_hot_effective_age_6mo_unfiltered")
		ageRatio := sixMonths.P95 / oneDay.P95
		report.Assertions = append(report.Assertions, perfAssertion{
			Name: "hot_6mo_p95_vs_1d", Target: "<= 1.20x", Observed: ageRatio, Unit: "ratio", Satisfied: ageRatio <= 1.20,
			Scope: "warm local Pebble, equal cardinality, effective axis, unfiltered, daily Publish+Compact aging",
		})
	}
	if phaseSelection.Includes(perfPhaseReplicaDigest) {
		report.Assertions = append(report.Assertions, perfAssertion{
			Name:      "simulated_replica_digest_convergence",
			Target:    "logical and served semantic digests equal across different publication/compaction layouts",
			Observed:  1,
			Unit:      "boolean",
			Satisfied: report.ReplicaDigest.LogicalDigestEqual && report.ReplicaDigest.SemanticDigestEqual && report.ReplicaDigest.DifferentPhysicalShape,
			Scope:     "two independent local stores ingest identical effects with one large publication versus daily publications plus compaction; not a networked replica test",
		})
	}
	if phaseSelection.IncludesAny(perfPhaseColdUnfiltered, perfPhaseColdFiltered, perfPhaseColdGrouped) {
		report.Assertions = append(report.Assertions, perfAssertion{
			Name:      "cold_logical_run_count_after_aging",
			Target:    fmt.Sprintf("<= %d runs from %s", report.Cold.RunCountBound, report.Cold.RunCountFormula),
			Observed:  float64(report.Cold.FinalLogicalRuns),
			Unit:      "runs",
			Satisfied: report.Cold.RunCountBounded,
			Scope:     "same two-year cardinality; daily Publish+Compact+Tier; catches O(history) cold-run growth",
		})
		report.Assertions = append(report.Assertions, perfAssertion{
			Name:      "cold_runs_per_level_after_aging",
			Target:    fmt.Sprintf("< %d hot+cold runs at every level", balancehistorystore.DefaultRunCompactionThreshold),
			Observed:  float64(perfMaxRunsAtLevel(report.Cold.RunLevels)),
			Unit:      "runs",
			Satisfied: report.Cold.RunsPerLevelBounded,
			Scope:     "combined hot+cold logical runs; each level must remain below the compaction threshold",
		})
	}

	report.HarnessElapsedMS = float64(time.Since(harnessStarted)) / float64(time.Millisecond)
	writePerfEvidence(t, report)
	if os.Getenv("PIT_PERF_ENFORCE") == "1" {
		for _, assertion := range report.Assertions {
			if !assertion.Satisfied {
				t.Errorf("performance assertion %s failed: observed %.4f %s, target %s", assertion.Name, assertion.Observed, assertion.Unit, assertion.Target)
			}
		}
	}
}

func selectedPerfProfile(t *testing.T) perfProfile {
	t.Helper()

	profiles := map[string]perfProfile{
		"smoke": {
			Name: "smoke", Samples: 40, ShapeSamples: 10, Days: 190, Accounts: 32, AssetBuckets: 4, Colors: 2,
			PostingsPerDay: 8, Runs: 4, CardinalitySamples: 10, InterferenceSamples: 60, InterferenceBatchSize: 8,
		},
		"local": {
			Name: "local", Samples: 300, ShapeSamples: 60, Days: 731, Accounts: 64, AssetBuckets: 8, Colors: 4,
			PostingsPerDay: 32, Runs: 12, CardinalitySamples: 80, InterferenceSamples: 300, InterferenceBatchSize: 32,
		},
		"full": {
			Name: "full", Samples: 1_000, ShapeSamples: 200, Days: 731, Accounts: 512, AssetBuckets: 16, Colors: 8,
			PostingsPerDay: 256, Runs: 24, CardinalitySamples: 300, InterferenceSamples: 1_000, InterferenceBatchSize: 128,
		},
	}
	name := os.Getenv("PIT_PERF_PROFILE")
	if name == "" {
		name = "local"
	}
	profile, ok := profiles[name]
	if !ok {
		t.Fatalf("unknown PIT_PERF_PROFILE %q; expected smoke, local, or full", name)
	}
	if override := os.Getenv("PIT_PERF_SAMPLES"); override != "" {
		samples, err := strconv.Atoi(override)
		if err != nil || samples < 10 {
			t.Fatalf("invalid PIT_PERF_SAMPLES %q; expected integer >= 10", override)
		}
		profile.Samples = samples
		profile.ShapeSamples = max(10, samples/5)
		profile.CardinalitySamples = max(10, samples/4)
		profile.InterferenceSamples = samples
	}

	return profile
}

func perfSelectedColdObjectLatency(t *testing.T) time.Duration {
	t.Helper()

	value := os.Getenv("PIT_PERF_COLD_OBJECT_LATENCY")
	if value == "" {
		return 0
	}
	latency, err := time.ParseDuration(value)
	if err != nil || latency < 0 {
		t.Fatalf("invalid PIT_PERF_COLD_OBJECT_LATENCY %q; expected a non-negative Go duration", value)
	}

	return latency
}

type perfSeedLifecycle func(publication int, store *balancehistorystore.Store) (int, error)

func seedPerfHistory(t *testing.T, config perfDatasetConfig) seededHistory {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	if err != nil {
		t.Fatalf("opening PIT performance history store %s: %v", config.Name, err)
	}

	return seedPerfHistoryIntoStore(t, store, config, nil)
}

func seedPerfHistoryIntoStore(
	t *testing.T,
	store *balancehistorystore.Store,
	config perfDatasetConfig,
	lifecycle perfSeedLifecycle,
) seededHistory {
	t.Helper()

	accountIDs := perfAccountIDs(config.Accounts)
	baselinePostings := config.Accounts * config.AssetBuckets * config.Colors
	totalPostings := baselinePostings + config.Days*config.PostingsPerDay
	totalEffects := totalPostings * 2
	publicationSamples := make([]int64, 0, config.Runs)
	logicalCompactions := 0
	tierPasses := 0
	tieredRuns := 0
	lastAudit := uint64(0)
	lastLog := uint64(0)
	remainingDays := config.Days
	nextDay := 1
	agingStarted := time.Now()
	for run := range config.Runs {
		runsLeft := config.Runs - run
		daysInRun := int(math.Ceil(float64(remainingDays) / float64(runsLeft)))
		effects := make([]historydomain.Effect, 0, daysInRun*config.PostingsPerDay*2)
		if run == 0 {
			lastAudit++
			lastLog++
			effects = append(effects, perfBaselineEffects(config, lastAudit, lastLog)...)
		}
		for day := nextDay; day < nextDay+daysInRun; day++ {
			lastAudit++
			lastLog++
			effects = append(effects, perfDailyEffects(config, day, lastAudit, lastLog)...)
		}
		nextDay += daysInRun
		remainingDays -= daysInRun

		started := time.Now()
		_, err := store.Publish(balancehistorystore.Publication{
			Effects: effects,
			Coverage: balancehistorystore.Coverage{
				AuditSequence: lastAudit, LogSequence: lastLog, SourceComplete: true,
			},
		})
		publicationSamples = append(publicationSamples, time.Since(started).Nanoseconds())
		if err != nil {
			_ = store.Close()
			t.Fatalf("publishing PIT performance run %d for %s: %v", run, config.Name, err)
		}
		if config.CompactEachPublication {
			compacted, compactErr := store.Compact(balancehistorystore.DefaultRunCompactionThreshold)
			if compactErr != nil {
				_ = store.Close()
				t.Fatalf("compacting PIT performance publication %d for %s: %v", run, config.Name, compactErr)
			}
			if compacted {
				logicalCompactions++
			}
		}
		if lifecycle != nil {
			tierPasses++
			changed, lifecycleErr := lifecycle(run, store)
			if lifecycleErr != nil {
				_ = store.Close()
				t.Fatalf("running PIT performance lifecycle after publication %d for %s: %v", run, config.Name, lifecycleErr)
			}
			tieredRuns += changed
		}
	}
	agingElapsed := time.Since(agingStarted)
	if err := store.DB().Flush(); err != nil {
		_ = store.Close()
		t.Fatalf("flushing PIT performance dataset %s for capacity measurement: %v", config.Name, err)
	}

	manifest, err := store.Manifest()
	if err != nil {
		_ = store.Close()
		t.Fatalf("reading PIT performance manifest for %s: %v", config.Name, err)
	}
	hotRuns := 0
	coldRuns := 0
	for _, run := range manifest.Runs {
		if run.LocalRemoved {
			coldRuns++
		} else {
			hotRuns++
		}
	}
	runCountLimit := perfLogarithmicRunCountLimit(config.Runs, balancehistorystore.DefaultRunCompactionThreshold)
	runLevels, runsPerLevelBounded := perfSummarizeRunLevels(
		manifest,
		balancehistorystore.DefaultRunCompactionThreshold,
	)
	metrics := store.DB().Metrics()
	publication := summarizePerfDurations("publish_"+config.Name, "history-publication", publicationSamples, map[string]any{
		"publications": config.Runs, "effects": totalEffects, "walSyncPerPublication": false,
		"compactEachPublication": config.CompactEachPublication, "tierEachPublication": lifecycle != nil,
	})
	totalPublicationNS := int64(0)
	for _, duration := range publicationSamples {
		totalPublicationNS += duration
	}
	diskBytes := metrics.DiskSpaceUsage()
	result := perfDatasetResult{
		Config:               config,
		Postings:             totalPostings,
		Effects:              totalEffects,
		PublishedRuns:        config.Runs,
		LogicalCompactions:   logicalCompactions,
		TierPasses:           tierPasses,
		TieredRuns:           tieredRuns,
		FinalLogicalRuns:     len(manifest.Runs),
		FinalHotRuns:         hotRuns,
		FinalColdRuns:        coldRuns,
		RunCountFormula:      perfRunCountFormula,
		RunCountLimit:        runCountLimit,
		RunCountBounded:      len(manifest.Runs) <= runCountLimit,
		RunsPerLevelBounded:  runsPerLevelBounded,
		RunLevels:            runLevels,
		AgingElapsedMS:       float64(agingElapsed) / float64(time.Millisecond),
		Publication:          publication,
		PublicationEffectsS:  float64(totalEffects) / (float64(totalPublicationNS) / float64(time.Second)),
		DiskBytes:            diskBytes,
		PebbleCompactionDebt: metrics.Compact.EstimatedDebt,
		WALBytesWritten:      metrics.WAL.BytesWritten,
		BytesPerPosting:      float64(diskBytes) / float64(totalPostings),
		BytesPerEffect:       float64(diskBytes) / float64(totalEffects),
	}

	return seededHistory{
		store:      store,
		result:     result,
		head:       uint64(config.Days) * perfDayMicros,
		accountIDs: accountIDs,
	}
}

func perfLogarithmicRunCountLimit(publications, threshold int) int {
	if publications <= 0 || threshold <= 1 {
		return 0
	}
	levels := 0
	coveredPublications := 1
	for coveredPublications < publications {
		coveredPublications *= threshold
		levels++
	}

	return (threshold - 1) * (1 + levels)
}

func perfSummarizeRunLevels(
	manifest balancehistorystore.Manifest,
	threshold int,
) ([]perfRunLevel, bool) {
	byLevel := make(map[uint32]*perfRunLevel)
	levels := make([]uint32, 0)
	for _, run := range manifest.Runs {
		level := byLevel[run.Level]
		if level == nil {
			level = &perfRunLevel{Level: run.Level, Limit: threshold}
			byLevel[run.Level] = level
			levels = append(levels, run.Level)
		}
		if run.LocalRemoved {
			level.ColdRuns++
		} else {
			level.HotRuns++
		}
		level.TotalRuns++
	}
	slices.Sort(levels)
	summary := make([]perfRunLevel, 0, len(levels))
	bounded := true
	for _, levelNumber := range levels {
		level := *byLevel[levelNumber]
		level.Bounded = level.TotalRuns < threshold
		bounded = bounded && level.Bounded
		summary = append(summary, level)
	}

	return summary, bounded
}

func perfMaxRunsAtLevel(levels []perfRunLevel) int {
	maximum := 0
	for _, level := range levels {
		maximum = max(maximum, level.TotalRuns)
	}

	return maximum
}

func measurePerfColdMatrix(
	t *testing.T,
	profile perfProfile,
	config perfDatasetConfig,
	equivalentHotDiskBytes uint64,
	phaseSelection perfPhaseSelection,
) (perfDatasetResult, []perfLatency, perfColdSummary) {
	t.Helper()

	archiveRoot := t.TempDir()
	simulatedObjectLatency := perfSelectedColdObjectLatency(t)
	coldBackend := newPerfObservedColdStorage(
		coldstorage.NewFilesystemStorage(filepath.Join(archiveRoot, "objects")),
		simulatedObjectLatency,
	)
	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	if err != nil {
		t.Fatalf("opening PIT performance cold history store: %v", err)
	}
	tierArchive, err := balancehistoryarchive.New(
		coldBackend,
		balancehistoryarchive.Config{
			BaseBucketID:  "pit-performance",
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(archiveRoot, "tier-cache"),
			CacheMaxBytes: 512 << 20,
		},
		noop.NewMeterProvider().Meter("pit-performance-tier-archive"),
	)
	if err != nil {
		_ = store.Close()
		t.Fatalf("opening PIT tiering archive: %v", err)
	}
	const (
		minimumTierLevel = uint32(1)
		retainLocalRuns  = 8
		maxRunsPerPass   = 4
	)
	if err := store.ConfigureTiering(balancehistorystore.TieringConfig{
		Archive:         tierArchive,
		MinimumLevel:    minimumTierLevel,
		RetainLocalRuns: retainLocalRuns,
		MaxRunsPerPass:  maxRunsPerPass,
	}); err != nil {
		_ = tierArchive.Close()
		_ = store.Close()
		t.Fatalf("configuring PIT cold tier: %v", err)
	}
	tierElapsed := time.Duration(0)
	seeded := seedPerfHistoryIntoStore(t, store, config, func(_ int, lifecycleStore *balancehistorystore.Store) (int, error) {
		tierStarted := time.Now()
		changed, tierErr := lifecycleStore.Tier(context.Background())
		tierElapsed += time.Since(tierStarted)

		return changed, tierErr
	})
	if err := seeded.store.SyncWAL(); err != nil {
		_ = tierArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("syncing PIT tiered manifest: %v", err)
	}
	if err := seeded.store.DB().Flush(); err != nil {
		_ = tierArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("flushing PIT tiered manifest: %v", err)
	}
	manifest, err := seeded.store.Manifest()
	if err != nil {
		_ = tierArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("reading PIT tiered manifest: %v", err)
	}
	coldParts := 0
	archivedParts := 0
	for _, run := range manifest.Runs {
		if run.LocalRemoved && !run.Archived {
			_ = tierArchive.Close()
			_ = seeded.store.Close()
			t.Fatalf("cold-only run %d lacks an archive", run.ID)
		}
		if run.Archived {
			archivedParts += len(run.ArchiveParts)
		}
		if run.LocalRemoved {
			coldParts += len(run.ArchiveParts)
		}
	}

	missArchive, err := balancehistoryarchive.New(
		coldBackend,
		balancehistoryarchive.Config{
			BaseBucketID:  "pit-performance",
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(archiveRoot, "miss-cache"),
			CacheMaxBytes: 1,
		},
		noop.NewMeterProvider().Meter("pit-performance-cold-miss"),
	)
	if err != nil {
		_ = tierArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("opening PIT cold-miss cache: %v", err)
	}
	if err := seeded.store.ConfigureTiering(balancehistorystore.TieringConfig{Archive: missArchive}); err != nil {
		_ = missArchive.Close()
		_ = tierArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("configuring PIT cold-miss archive: %v", err)
	}
	if err := tierArchive.Close(); err != nil {
		_ = missArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("closing PIT tier-upload archive: %v", err)
	}

	ages := []struct {
		name string
		days int
	}{
		{name: "1d", days: 1},
		{name: "6mo", days: min(180, profile.Days-1)},
		{name: "2y", days: min(730, profile.Days-1)},
	}
	accounts := seeded.accountIDs[:min(16, len(seeded.accountIDs))]
	shapes := []struct {
		phase    string
		name     string
		accounts []string
		opts     query.AggregateOptions
	}{
		{phase: perfPhaseColdUnfiltered, name: "unfiltered"},
		{phase: perfPhaseColdFiltered, name: "filtered_16", accounts: accounts},
		{phase: perfPhaseColdGrouped, name: "grouped", opts: query.AggregateOptions{GroupByPrefixes: []string{"users:", "merchants:"}}},
	}
	missSamples := 20
	switch profile.Name {
	case "smoke":
		missSamples = 5
	case "full":
		missSamples = 50
	}
	measurements := make([]perfLatency, 0, 36)
	for _, axis := range []struct {
		name  string
		value balancehistorystore.Axis
	}{
		{name: "effective", value: balancehistorystore.AxisEffective},
		{name: "insertion", value: balancehistorystore.AxisInsertion},
	} {
		for _, age := range ages {
			at := seeded.head - uint64(age.days)*perfDayMicros
			for _, shape := range shapes {
				if !phaseSelection.Includes(shape.phase) {
					continue
				}
				operation := func() error {
					view, openErr := seeded.store.OpenViewContext(context.Background(), manifest.LogWatermark)
					if openErr != nil {
						return openErr
					}
					result, queryErr := query.AggregateHistoricalVolumes(
						view, perfLedgerID, axis.value, at, shape.accounts, shape.opts,
					)
					perfResultSink = result

					return errors.Join(queryErr, view.Close())
				}
				if err := operation(); err != nil {
					t.Fatalf("warming PIT cold-miss measurement for %s/%s/%s: %v", axis.name, age.name, shape.name, err)
				}
				objectBefore := coldBackend.snapshot()
				measurement := measurePerfLatencyWithWarmup(
					t,
					"pit_cold_miss_"+axis.name+"_age_"+age.name+"_"+shape.name,
					"pit-read-cold-miss",
					missSamples,
					0,
					map[string]any{
						"axis":             axis.name,
						"ageDays":          age.days,
						"shape":            shape.name,
						"includesViewOpen": true,
						"backend":          "local-filesystem",
					},
					operation,
				)
				measurement.ObjectIO = diffPerfObjectIO(
					objectBefore,
					coldBackend.snapshot(),
					measurement.Samples,
					simulatedObjectLatency,
				)
				measurements = append(measurements, measurement)
			}
		}
	}

	hitArchive, err := balancehistoryarchive.New(
		coldBackend,
		balancehistoryarchive.Config{
			BaseBucketID:  "pit-performance",
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(archiveRoot, "hit-cache"),
			CacheMaxBytes: 512 << 20,
		},
		noop.NewMeterProvider().Meter("pit-performance-cold-hit"),
	)
	if err != nil {
		_ = missArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("opening PIT cold-hit cache: %v", err)
	}
	if err := seeded.store.ConfigureTiering(balancehistorystore.TieringConfig{Archive: hitArchive}); err != nil {
		_ = hitArchive.Close()
		_ = missArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("configuring PIT cold-hit archive: %v", err)
	}
	if err := missArchive.Close(); err != nil {
		_ = hitArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("closing PIT cold-miss archive: %v", err)
	}
	warmHitView, err := seeded.store.OpenViewContext(context.Background(), manifest.LogWatermark)
	if err != nil {
		_ = hitArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("opening PIT cold-cache warm-up view: %v", err)
	}
	result, queryErr := query.AggregateHistoricalVolumes(
		warmHitView,
		perfLedgerID,
		balancehistorystore.AxisEffective,
		seeded.head-perfDayMicros,
		nil,
		query.AggregateOptions{},
	)
	perfResultSink = result
	if err := errors.Join(queryErr, warmHitView.Close()); err != nil {
		_ = hitArchive.Close()
		_ = seeded.store.Close()
		t.Fatalf("warming PIT cold-cache-hit archive: %v", err)
	}
	for _, axis := range []struct {
		name  string
		value balancehistorystore.Axis
	}{
		{name: "effective", value: balancehistorystore.AxisEffective},
		{name: "insertion", value: balancehistorystore.AxisInsertion},
	} {
		for _, age := range ages {
			at := seeded.head - uint64(age.days)*perfDayMicros
			for _, shape := range shapes {
				if !phaseSelection.Includes(shape.phase) {
					continue
				}
				objectBefore := coldBackend.snapshot()
				measurement := measurePerfLatency(
					t,
					"pit_cold_cache_hit_"+axis.name+"_age_"+age.name+"_"+shape.name,
					"pit-read-cold-cache-hit",
					profile.ShapeSamples,
					map[string]any{
						"axis":             axis.name,
						"ageDays":          age.days,
						"shape":            shape.name,
						"includesViewOpen": true,
						"prewarmedCache":   true,
						"backend":          "local-filesystem",
					},
					func() error {
						view, openErr := seeded.store.OpenViewContext(context.Background(), manifest.LogWatermark)
						if openErr != nil {
							return openErr
						}
						result, queryErr := query.AggregateHistoricalVolumes(
							view, perfLedgerID, axis.value, at, shape.accounts, shape.opts,
						)
						perfResultSink = result

						return errors.Join(queryErr, view.Close())
					},
				)
				measurement.ObjectIO = diffPerfObjectIO(
					objectBefore,
					coldBackend.snapshot(),
					measurement.Samples,
					simulatedObjectLatency,
				)
				measurements = append(measurements, measurement)
			}
		}
	}
	if err := hitArchive.Close(); err != nil {
		_ = seeded.store.Close()
		t.Fatalf("closing PIT cold-hit archive: %v", err)
	}
	postTierDiskBytes := seeded.store.DB().Metrics().DiskSpaceUsage()
	coldObjectBytes := perfDirectoryBytes(t, filepath.Join(archiveRoot, "objects"))
	objectLatencyMode := "observed local-filesystem fetch; no injected latency"
	if simulatedObjectLatency > 0 {
		objectLatencyMode = "injected per-Fetch delay plus observed local-filesystem transfer"
	}
	if err := seeded.store.Close(); err != nil {
		t.Fatalf("closing PIT cold history store: %v", err)
	}

	return seeded.result, measurements, perfColdSummary{
		TieredRuns:          seeded.result.TieredRuns,
		TierPasses:          seeded.result.TierPasses,
		TierElapsedMS:       float64(tierElapsed) / float64(time.Millisecond),
		HotDiskBytes:        equivalentHotDiskBytes,
		PostTierDiskBytes:   postTierDiskBytes,
		ColdObjectBytes:     coldObjectBytes,
		FinalLogicalRuns:    seeded.result.FinalLogicalRuns,
		FinalHotRuns:        seeded.result.FinalHotRuns,
		FinalColdRuns:       seeded.result.FinalColdRuns,
		ColdParts:           coldParts,
		ArchivedParts:       archivedParts,
		RunCountFormula:     seeded.result.RunCountFormula,
		RunCountBound:       seeded.result.RunCountLimit,
		RunCountBounded:     seeded.result.RunCountBounded,
		RunsPerLevelBounded: seeded.result.RunsPerLevelBounded,
		RunLevels:           seeded.result.RunLevels,
		ColdMissObjectIO:    mergePerfObjectIO(measurements, "pit-read-cold-miss", simulatedObjectLatency),
		CacheHitObjectIO:    mergePerfObjectIO(measurements, "pit-read-cold-cache-hit", simulatedObjectLatency),
		ObjectLatencyMode:   objectLatencyMode,
		ColdMissSamples:     missSamples,
		CacheHitSamples:     profile.ShapeSamples,
		Backend:             "local-filesystem",
		CacheMode:           "cold miss uses a 1-byte cache; cache hit prewarms the verified indexed archive cache and includes OpenView+query+Close per sample",
		Lifecycle:           "one synthetic day per L0 publication; Publish then one Compact then one Tier pass; threshold=4, minimumTierLevel=1, retainLocalRuns=8, maxRunsPerPass=4; cold Fetch is sequential by intersecting run/part",
	}
}

func measurePerfReplicaDigest(t *testing.T, profile perfProfile) perfReplicaDigest {
	t.Helper()

	started := time.Now()
	days := min(90, profile.Days)
	base := perfDatasetConfig{
		Days:           days,
		Accounts:       min(32, profile.Accounts),
		AssetBuckets:   min(4, profile.AssetBuckets),
		Colors:         min(2, profile.Colors),
		PostingsPerDay: min(16, profile.PostingsPerDay),
		BackdatedRate:  10,
	}
	replicaAConfig := base
	replicaAConfig.Name = "replica-digest-single-publication"
	replicaAConfig.Runs = 1
	replicaBConfig := base
	replicaBConfig.Name = "replica-digest-daily-compaction"
	replicaBConfig.Runs = days
	replicaBConfig.CompactEachPublication = true

	replicaA := seedPerfHistory(t, replicaAConfig)
	replicaB := seedPerfHistory(t, replicaBConfig)
	defer func() {
		if err := replicaA.store.Close(); err != nil {
			t.Errorf("closing PIT digest replica A: %v", err)
		}
		if err := replicaB.store.Close(); err != nil {
			t.Errorf("closing PIT digest replica B: %v", err)
		}
	}()
	manifestA, err := replicaA.store.Manifest()
	if err != nil {
		t.Fatalf("reading PIT digest replica A manifest: %v", err)
	}
	manifestB, err := replicaB.store.Manifest()
	if err != nil {
		t.Fatalf("reading PIT digest replica B manifest: %v", err)
	}
	viewA, err := replicaA.store.OpenView(manifestA.LogWatermark)
	if err != nil {
		t.Fatalf("opening PIT digest replica A view: %v", err)
	}
	defer func() {
		if err := viewA.Close(); err != nil {
			t.Errorf("closing PIT digest replica A view: %v", err)
		}
	}()
	viewB, err := replicaB.store.OpenView(manifestB.LogWatermark)
	if err != nil {
		t.Fatalf("opening PIT digest replica B view: %v", err)
	}
	defer func() {
		if err := viewB.Close(); err != nil {
			t.Errorf("closing PIT digest replica B view: %v", err)
		}
	}()
	semanticA, err := viewA.SemanticDigest(context.Background())
	if err != nil {
		t.Fatalf("computing PIT digest replica A semantic digest: %v", err)
	}
	semanticB, err := viewB.SemanticDigest(context.Background())
	if err != nil {
		t.Fatalf("computing PIT digest replica B semantic digest: %v", err)
	}

	return perfReplicaDigest{
		Days:                   days,
		ReplicaAPublications:   replicaAConfig.Runs,
		ReplicaBPublications:   replicaBConfig.Runs,
		ReplicaARuns:           len(manifestA.Runs),
		ReplicaBRuns:           len(manifestB.Runs),
		LogicalDigestA:         fmt.Sprintf("%x", manifestA.LogicalDigest),
		LogicalDigestB:         fmt.Sprintf("%x", manifestB.LogicalDigest),
		SemanticDigestA:        hex.EncodeToString(semanticA[:]),
		SemanticDigestB:        hex.EncodeToString(semanticB[:]),
		LogicalDigestEqual:     manifestA.LogicalDigest == manifestB.LogicalDigest,
		SemanticDigestEqual:    semanticA == semanticB,
		DifferentPhysicalShape: replicaAConfig.Runs != replicaBConfig.Runs || len(manifestA.Runs) != len(manifestB.Runs),
		ElapsedMS:              float64(time.Since(started)) / float64(time.Millisecond),
	}
}

func perfDirectoryBytes(t *testing.T, path string) uint64 {
	t.Helper()

	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("reading PIT performance directory %s: %v", path, err)
	}
	bytes := uint64(0)
	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			bytes += perfDirectoryBytes(t, entryPath)

			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("stating PIT performance object %s: %v", entryPath, err)
		}
		bytes += uint64(info.Size())
	}

	return bytes
}

func perfBaselineEffects(config perfDatasetConfig, auditSequence, logSequence uint64) []historydomain.Effect {
	effects := make([]historydomain.Effect, 0, config.Accounts*config.AssetBuckets*config.Colors*2)
	order := uint32(0)
	for account := range config.Accounts {
		for asset := range config.AssetBuckets {
			for color := range config.Colors {
				effects = append(effects, perfPostingEffects(
					config, account, asset, color, 1, perfDayMicros, perfDayMicros, auditSequence, logSequence, order,
				)...)
				order += 2
			}
		}
	}

	return effects
}

func perfDailyEffects(config perfDatasetConfig, day int, auditSequence, logSequence uint64) []historydomain.Effect {
	effects := make([]historydomain.Effect, 0, config.PostingsPerDay*2)
	insertion := uint64(day) * perfDayMicros
	for posting := range config.PostingsPerDay {
		mix := uint64(day*1_000_003 + posting*97_409)
		account := int(mix % uint64(config.Accounts))
		asset := int((mix / 7) % uint64(config.AssetBuckets))
		color := int((mix / 17) % uint64(config.Colors))
		effective := insertion
		if int((mix/31)%100) < config.BackdatedRate {
			backdatedDays := 30 + (mix/101)%180
			if insertion > (backdatedDays+1)*perfDayMicros {
				effective -= backdatedDays * perfDayMicros
			} else {
				effective = perfDayMicros
			}
		}
		effects = append(effects, perfPostingEffects(
			config, account, asset, color, uint64(posting+day+1), effective, insertion,
			auditSequence, logSequence, uint32(posting*2),
		)...)
	}

	return effects
}

func perfPostingEffects(
	config perfDatasetConfig,
	accountIndex, assetIndex, colorIndex int,
	amount, effective, insertion, auditSequence, logSequence uint64,
	order uint32,
) []historydomain.Effect {
	base, precision := perfAsset(assetIndex)
	color := perfColor(colorIndex)
	common := historydomain.Effect{
		LedgerID: perfLedgerID, AuditSequence: auditSequence, LogSequence: logSequence,
		EffectiveAt: effective, InsertedAt: insertion, AssetBase: base, AssetPrecision: precision, Color: color,
	}
	input := common
	input.OrderIndex = order
	input.Account = perfAccount(accountIndex)
	input.Input = historydomain.AmountFromUint64(amount)
	output := common
	output.OrderIndex = order + 1
	output.Account = "world"
	output.Output = historydomain.AmountFromUint64(amount)

	return []historydomain.Effect{input, output}
}

func perfAccountIDs(count int) []string {
	accounts := make([]string, count)
	for index := range count {
		accounts[index] = perfAccount(index)
	}

	return accounts
}

func perfAccount(index int) string {
	if index%2 == 0 {
		return fmt.Sprintf("users:%06d", index)
	}

	return fmt.Sprintf("merchants:%06d", index)
}

func perfAsset(index int) (string, uint8) {
	return fmt.Sprintf("ASSET%02d", index/2), uint8(2 + index%2)
}

func perfColor(index int) string {
	if index == 0 {
		return ""
	}

	return fmt.Sprintf("color-%02d", index)
}

func measurePerfLatency(
	t *testing.T,
	name, category string,
	samples int,
	parameters map[string]any,
	operation func() error,
) perfLatency {
	return measurePerfLatencyWithWarmup(t, name, category, samples, min(10, samples), parameters, operation)
}

func measurePerfLatencyWithWarmup(
	t *testing.T,
	name, category string,
	samples, warmups int,
	parameters map[string]any,
	operation func() error,
) perfLatency {
	t.Helper()

	for range warmups {
		if err := operation(); err != nil {
			t.Fatalf("warming performance scenario %s: %v", name, err)
		}
	}
	runtimeBefore := capturePerfRuntime()
	durations := make([]int64, samples)
	for index := range samples {
		started := time.Now()
		err := operation()
		durations[index] = time.Since(started).Nanoseconds()
		if err != nil {
			t.Fatalf("running performance scenario %s sample %d: %v", name, index, err)
		}
	}
	runtimeAfter := capturePerfRuntime()

	measurement := summarizePerfDurations(name, category, durations, parameters)
	attachPerfRuntime(&measurement, runtimeBefore, runtimeAfter)

	return measurement
}

func summarizePerfDurations(name, category string, samples []int64, parameters map[string]any) perfLatency {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	total := int64(0)
	for _, sample := range samples {
		total += sample
	}

	elapsedSeconds := float64(total) / float64(time.Second)

	return perfLatency{
		Name: name, Category: category, Unit: "microseconds", Parameters: parameters, Samples: len(samples),
		Min:                 durationMicros(ordered[0]),
		P50:                 durationMicros(perfPercentile(ordered, 0.50)),
		P95:                 durationMicros(perfPercentile(ordered, 0.95)),
		P99:                 durationMicros(perfPercentile(ordered, 0.99)),
		Max:                 durationMicros(ordered[len(ordered)-1]),
		Mean:                durationMicros(total / int64(len(samples))),
		OperationsPerSecond: float64(len(samples)) / elapsedSeconds,
		SamplesNS:           slices.Clone(samples),
	}
}

func capturePerfRuntime() perfRuntimeSnapshot {
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

	return perfRuntimeSnapshot{
		AllocatedBytes: memory.TotalAlloc,
		Allocations:    memory.Mallocs,
		GoCPUSeconds:   cpuSeconds,
	}
}

func attachPerfRuntime(measurement *perfLatency, before, after perfRuntimeSnapshot) {
	if measurement == nil || measurement.Samples == 0 {
		return
	}
	operations := float64(measurement.Samples)
	measurement.AllocatedBytesPerOp = float64(after.AllocatedBytes-before.AllocatedBytes) / operations
	measurement.AllocationsPerOp = float64(after.Allocations-before.Allocations) / operations
	measurement.GoCPUSeconds = after.GoCPUSeconds - before.GoCPUSeconds
	measurement.GoCPUMicrosecondsPer = measurement.GoCPUSeconds * float64(time.Second/time.Microsecond) / operations
}

func measurePerfHistoryLatency(
	t *testing.T,
	store *balancehistorystore.Store,
	name, category string,
	samples int,
	parameters map[string]any,
	operation func() error,
) perfLatency {
	t.Helper()

	before := store.DB().Metrics()
	measurement := measurePerfLatency(t, name, category, samples, parameters, operation)
	after := store.DB().Metrics()
	measurement.IO = &perfReadIO{
		BlockCacheHits:   after.BlockCache.Hits - before.BlockCache.Hits,
		BlockCacheMisses: after.BlockCache.Misses - before.BlockCache.Misses,
		WALBytesIn:       after.WAL.BytesIn - before.WAL.BytesIn,
		WALBytesWritten:  after.WAL.BytesWritten - before.WAL.BytesWritten,
		Flushes:          after.Flush.Count - before.Flush.Count,
		Compactions:      after.Compact.Count - before.Compact.Count,
		DiskBytesBefore:  before.DiskSpaceUsage(),
		DiskBytesAfter:   after.DiskSpaceUsage(),
	}

	return measurement
}

func perfPercentile(ordered []int64, quantile float64) int64 {
	index := int(math.Ceil(quantile*float64(len(ordered)))) - 1
	index = max(0, min(index, len(ordered)-1))

	return ordered[index]
}

func durationMicros(nanoseconds int64) float64 {
	return float64(nanoseconds) / float64(time.Microsecond)
}

func measurePerfCompaction(
	t *testing.T,
	store *balancehistorystore.Store,
	at uint64,
	samples int,
) perfCompaction {
	t.Helper()

	beforeManifest, err := store.Manifest()
	if err != nil {
		t.Fatalf("reading pre-compaction manifest: %v", err)
	}
	beforeMetrics := store.DB().Metrics()
	beforeView, err := store.OpenView(beforeManifest.LogWatermark)
	if err != nil {
		t.Fatalf("opening pre-compaction measurement view: %v", err)
	}
	beforeRead := measurePerfHistoryLatency(t, store, "pit_hot_unfiltered_before_compaction", "pit-compaction", samples, nil, func() error {
		result, queryErr := query.AggregateHistoricalVolumes(
			beforeView, perfLedgerID, balancehistorystore.AxisEffective, at, nil, query.AggregateOptions{},
		)
		perfResultSink = result

		return queryErr
	})
	if err := beforeView.Close(); err != nil {
		t.Fatalf("closing pre-compaction measurement view: %v", err)
	}

	started := time.Now()
	operations := 0
	for {
		compacted, compactErr := store.Compact(balancehistorystore.DefaultRunCompactionThreshold)
		if compactErr != nil {
			t.Fatalf("compacting PIT performance store: %v", compactErr)
		}
		if !compacted {
			break
		}
		operations++
	}
	elapsed := time.Since(started)
	afterManifest, err := store.Manifest()
	if err != nil {
		t.Fatalf("reading post-compaction manifest: %v", err)
	}
	afterMetrics := store.DB().Metrics()
	afterView, err := store.OpenView(afterManifest.LogWatermark)
	if err != nil {
		t.Fatalf("opening post-compaction measurement view: %v", err)
	}
	afterRead := measurePerfHistoryLatency(t, store, "pit_hot_unfiltered_after_compaction", "pit-compaction", samples, nil, func() error {
		result, queryErr := query.AggregateHistoricalVolumes(
			afterView, perfLedgerID, balancehistorystore.AxisEffective, at, nil, query.AggregateOptions{},
		)
		perfResultSink = result

		return queryErr
	})
	if err := afterView.Close(); err != nil {
		t.Fatalf("closing post-compaction measurement view: %v", err)
	}
	if err := store.DB().Flush(); err != nil {
		t.Fatalf("flushing logical compaction output: %v", err)
	}
	afterFlushMetrics := store.DB().Metrics()
	totalMetrics := afterFlushMetrics.Total()

	return perfCompaction{
		Operations: operations, ElapsedMS: float64(elapsed) / float64(time.Millisecond),
		RunsBefore: len(beforeManifest.Runs), RunsAfter: len(afterManifest.Runs),
		DiskBytesBefore: beforeMetrics.DiskSpaceUsage(), DiskBytesAfterImmediate: afterMetrics.DiskSpaceUsage(),
		DiskBytesAfterFlush: afterFlushMetrics.DiskSpaceUsage(),
		DebtBytesBefore:     beforeMetrics.Compact.EstimatedDebt, DebtBytesAfterImmediate: afterMetrics.Compact.EstimatedDebt,
		DebtBytesAfterFlush:   afterFlushMetrics.Compact.EstimatedDebt,
		WALBytesWrittenBefore: beforeMetrics.WAL.BytesWritten, WALBytesWrittenAfter: afterMetrics.WAL.BytesWritten,
		PebbleCumulativeWriteAmp:   totalMetrics.WriteAmp(),
		UnfilteredBeforeCompaction: beforeRead, UnfilteredAfterCompaction: afterRead,
	}
}

func measureLiveAndWriteInterference(t *testing.T, profile perfProfile) (perfLatency, perfLatency, perfLatency, perfLatency) {
	t.Helper()

	root := t.TempDir()
	meter := noop.NewMeterProvider().Meter("pit-performance-evidence")
	primary, err := dal.NewStore(filepath.Join(root, "primary"), logging.NopZap(), meter, dal.DefaultConfig())
	if err != nil {
		t.Fatalf("opening primary performance store: %v", err)
	}
	defer func() {
		if err := primary.Close(); err != nil {
			t.Errorf("closing primary performance store: %v", err)
		}
	}()
	attrs := attributes.New()
	seedLiveVolumes(t, primary, attrs, profile.Accounts, profile.AssetBuckets, profile.Colors)
	if err := primary.Flush(); err != nil {
		t.Fatalf("flushing live performance baseline: %v", err)
	}
	handle, err := primary.NewReadHandle()
	if err != nil {
		t.Fatalf("opening live performance read handle: %v", err)
	}
	liveIOBefore := capturePrimaryPerfIO(t, primary)
	live := measurePerfLatency(t, "live_aggregate_volumes_unfiltered", "live-read", profile.Samples, map[string]any{
		"accounts": profile.Accounts, "assetBuckets": profile.AssetBuckets, "colors": profile.Colors,
	}, func() error {
		result, queryErr := query.AggregateAllVolumes(handle, attrs.Volume, perfLedger, query.AggregateOptions{})
		perfResultSink = result

		return queryErr
	})
	live.IO = diffPrimaryPerfIO(liveIOBefore, capturePrimaryPerfIO(t, primary))
	if err := handle.Close(); err != nil {
		t.Fatalf("closing live performance read handle: %v", err)
	}

	baseline := measurePrimaryDurableWrites(t, primary, attrs, 0, profile.InterferenceSamples)

	steadyState := measurePrimaryWritesWithHistory(
		t,
		root,
		"steady-state",
		primary,
		attrs,
		profile,
		profile.InterferenceSamples,
		100*time.Millisecond,
	)
	backfillSaturation := measurePrimaryWritesWithHistory(
		t,
		root,
		"unthrottled-backfill-saturation",
		primary,
		attrs,
		profile,
		profile.InterferenceSamples*2,
		0,
	)

	baseline.Name = "synthetic_primary_durable_write_baseline"
	baseline.Category = "store-stress"
	baseline.Parameters = map[string]any{"operation": "Commit+SyncWAL", "historyPublisher": false}
	steadyState.Name = "synthetic_primary_write_with_manual_history_cadence"
	steadyState.Category = "store-stress"
	steadyState.Parameters["publisherMode"] = "cadence-limited-steady-state"
	backfillSaturation.Name = "synthetic_primary_write_with_manual_store_saturation"
	backfillSaturation.Category = "store-stress"
	backfillSaturation.Parameters["publisherMode"] = "unthrottled-backfill-saturation"

	return live, baseline, steadyState, backfillSaturation
}

func measurePrimaryWritesWithHistory(
	t *testing.T,
	root, name string,
	primary *dal.Store,
	attrs *attributes.Attributes,
	profile perfProfile,
	writeStart int,
	publicationInterval time.Duration,
) perfLatency {
	t.Helper()

	history, err := balancehistorystore.New(
		filepath.Join(root, "history-"+name),
		logging.NopZap(),
		balancehistorystore.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("opening %s write-interference history store: %v", name, err)
	}
	defer func() {
		if err := history.Close(); err != nil {
			t.Errorf("closing %s write-interference history store: %v", name, err)
		}
	}()
	stop := make(chan struct{})
	done := make(chan struct{})
	ready := make(chan struct{})
	errCh := make(chan error, 1)
	var closeReady sync.Once
	go func() {
		defer close(done)

		sequence := uint64(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			sequence++
			effects := make([]historydomain.Effect, 0, profile.InterferenceBatchSize*2)
			for posting := range profile.InterferenceBatchSize {
				effects = append(effects, perfPostingEffects(
					perfDatasetConfig{Accounts: 64, AssetBuckets: 4, Colors: 2},
					posting%64, posting%4, posting%2, uint64(posting+1), sequence, sequence, sequence, sequence, uint32(posting*2),
				)...)
			}
			if _, publishErr := history.Publish(balancehistorystore.Publication{
				Effects: effects,
				Coverage: balancehistorystore.Coverage{
					AuditSequence: sequence, LogSequence: sequence, SourceComplete: true,
				},
			}); publishErr != nil {
				select {
				case errCh <- publishErr:
				default:
				}

				return
			}
			if sequence%balancehistorystore.DefaultRunCompactionThreshold == 0 {
				if _, compactErr := history.Compact(balancehistorystore.DefaultRunCompactionThreshold); compactErr != nil {
					select {
					case errCh <- compactErr:
					default:
					}

					return
				}
			}
			if sequence >= 3 {
				closeReady.Do(func() { close(ready) })
			}
			if publicationInterval > 0 {
				timer := time.NewTimer(publicationInterval)
				select {
				case <-stop:
					if !timer.Stop() {
						<-timer.C
					}

					return
				case <-timer.C:
				}
			}
		}
	}()
	select {
	case <-ready:
	case interferenceErr := <-errCh:
		close(stop)
		t.Fatalf("starting %s history write interference: %v", name, interferenceErr)
	case <-time.After(10 * time.Second):
		close(stop)
		t.Fatal("timed out starting history write interference")
	}
	interference := measurePrimaryDurableWrites(t, primary, attrs, writeStart, profile.InterferenceSamples)
	close(stop)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out stopping history write interference")
	}
	select {
	case interferenceErr := <-errCh:
		t.Fatalf("running %s history write interference: %v", name, interferenceErr)
	default:
	}

	interference.Category = "write-interference"
	interference.Parameters = map[string]any{
		"operation": "Commit+SyncWAL", "historyPublisher": true,
		"historyBatchEffects": profile.InterferenceBatchSize * 2, "logicalCompactionThreshold": balancehistorystore.DefaultRunCompactionThreshold,
		"publicationIntervalMs": float64(publicationInterval) / float64(time.Millisecond),
	}

	return interference
}

func seedLiveVolumes(
	t *testing.T,
	store *dal.Store,
	attrs *attributes.Attributes,
	accounts, assetBuckets, colors int,
) {
	t.Helper()

	batch := store.OpenWriteSession()
	for account := range accounts {
		for asset := range assetBuckets {
			for color := range colors {
				base, precision := perfAsset(asset)
				key := domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: perfLedger, Account: perfAccount(account)},
					Asset:      domain.FormatAsset(base, precision), AssetBase: base, AssetPrecision: precision, Color: perfColor(color),
				}
				_, err := attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{
					Input: commonpb.NewUint256FromUint64(uint64(account + asset + color + 1)),
				})
				if err != nil {
					_ = batch.Cancel()
					t.Fatalf("seeding live volume: %v", err)
				}
			}
		}
	}
	if err := batch.Commit(); err != nil {
		_ = batch.Cancel()
		t.Fatalf("committing live volume seed: %v", err)
	}
}

func measurePrimaryDurableWrites(
	t *testing.T,
	store *dal.Store,
	attrs *attributes.Attributes,
	start, samples int,
) perfLatency {
	t.Helper()

	ioBefore := capturePrimaryPerfIO(t, store)
	runtimeBefore := capturePerfRuntime()
	durations := make([]int64, samples)
	for index := range samples {
		sequence := start + index
		batch := store.OpenWriteSession()
		key := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: perfLedger, Account: fmt.Sprintf("write:%08d", sequence)},
			Asset:      "USD/2", AssetBase: "USD", AssetPrecision: 2,
		}
		started := time.Now()
		_, setErr := attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(uint64(sequence + 1)),
		})
		if setErr != nil {
			_ = batch.Cancel()
			t.Fatalf("staging primary durable write %d: %v", sequence, setErr)
		}
		if err := batch.Commit(); err != nil {
			_ = batch.Cancel()
			t.Fatalf("committing primary durable write %d: %v", sequence, err)
		}
		if err := store.SyncWAL(); err != nil {
			t.Fatalf("syncing primary durable write %d: %v", sequence, err)
		}
		durations[index] = time.Since(started).Nanoseconds()
	}
	runtimeAfter := capturePerfRuntime()

	measurement := summarizePerfDurations("primary_durable_write", "write-interference", durations, nil)
	attachPerfRuntime(&measurement, runtimeBefore, runtimeAfter)
	ioAfter := capturePrimaryPerfIO(t, store)
	measurement.IO = diffPrimaryPerfIO(ioBefore, ioAfter)

	return measurement
}

func capturePrimaryPerfIO(t *testing.T, store *dal.Store) *servicepb.PebbleMetrics {
	t.Helper()

	metrics, ok := store.GetMetrics().(*servicepb.PebbleMetrics)
	if !ok || metrics == nil {
		t.Fatal("primary store did not expose Pebble metrics")
	}

	return metrics
}

func diffPrimaryPerfIO(before, after *servicepb.PebbleMetrics) *perfReadIO {
	return &perfReadIO{
		BlockCacheHits:   after.GetBlockCache().GetHits() - before.GetBlockCache().GetHits(),
		BlockCacheMisses: after.GetBlockCache().GetMisses() - before.GetBlockCache().GetMisses(),
		WALBytesIn:       after.GetWal().GetBytesIn() - before.GetWal().GetBytesIn(),
		WALBytesWritten:  after.GetWal().GetBytesWritten() - before.GetWal().GetBytesWritten(),
		Flushes:          after.GetFlush().GetCount() - before.GetFlush().GetCount(),
		Compactions:      after.GetCompact().GetCount() - before.GetCompact().GetCount(),
		DiskBytesBefore:  before.GetDiskSpaceUsage(),
		DiskBytesAfter:   after.GetDiskSpaceUsage(),
	}
}

func findPerfMeasurement(t *testing.T, measurements []perfLatency, name string) perfLatency {
	t.Helper()

	for _, measurement := range measurements {
		if measurement.Name == name {
			return measurement
		}
	}
	t.Fatalf("missing performance measurement %s", name)

	return perfLatency{}
}

func writePerfEvidence(t *testing.T, report perfEvidenceReport) {
	t.Helper()

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshaling PIT performance evidence: %v", err)
	}
	output := os.Getenv("PIT_PERF_OUTPUT")
	if output == "" {
		t.Logf("PIT_PERF_OUTPUT is unset; raw evidence follows:\n%s", encoded)

		return
	}
	if !filepath.IsAbs(output) {
		cwd, cwdErr := os.Getwd()
		if cwdErr != nil {
			t.Fatalf("resolving PIT performance output path: %v", cwdErr)
		}
		output = filepath.Join(cwd, output)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		t.Fatalf("creating PIT performance output directory: %v", err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("writing PIT performance evidence %s: %v", output, err)
	}
	t.Logf("wrote PIT performance evidence to %s", output)
}

func valueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}

	return value
}

func TestPerfPercentileUsesNearestRank(t *testing.T) {
	t.Parallel()

	samples := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	if got := perfPercentile(samples, 0.50); got != 10 {
		t.Fatalf("p50 = %d, want 10", got)
	}
	if got := perfPercentile(samples, 0.95); got != 19 {
		t.Fatalf("p95 = %d, want 19", got)
	}
	if got := perfPercentile(samples, 0.99); got != 20 {
		t.Fatalf("p99 = %d, want 20", got)
	}
}

func TestPerfLogarithmicRunCountLimit(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		publications int
		want         int
	}{
		{publications: 1, want: 3},
		{publications: 4, want: 6},
		{publications: 5, want: 9},
		{publications: 16, want: 9},
		{publications: 17, want: 12},
		{publications: 731, want: 18},
	} {
		if got := perfLogarithmicRunCountLimit(testCase.publications, 4); got != testCase.want {
			t.Fatalf("run count limit for %d publications = %d, want %d", testCase.publications, got, testCase.want)
		}
	}
}

func TestPerfRunLevelSummaryCountsHotAndColdTogether(t *testing.T) {
	t.Parallel()

	manifest := balancehistorystore.Manifest{Runs: []balancehistorystore.RunRef{
		{Level: 0},
		{Level: 0, LocalRemoved: true},
		{Level: 0},
		{Level: 1, LocalRemoved: true},
		{Level: 1},
	}}
	levels, bounded := perfSummarizeRunLevels(manifest, 4)
	if !bounded {
		t.Fatal("three combined hot+cold runs at one level should remain below threshold four")
	}
	if got := perfMaxRunsAtLevel(levels); got != 3 {
		t.Fatalf("max runs per level = %d, want 3", got)
	}

	manifest.Runs = append(manifest.Runs, balancehistorystore.RunRef{Level: 0, LocalRemoved: true})
	_, bounded = perfSummarizeRunLevels(manifest, 4)
	if bounded {
		t.Fatal("four combined hot+cold runs at one level should violate exclusive threshold four")
	}
}

func TestPerfEvidenceIsOptIn(t *testing.T) {
	t.Parallel()

	if os.Getenv("PIT_PERF") == "1" {
		t.Skip("performance harness explicitly enabled")
	}
	if _, err := os.Stat(os.Getenv("PIT_PERF_OUTPUT")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checking disabled performance output: %v", err)
	}
}
