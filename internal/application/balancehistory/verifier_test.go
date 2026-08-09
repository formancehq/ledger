package balancehistory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	domainhistory "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newHistoryVerifierForTest(
	t *testing.T,
	source Source,
	store *balancehistorystore.Store,
) *HistoryVerifier {
	t.Helper()

	verifier, err := NewHistoryVerifier(
		source,
		store,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-verifier-test"),
		VerifierConfig{BatchSize: 1},
	)
	require.NoError(t, err)

	return verifier
}

func newVerifierFixture(
	t *testing.T,
) (*HotSource, *balancehistorystore.Store, hotSourceFixture, hotSourceFixture) {
	t.Helper()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	transaction := builderSuccessfulFixture(
		t,
		2,
		created.entry.GetHash(),
		"transaction",
		builderTransactionLog(2, "default", 42),
	)
	seedHotSource(t, primary, created, transaction)

	source := NewHotSource(primary)
	history := newBuilderTestHistoryStore(t)
	require.NoError(t, newBuilderForTest(t, source, history, nil).boot(context.Background()))

	return source, history, created, transaction
}

func requireHistoryQuarantined(t *testing.T, store *balancehistorystore.Store) {
	t.Helper()

	_, err := store.Manifest()
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
}

func TestHistoryVerifierAcceptsCompleteProjection(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	verifier := newHistoryVerifierForTest(t, source, history)

	require.NoError(t, verifier.Verify(context.Background()))
	require.Equal(t, uint64(1), verifier.VerifiedRuns())
	require.Zero(t, verifier.Failures())
	require.Positive(t, verifier.LastSuccessUnix())
}

func TestHistoryVerifierAcceptsCompleteEmptyProjection(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	source := NewHotSource(primary)
	history := newBuilderTestHistoryStore(t)
	require.NoError(t, newBuilderForTest(t, source, history, nil).boot(context.Background()))

	verifier := newHistoryVerifierForTest(t, source, history)
	require.NoError(t, verifier.Verify(context.Background()))
	require.Zero(t, verifier.Failures())
	require.Positive(t, verifier.LastSuccessUnix())
}

func TestDefaultVerifierConfigRunsFullSemanticReplayDaily(t *testing.T) {
	t.Parallel()

	config := DefaultVerifierConfig()
	require.Equal(t, 15*time.Minute, config.Interval)
	require.Equal(t, uint64(96), config.ReplayEvery)
	require.Equal(t, 1, config.SampleArchiveParts)
	require.Equal(t, DefaultBackfillYield, config.ReplayYield)
	require.Empty(t, config.ScratchParent)
}

func TestHistoryVerifierUsesAndCleansConfiguredScratchParent(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	scratchParent := filepath.Join(t.TempDir(), "nested", "verifier-scratch")
	config := DefaultVerifierConfig()
	config.BatchSize = 1
	config.ReplayYield = time.Nanosecond
	config.ScratchParent = scratchParent
	verifier, err := NewHistoryVerifier(
		source,
		history,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-scratch-parent-test"),
		config,
	)
	require.NoError(t, err)

	info, err := os.Stat(scratchParent)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.NoError(t, verifier.Verify(context.Background()))
	entries, err := os.ReadDir(scratchParent)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestHistoryVerifierCleansConfiguredScratchAfterCancellationDuringYield(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancelingSource := NewMockSource(gomock.NewController(t))
	cancelingSource.EXPECT().Head(gomock.Any()).DoAndReturn(source.Head)
	cancelingSource.EXPECT().Read(gomock.Any(), gomock.Any(), 1).DoAndReturn(
		func(readCtx context.Context, after Position, limit int) (Batch, error) {
			batch, err := source.Read(readCtx, after, limit)
			cancel()

			return batch, err
		},
	)

	scratchParent := filepath.Join(t.TempDir(), "verifier-scratch")
	config := DefaultVerifierConfig()
	config.BatchSize = 1
	config.ReplayYield = time.Hour
	config.ScratchParent = scratchParent
	verifier, err := NewHistoryVerifier(
		cancelingSource,
		history,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-scratch-cancel-test"),
		config,
	)
	require.NoError(t, err)

	require.ErrorIs(t, verifier.Verify(ctx), context.Canceled)
	entries, err := os.ReadDir(scratchParent)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestHistoryVerifierCleansConfiguredScratchAfterReplayFailure(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	missingSource := NewMockSource(gomock.NewController(t))
	missingSource.EXPECT().Head(gomock.Any()).DoAndReturn(source.Head)
	missingSource.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		Batch{},
		&ErrSourceMissing{Detail: "test source gap"},
	)

	scratchParent := filepath.Join(t.TempDir(), "verifier-scratch")
	config := DefaultVerifierConfig()
	config.BatchSize = 1
	config.ReplayYield = time.Nanosecond
	config.ScratchParent = scratchParent
	verifier, err := NewHistoryVerifier(
		missingSource,
		history,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-scratch-failure-test"),
		config,
	)
	require.NoError(t, err)

	err = verifier.Verify(context.Background())
	var sourceMissing *ErrSourceMissing
	require.ErrorAs(t, err, &sourceMissing)
	entries, err := os.ReadDir(scratchParent)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestNewHistoryVerifierRejectsNonDirectoryScratchParent(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	scratchParent := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(scratchParent, []byte("file"), 0o600))
	config := DefaultVerifierConfig()
	config.ScratchParent = scratchParent

	_, err := NewHistoryVerifier(
		source,
		history,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-invalid-scratch-parent-test"),
		config,
	)
	require.ErrorContains(t, err, "scratch parent")
}

func TestHistoryVerifierRunsPeriodicallyWithDeterministicReplaySampling(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	verifier, err := NewHistoryVerifier(
		source,
		history,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-periodic-verifier-test"),
		VerifierConfig{
			Interval:           time.Hour,
			BatchSize:          1,
			ReplayEvery:        3,
			SampleArchiveParts: 1,
			ReplayYield:        time.Nanosecond,
		},
	)
	require.NoError(t, err)
	fullCalls := 0
	sampleOffsets := make([]uint64, 0, 3)
	verifier.verifyStoreFull = func(context.Context) error {
		fullCalls++

		return nil
	}
	verifier.verifyStoreSample = func(
		_ context.Context,
		offset uint64,
		maxParts int,
	) (balancehistorystore.VerificationStats, error) {
		require.Equal(t, 1, maxParts)
		sampleOffsets = append(sampleOffsets, offset)

		return balancehistorystore.VerificationStats{
			ArchiveParts: 1,
			ArchiveBytes: 10,
			NextOffset:   offset + 1,
		}, nil
	}

	for range 5 {
		verifier.runPeriodic(context.Background())
	}

	require.Equal(t, 1, fullCalls)
	require.Equal(t, []uint64{0, 1, 2, 3}, sampleOffsets)
	require.Equal(t, uint64(5), verifier.VerifiedRuns())
	require.Zero(t, verifier.Failures())
}

func TestHistoryVerifierStopCancelsInFlightPhysicalScan(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	verifier, err := NewHistoryVerifier(
		source,
		history,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-stop-verifier-test"),
		VerifierConfig{Interval: time.Hour, ReplayEvery: 1},
	)
	require.NoError(t, err)
	started := make(chan struct{})
	verifier.verifyStoreFull = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	}
	verifier.Start()
	t.Cleanup(verifier.Stop)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("physical verification did not start")
	}
	stopped := make(chan struct{})
	go func() {
		verifier.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("verifier stop did not cancel the physical scan")
	}
	require.Zero(t, verifier.Failures())
	view, err := history.OpenView(2)
	require.NoError(t, err)
	require.NoError(t, view.Close())
}

func TestHistoryVerifierPersistsPhysicalSourceMissingUntilCertifiedRepair(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	verifier := newHistoryVerifierForTest(t, source, history)
	verifier.verifyStoreFull = func(context.Context) error {
		return &balancehistorystore.ErrSourceMissing{Detail: "cold archive part is absent"}
	}

	err := verifier.Verify(context.Background())
	var missing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	manifest, manifestErr := history.Manifest()
	require.NoError(t, manifestErr)
	_, openErr := history.OpenView(manifest.LogWatermark)
	require.ErrorAs(t, openErr, &missing)

	verifier.verifyStoreFull = history.VerifyContext
	require.NoError(t, verifier.Certify(
		context.Background(),
		manifest.AuditWatermark,
		manifest.LogWatermark,
	))
	_, openErr = history.OpenView(manifest.LogWatermark)
	require.ErrorAs(t, openErr, &missing)
	require.NoError(t, history.ClearFailure(manifest.AuditWatermark, manifest.LogWatermark))
	view, openErr := history.OpenView(manifest.LogWatermark)
	require.NoError(t, openErr)
	require.NoError(t, view.Close())
}

func TestHistoryVerifierRecordsBoundedArchiveWorkWithoutUnboundedLabels(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	verifier, err := NewHistoryVerifier(
		source,
		history,
		builderTestClusterID,
		logging.NopZap(),
		provider.Meter("balance-history-verifier-metrics-test"),
		VerifierConfig{SampleArchiveParts: 2},
	)
	require.NoError(t, err)
	verifier.verifyStoreSample = func(
		context.Context,
		uint64,
		int,
	) (balancehistorystore.VerificationStats, error) {
		return balancehistorystore.VerificationStats{
			ArchiveParts: 2,
			ArchiveBytes: 123,
			NextOffset:   2,
		}, nil
	}
	require.NoError(t, verifier.verifyObserved(context.Background(), false, 0, 0))

	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resources))
	values := map[string]int64{}
	physicalDurationFound := false
	for _, scope := range resources.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			switch data := instrument.Data.(type) {
			case metricdata.Sum[int64]:
				require.Len(t, data.DataPoints, 1)
				require.Empty(t, data.DataPoints[0].Attributes.ToSlice())
				values[instrument.Name] = data.DataPoints[0].Value
			case metricdata.Histogram[float64]:
				if instrument.Name != "balancehistory.verifier.physical.duration" {
					continue
				}
				physicalDurationFound = true
				require.Len(t, data.DataPoints, 1)
				require.Equal(t, uint64(1), data.DataPoints[0].Count)
				attributes := data.DataPoints[0].Attributes.ToSlice()
				require.Len(t, attributes, 1)
				require.Equal(t, "scope", string(attributes[0].Key))
				require.Equal(t, "sample", attributes[0].Value.AsString())
			}
		}
	}
	require.Equal(t, int64(2), values["balancehistory.verifier.archive.parts"])
	require.Equal(t, int64(123), values["balancehistory.verifier.archive.bytes"])
	require.True(t, physicalDurationFound)
}

func TestHistoryVerifierPinsManifestWhenSourceHeadIsAhead(t *testing.T) {
	t.Parallel()

	source, history, _, transaction := newVerifierFixture(t)
	primary, ok := source.reader.(*dal.Store)
	require.True(t, ok)

	ahead := builderSuccessfulFixture(
		t,
		3,
		transaction.entry.GetHash(),
		"ahead",
		builderTransactionLog(3, "default", 5),
	)
	seedHotSource(t, primary, ahead)

	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(2), manifest.AuditWatermark)

	verifier := newHistoryVerifierForTest(t, source, history)
	require.NoError(t, verifier.Verify(context.Background()))
	requireHistoryNotQuarantined(t, history)
}

func TestHistoryVerifierQuarantinesHashMismatchAtSameWatermark(t *testing.T) {
	t.Parallel()

	_, history, _, _ := newVerifierFixture(t)

	divergentPrimary := newHotSourceTestStore(t)
	divergentCreated := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	divergentTransaction := builderSuccessfulFixture(
		t,
		2,
		divergentCreated.entry.GetHash(),
		"different-transaction",
		builderTransactionLog(2, "default", 99),
	)
	seedHotSource(t, divergentPrimary, divergentCreated, divergentTransaction)

	err := newHistoryVerifierForTest(t, NewHotSource(divergentPrimary), history).Verify(context.Background())
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.ErrorContains(t, err, "audit hash")
	requireHistoryQuarantined(t, history)
}

func TestHistoryVerifierQuarantinesCorruptRun(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	iter, err := history.DB().NewIter(&pebble.IterOptions{
		LowerBound: []byte{0x10},
		UpperBound: []byte{0x11},
	})
	require.NoError(t, err)
	require.True(t, iter.First())
	key := append([]byte(nil), iter.Key()...)
	value := append([]byte(nil), iter.Value()...)
	require.NotEmpty(t, value)
	require.NoError(t, iter.Close())
	value[0] ^= 0xff
	require.NoError(t, history.DB().Set(key, value, pebble.Sync))

	err = newHistoryVerifierForTest(t, source, history).Verify(context.Background())
	var corrupt *balancehistorystore.ErrCorrupt
	require.ErrorAs(t, err, &corrupt)
	requireHistoryQuarantined(t, history)
}

func TestHistoryVerifierQuarantinesCoherentlyTamperedServedProjection(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	before, err := history.Manifest()
	require.NoError(t, err)
	require.NotEmpty(t, before.Runs)

	coherentlyTamperHistoryRun(t, history, before.Runs[0].ID)
	after, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, before.LogicalDigest, after.LogicalDigest)
	require.NotEqual(t, before.Runs[0].Checksum, after.Runs[0].Checksum)
	// The attacker updated every self-declared integrity field consistently.
	// Physical verification alone therefore cannot detect this substitution.
	require.NoError(t, history.Verify())

	err = newHistoryVerifierForTest(t, source, history).Verify(context.Background())
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.ErrorContains(t, err, "semantic digest")
	requireHistoryQuarantined(t, history)
}

func TestHistoryVerifierMarksIncompleteSourceAndHealsAfterFullReplay(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	missingSource := NewMockSource(gomock.NewController(t))
	missingSource.EXPECT().Head(gomock.Any()).AnyTimes().DoAndReturn(source.Head)
	missingSource.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(
		Batch{},
		&ErrSourceMissing{Detail: "test archive segment is absent"},
	)
	verifier := newHistoryVerifierForTest(t, missingSource, history)

	err := verifier.Verify(context.Background())
	var sourceMissing *ErrSourceMissing
	require.ErrorAs(t, err, &sourceMissing)
	require.Equal(t, uint64(1), verifier.Failures())

	manifest, manifestErr := history.Manifest()
	require.NoError(t, manifestErr)
	_, openErr := history.OpenView(manifest.LogWatermark)
	var storedSourceMissing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, openErr, &storedSourceMissing)
	var quarantined *balancehistorystore.ErrQuarantined
	require.False(t, errors.As(openErr, &quarantined))

	repaired := newHistoryVerifierForTest(t, source, history)
	require.NoError(t, repaired.Certify(context.Background(), manifest.AuditWatermark, manifest.LogWatermark))
	require.NoError(t, history.ClearFailure(manifest.AuditWatermark, manifest.LogWatermark))
	view, openErr := history.OpenView(manifest.LogWatermark)
	require.NoError(t, openErr)
	require.NoError(t, view.Close())
}

func TestHistoryVerifierCertifyProvesButDoesNotClearSourceFailure(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	require.NoError(t, history.MarkSourceMissing("source repair is awaiting certification"))
	verifier := newHistoryVerifierForTest(t, source, history)

	require.NoError(t, verifier.Certify(context.Background(), 2, 2))
	_, err := history.OpenView(2)
	var sourceMissing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, err, &sourceMissing)

	require.NoError(t, history.ClearFailure(2, 2))
	view, err := history.OpenView(2)
	require.NoError(t, err)
	require.NoError(t, view.Close())
}

func TestHistoryVerifierCertifiesRebuildBeforeReadsReopen(t *testing.T) {
	t.Parallel()

	source, history, _, _ := newVerifierFixture(t)
	require.NoError(t, history.ResetForRebuild())

	batch, err := source.Read(context.Background(), Position{}, 10)
	require.NoError(t, err)
	effects, next, reducerState, err := reduceVerifiedBatch(
		builderTestClusterID,
		domainhistory.NewReducer(),
		Position{},
		batch,
	)
	require.NoError(t, err)
	_, err = history.Publish(balancehistorystore.Publication{
		Effects: effects,
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  next.AuditSequence,
			LogSequence:    next.LogSequence,
			AuditHash:      next.AuditHash,
			SourceComplete: true,
		},
		ReducerState: reducerState,
	})
	require.NoError(t, err)

	verifier := newHistoryVerifierForTest(t, source, history)
	require.NoError(t, verifier.Certify(context.Background(), 2, 2))
	_, err = history.OpenView(2)
	var rebuilding *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &rebuilding)

	require.NoError(t, history.CompleteRebuild(2, 2))
	view, err := history.OpenView(2)
	require.NoError(t, err)
	require.NoError(t, view.Close())
}

func TestHistoryVerifierQuarantinesLogicalDigestMismatch(t *testing.T) {
	t.Parallel()

	source, validHistory, _, _ := newVerifierFixture(t)
	validManifest, err := validHistory.Manifest()
	require.NoError(t, err)

	divergentHistory := newBuilderTestHistoryStore(t)
	_, err = divergentHistory.Publish(balancehistorystore.Publication{
		Effects: []domainhistory.Effect{{
			LedgerID:       7,
			AuditSequence:  2,
			LogSequence:    2,
			EffectiveAt:    100,
			InsertedAt:     200,
			Account:        "cash",
			AssetBase:      "USD",
			AssetPrecision: 0,
			Input:          domainhistory.AmountFromUint64(41),
		}},
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  2,
			LogSequence:    2,
			AuditHash:      validManifest.AuditHash,
			SourceComplete: true,
		},
		ReducerState: validManifest.ReducerState,
	})
	require.NoError(t, err)

	err = newHistoryVerifierForTest(t, source, divergentHistory).Verify(context.Background())
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.ErrorContains(t, err, "logical digest")
	requireHistoryQuarantined(t, divergentHistory)
}

func TestHistoryVerifierQuarantinesReducerStateMismatch(t *testing.T) {
	t.Parallel()

	source, _, _, _ := newVerifierFixture(t)
	batch, err := source.Read(context.Background(), Position{}, 10)
	require.NoError(t, err)
	effects, next, reducerState, err := reduceVerifiedBatch(
		builderTestClusterID,
		domainhistory.NewReducer(),
		Position{},
		batch,
	)
	require.NoError(t, err)
	reducerState.Active = nil

	history := newBuilderTestHistoryStore(t)
	_, err = history.Publish(balancehistorystore.Publication{
		Effects: effects,
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  next.AuditSequence,
			LogSequence:    next.LogSequence,
			AuditHash:      next.AuditHash,
			SourceComplete: true,
		},
		ReducerState: reducerState,
	})
	require.NoError(t, err)

	err = newHistoryVerifierForTest(t, source, history).Verify(context.Background())
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.ErrorContains(t, err, "reducer state")
	requireHistoryQuarantined(t, history)
}

func TestNewHistoryVerifierValidatesRequiredDependencies(t *testing.T) {
	t.Parallel()

	store := newBuilderTestHistoryStore(t)
	source := NewMockSource(gomock.NewController(t))
	meter := noop.NewMeterProvider().Meter("balance-history-verifier-validation-test")

	_, err := NewHistoryVerifier(nil, store, builderTestClusterID, logging.NopZap(), meter, VerifierConfig{})
	require.ErrorContains(t, err, "source is nil")
	_, err = NewHistoryVerifier(source, nil, builderTestClusterID, logging.NopZap(), meter, VerifierConfig{})
	require.ErrorContains(t, err, "store is nil")
	_, err = NewHistoryVerifier(source, store, "", logging.NopZap(), meter, VerifierConfig{})
	require.ErrorContains(t, err, "cluster id is empty")

	verifier, err := NewHistoryVerifier(source, store, builderTestClusterID, logging.NopZap(), nil, VerifierConfig{})
	require.NoError(t, err)
	require.Equal(t, DefaultVerifierInterval, verifier.config.Interval)
	require.Equal(t, DefaultVerifierBatchSize, verifier.config.BatchSize)
	require.Equal(t, uint64(DefaultVerifierReplayEvery), verifier.config.ReplayEvery)
	require.Equal(t, DefaultVerifierSampleArchiveParts, verifier.config.SampleArchiveParts)
	require.Equal(t, DefaultBackfillYield, verifier.config.ReplayYield)
}

func TestHistoryVerifierValidateHeadFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		manifest balancehistorystore.Manifest
		head     Position
		want     string
	}{
		{
			name:     "manifest hash missing",
			manifest: balancehistorystore.Manifest{AuditWatermark: 1},
			head:     Position{AuditSequence: 1, AuditHash: []byte("head")},
			want:     "has no audit hash",
		},
		{
			name:     "source hash missing",
			manifest: balancehistorystore.Manifest{},
			head:     Position{AuditSequence: 1},
			want:     "authoritative head at audit 1 has no audit hash",
		},
		{
			name: "source audit behind",
			manifest: balancehistorystore.Manifest{
				AuditWatermark: 2,
				LogWatermark:   2,
				AuditHash:      []byte("manifest"),
			},
			head: Position{AuditSequence: 1, LogSequence: 2, AuditHash: []byte("head")},
			want: "is behind manifest",
		},
		{
			name: "source log behind",
			manifest: balancehistorystore.Manifest{
				AuditWatermark: 1,
				LogWatermark:   2,
				AuditHash:      []byte("manifest"),
			},
			head: Position{AuditSequence: 2, LogSequence: 1, AuditHash: []byte("head")},
			want: "is behind manifest",
		},
		{
			name: "same audit different log",
			manifest: balancehistorystore.Manifest{
				AuditWatermark: 1,
				LogWatermark:   1,
				AuditHash:      []byte("manifest"),
			},
			head: Position{AuditSequence: 1, LogSequence: 2, AuditHash: []byte("head")},
			want: "have log watermarks 2 and 1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := newBuilderTestHistoryStore(t)
			verifier := &HistoryVerifier{store: store}
			err := verifier.validateHead(test.manifest, test.head)
			require.ErrorContains(t, err, test.want)
		})
	}

	store := newBuilderTestHistoryStore(t)
	verifier := &HistoryVerifier{store: store}
	require.NoError(t, verifier.validateHead(
		balancehistorystore.Manifest{AuditWatermark: 1, LogWatermark: 1, AuditHash: []byte("hash")},
		Position{AuditSequence: 2, LogSequence: 2, AuditHash: []byte("head")},
	))
}

func TestHistoryVerifierReplayIntoHandlesCancellationAndSourceGaps(t *testing.T) {
	t.Parallel()

	t.Run("canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		verifier := &HistoryVerifier{config: VerifierConfig{BatchSize: 1}}
		scratch := newBuilderTestHistoryStore(t)
		_, _, _, err := verifier.replayInto(
			ctx,
			1,
			scratch,
			Position{},
			[32]byte{},
			domainhistory.NewReducer(),
		)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("source error", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("read failed")
		source := NewMockSource(gomock.NewController(t))
		source.EXPECT().Read(gomock.Any(), Position{}, 1).Return(Batch{}, wantErr)
		verifier := &HistoryVerifier{source: source, config: VerifierConfig{BatchSize: 1}}
		scratch := newBuilderTestHistoryStore(t)
		_, _, _, err := verifier.replayInto(
			context.Background(),
			1,
			scratch,
			Position{},
			[32]byte{},
			domainhistory.NewReducer(),
		)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "reading authoritative source")
	})

	t.Run("empty batch", func(t *testing.T) {
		t.Parallel()

		source := NewMockSource(gomock.NewController(t))
		source.EXPECT().Read(gomock.Any(), Position{}, 1).Return(Batch{}, nil)
		verifier := &HistoryVerifier{source: source, config: VerifierConfig{BatchSize: 1}}
		scratch := newBuilderTestHistoryStore(t)
		_, _, _, err := verifier.replayInto(
			context.Background(),
			1,
			scratch,
			Position{},
			[32]byte{},
			domainhistory.NewReducer(),
		)
		var missing *ErrSourceMissing
		require.ErrorAs(t, err, &missing)
		require.ErrorContains(t, err, "returned no proposal")
	})

	t.Run("empty target", func(t *testing.T) {
		t.Parallel()

		verifier := &HistoryVerifier{}
		scratch := newBuilderTestHistoryStore(t)
		digest, position, reducerState, err := verifier.replayInto(
			context.Background(),
			0,
			scratch,
			Position{},
			[32]byte{},
			domainhistory.NewReducer(),
		)
		require.NoError(t, err)
		require.Equal(t, [32]byte{}, digest)
		require.Equal(t, Position{}, position)
		require.Equal(t, domainhistory.State{}, reducerState)
	})
}

func TestHistoryVerifierErrorHelpers(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("scratch failed")
	scratchErr := &scratchProjectionError{err: wantErr}
	require.ErrorContains(t, scratchErr, "building balance history verification scratch projection")
	require.ErrorIs(t, scratchErr, wantErr)
	require.Nil(t, wrapNonNilError("ignored", nil))
	require.ErrorContains(t, wrapNonNilError("operation", wantErr), "operation")

	store := newBuilderTestHistoryStore(t)
	verifier := &HistoryVerifier{store: store}
	require.ErrorIs(t, verifier.markReplayFailure(context.Canceled), context.Canceled)
	require.Same(t, scratchErr, verifier.markReplayFailure(scratchErr))

	sourceErr := errors.New("source invalid")
	require.ErrorIs(t, verifier.markReplayFailure(sourceErr), sourceErr)
	_, err := store.OpenView(0)
	var sourceMissing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, err, &sourceMissing)
}

func requireHistoryNotQuarantined(t *testing.T, store *balancehistorystore.Store) {
	t.Helper()

	_, err := store.Manifest()
	require.NoError(t, err)
}

func coherentlyTamperHistoryRun(t *testing.T, store *balancehistorystore.Store, runID uint64) {
	t.Helper()

	dataPrefix := verifierStoreKey(0x10, runID)
	iter, err := store.DB().NewIter(&pebble.IterOptions{
		LowerBound: dataPrefix,
		UpperBound: append(append([]byte(nil), dataPrefix...), 0xff),
	})
	require.NoError(t, err)
	require.True(t, iter.First())
	dataKey := append([]byte(nil), iter.Key()...)
	require.Greater(t, len(dataKey), 17)
	catalogKey := append([]byte(nil), dataKey[:len(dataKey)-8]...)
	catalogKey[0] = 0x12
	require.NoError(t, iter.Close())

	manifest, err := store.Manifest()
	require.NoError(t, err)
	runIndex := -1
	for index := range manifest.Runs {
		if manifest.Runs[index].ID == runID {
			runIndex = index

			break
		}
	}
	require.NotEqual(t, -1, runIndex)

	batch := store.DB().NewBatch()
	t.Cleanup(func() { _ = batch.Close() })
	require.NoError(t, batch.Delete(dataKey, nil))
	require.NoError(t, batch.Delete(catalogKey, nil))
	require.NoError(t, batch.Commit(pebble.Sync))

	checksum := verifierRunChecksum(t, store, runID)
	require.Positive(t, manifest.Runs[runIndex].EntryCount)
	require.Positive(t, manifest.Runs[runIndex].IdentityCount)
	manifest.Runs[runIndex].EntryCount--
	manifest.Runs[runIndex].IdentityCount--
	manifest.Runs[runIndex].Checksum = checksum
	encodedRun, err := json.Marshal(manifest.Runs[runIndex])
	require.NoError(t, err)

	manifest.Digest = [32]byte{}
	unsignedManifest, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifest.Digest = sha256.Sum256(unsignedManifest)
	encodedManifest, err := json.Marshal(manifest)
	require.NoError(t, err)

	rewrite := store.DB().NewBatch()
	defer func() { require.NoError(t, rewrite.Close()) }()
	require.NoError(t, rewrite.Set(verifierStoreKey(0x11, runID), encodedRun, nil))
	require.NoError(t, rewrite.Set(verifierStoreKey(0x02, manifest.Version), encodedManifest, nil))
	require.NoError(t, rewrite.Commit(pebble.Sync))
}

func verifierRunChecksum(t *testing.T, store *balancehistorystore.Store, runID uint64) [32]byte {
	t.Helper()

	digest := sha256.New()
	for _, prefixByte := range []byte{0x10, 0x12} {
		prefix := verifierStoreKey(prefixByte, runID)
		iter, err := store.DB().NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: append(append([]byte(nil), prefix...), 0xff),
		})
		require.NoError(t, err)
		for valid := iter.First(); valid; valid = iter.Next() {
			verifierWriteCanonicalRecordHash(digest, iter.Key(), iter.Value())
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
	}

	var checksum [32]byte
	copy(checksum[:], digest.Sum(nil))

	return checksum
}

func verifierWriteCanonicalRecordHash(digest hash.Hash, key, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)-8))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(key[:1])
	_, _ = digest.Write(key[9:])
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func verifierStoreKey(prefix byte, id uint64) []byte {
	key := make([]byte, 9)
	key[0] = prefix
	binary.BigEndian.PutUint64(key[1:], id)

	return key
}
