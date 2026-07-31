package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type balanceHistoryColdTestApp struct {
	app           *fx.App
	meterProvider *sdkmetric.MeterProvider
	store         *balancehistorystore.Store
	runtime       *balanceHistoryRuntime
	provider      ctrl.VolumeViewProvider

	stopOnce sync.Once
	stopErr  error
}

func (a *balanceHistoryColdTestApp) Stop() error {
	a.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.stopErr = a.app.Stop(ctx)
		a.stopErr = errors.Join(a.stopErr, a.meterProvider.Shutdown(ctx))
	})

	return a.stopErr
}

func TestBalanceHistoryColdTierFilesystemLifecycle(t *testing.T) {
	t.Parallel()

	const (
		clusterID        = "balance-history-cold-test"
		ledgerID         = uint32(7)
		transactionCount = 10
	)
	root := t.TempDir()
	historyDir := filepath.Join(root, "history")
	coldRoot := filepath.Join(root, "cold")
	logger := logging.Testing()
	primaryMeter := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(
		filepath.Join(root, "primary"),
		logger,
		primaryMeter.Meter("test.primary"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, primary.Close())
		require.NoError(t, primaryMeter.Shutdown(context.Background()))
	})
	lastSequence := seedBalanceHistoryColdSource(t, primary, clusterID, transactionCount)

	cold := coldstorage.NewFilesystemStorage(coldRoot)
	config := balanceHistoryColdTestConfig(root, historyDir, clusterID)
	first := newBalanceHistoryColdTestApp(t, config, primary, cold)
	t.Cleanup(func() { require.NoError(t, first.Stop()) })
	startBalanceHistoryColdTestApp(t, first)

	require.Eventually(t, func() bool {
		return first.runtime.builder.LastProcessedAuditSequence() == lastSequence &&
			first.runtime.builder.Ready()
	}, 5*time.Second, 10*time.Millisecond)

	var archived balancehistorystore.RunRef
	require.Eventually(t, func() bool {
		manifest, manifestErr := first.store.Manifest()
		if manifestErr != nil {
			return false
		}
		for _, run := range manifest.Runs {
			if run.Archived && run.LocalRemoved {
				archived = run

				return true
			}
		}

		return false
	}, 5*time.Second, 10*time.Millisecond)
	require.NotEmpty(t, archived.ArchiveParts)
	rootedRef := archived.ArchiveParts[0].Ref
	require.NoError(t, first.Stop())

	cacheDir := filepath.Join(historyDir, "archive-cache")
	require.NoError(t, os.RemoveAll(cacheDir))

	second := newBalanceHistoryColdTestApp(t, config, primary, cold)
	t.Cleanup(func() { require.NoError(t, second.Stop()) })
	require.Zero(t, second.runtime.archive.CacheStats().Bytes)
	startBalanceHistoryColdTestApp(t, second)
	require.Eventually(t, func() bool {
		return second.runtime.builder.LastProcessedAuditSequence() == lastSequence &&
			second.runtime.builder.Ready()
	}, 5*time.Second, 10*time.Millisecond)

	view, err := second.provider.Open(
		context.Background(),
		"default",
		ledgerID,
		ctrl.PointInTimeSelector{At: 1_000, Axis: balancehistorystore.AxisEffective},
		lastSequence,
	)
	require.NoError(t, err)
	result, err := view.Aggregate(context.Background(), []string{"cash"}, query.AggregateOptions{})
	require.NoError(t, err)
	require.NoError(t, view.Close())
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, big.NewInt(transactionCount), result.GetVolumes()[0].GetInput().ToBigInt())
	require.Positive(t, second.runtime.archive.CacheStats().Bytes, "the archived run must be hydrated lazily after restart")

	orphan, err := second.runtime.archive.Archive(context.Background(), balancehistoryarchive.NewSliceStream(
		[]balancehistoryarchive.Record{{Key: []byte("orphan"), Value: []byte("not-rooted")}},
	))
	require.NoError(t, err)
	exists, err := second.runtime.archive.Exists(context.Background(), orphan)
	require.NoError(t, err)
	require.True(t, exists)
	require.Eventually(t, func() bool {
		exists, existsErr := second.runtime.archive.Exists(context.Background(), orphan)

		return existsErr == nil && !exists
	}, 5*time.Second, 10*time.Millisecond)
	rooted, err := second.runtime.archive.Exists(context.Background(), rootedRef)
	require.NoError(t, err)
	require.True(t, rooted, "remote GC must preserve manifest-rooted archive parts")
	require.NoError(t, second.Stop())

	disabledConfig := config
	disabledConfig.BalanceHistoryConfig.ColdTierEnabled = false
	disabledConfig.ColdStorageConfig = coldstorage.Config{Driver: "none"}
	disabledMeter := sdkmetric.NewMeterProvider()
	disabledApp := fx.New(
		fx.NopLogger,
		fx.Supply(disabledConfig, primary),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return disabledMeter },
		),
		balanceHistoryModule(),
		fx.Invoke(registerBalanceHistoryCloseLifecycle),
		fx.Invoke(registerBalanceHistoryQuiesceLifecycle),
	)
	require.ErrorContains(t, disabledApp.Err(), "cannot be disabled while archived runs remain")
	require.NoError(t, disabledMeter.Shutdown(context.Background()))
}

func balanceHistoryColdTestConfig(root, historyDir, clusterID string) Config {
	history := DefaultBalanceHistoryConfig()
	history.Dir = historyDir
	history.Enabled = true
	history.BuilderBatchSize = 1
	history.RunCompactionThreshold = 2
	history.MaintenanceInterval = 10 * time.Millisecond
	history.MaxCompactionsPerPass = 4
	history.DurabilityInterval = time.Millisecond
	history.VerifierInterval = time.Hour
	history.ColdTierEnabled = true
	history.RetainLocalRuns = 1
	history.ArchiveCacheMaxBytes = 1 << 20
	history.MaxSegmentBytes = 512
	history.MaxRunsPerTierPass = 1
	history.TierInterval = 10 * time.Millisecond
	history.RemoteGCInterval = 10 * time.Millisecond
	history.RemoteGCGracePeriod = time.Millisecond
	history.RemoteGCScanLimit = 100
	history.RemoteGCDeleteLimit = 10

	return Config{
		RaftConfig: node.NodeConfig{NodeID: 9},
		ClusterID:  clusterID,
		DataDir:    root,
		ColdStorageConfig: coldstorage.Config{
			Driver:   "filesystem",
			BasePath: filepath.Join(root, "cold"),
			BucketID: "pit-history",
		},
		BalanceHistoryConfig: history,
	}
}

func newBalanceHistoryColdTestApp(
	t *testing.T,
	config Config,
	primary *dal.Store,
	cold coldstorage.ColdStorage,
) *balanceHistoryColdTestApp {
	t.Helper()

	logger := logging.Testing()
	coldReader := coldstorage.NewColdReader(
		cold,
		balanceHistoryColdBucketID(config),
		t.TempDir(),
		2,
		0,
		logger,
	)
	testApp := &balanceHistoryColdTestApp{meterProvider: sdkmetric.NewMeterProvider()}
	testApp.app = fx.New(
		fx.NopLogger,
		fx.Supply(config, primary, coldReader),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return testApp.meterProvider },
			func() coldstorage.ColdStorage { return cold },
		),
		balanceHistoryModule(),
		fx.Invoke(registerBalanceHistoryCloseLifecycle),
		fx.Invoke(registerBalanceHistoryQuiesceLifecycle),
		fx.Invoke(registerColdReaderLifecycle),
		fx.Populate(&testApp.store, &testApp.runtime, &testApp.provider),
	)
	require.NoError(t, testApp.app.Err())

	return testApp
}

func startBalanceHistoryColdTestApp(t *testing.T, app *balanceHistoryColdTestApp) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.app.Start(ctx))
}

func seedBalanceHistoryColdSource(
	t *testing.T,
	store *dal.Store,
	clusterID string,
	transactionCount int,
) uint64 {
	t.Helper()

	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	previousHash := []byte(nil)
	lastSequence := uint64(transactionCount + 1)
	for sequence := uint64(1); sequence <= lastSequence; sequence++ {
		log := balanceHistoryColdTestLog(sequence)
		item := &auditpb.AuditItem{
			OrderIndex:      0,
			LogSequence:     sequence,
			SerializedOrder: fmt.Appendf(nil, "cold-test-order-%d", sequence),
		}
		entry := &auditpb.AuditEntry{
			Sequence:   sequence,
			Timestamp:  &commonpb.Timestamp{Data: sequence},
			ProposalId: sequence,
			OrderCount: 1,
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
				MinLogSequence: sequence,
				MaxLogSequence: sequence,
			}},
		}
		header, err := state.BuildHashedHeaderPayload(entry)
		require.NoError(t, err)
		_, entry.Hash = processing.NewHashGenerator(
			commonpb.HashAlgorithm(entry.GetHashVersion()),
			clusterID,
		).Compute(nil, previousHash, [][]byte{header, state.BuildPerItemPayload(item)})
		previousHash = append(previousHash[:0], entry.GetHash()...)

		require.NoError(t, batch.SetProto(balanceHistoryColdAuditKey(sequence), entry))
		require.NoError(t, batch.SetProto(balanceHistoryColdAuditItemKey(sequence), item))
		require.NoError(t, batch.SetProto(balanceHistoryColdLogKey(sequence), log))
	}
	require.NoError(t, batch.Commit())

	return lastSequence
}

func balanceHistoryColdTestLog(sequence uint64) *commonpb.Log {
	if sequence == 1 {
		return &commonpb.Log{
			Sequence: sequence,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{Name: "default", Id: 7},
			}},
		}
	}

	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "default",
			Log: &commonpb.LedgerLog{
				Id: sequence,
				Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
						Postings: []*commonpb.Posting{{
							Source: "world", Destination: "cash", Asset: "USD",
							Amount: &commonpb.Uint256{V0: 1},
						}},
						Timestamp:  &commonpb.Timestamp{Data: 100 + sequence},
						InsertedAt: &commonpb.Timestamp{Data: 200 + sequence},
					}},
				}},
			},
		}}},
	}
}

func balanceHistoryColdAuditKey(sequence uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(sequence).
		Build()
}

func balanceHistoryColdAuditItemKey(sequence uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).
		PutUint64(sequence).
		PutUint32(0).
		Build()
}

func balanceHistoryColdLogKey(sequence uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutUint64(sequence).
		Build()
}
