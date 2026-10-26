//go:build integration

package mirror

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A real INSERT has already allocated its sequence value before B commits.
// The adapter, translation, preload, FSM apply and durable boundary are real;
// applyingMirrorProposer substitutes only the Raft transport.
func TestWorker_PostgresGapCommitAndRollback(t *testing.T) {
	t.Parallel()
	for _, rollback := range []bool{false, true} {
		for _, pageSize := range []int{1, 100} {
			t.Run(fmt.Sprintf("rollback=%t/page=%d", rollback, pageSize), func(t *testing.T) {
				t.Parallel()
				dsn, observer := newPostgresLogFixture(t)
				ctx, cancel := context.WithTimeout(logging.TestingContext(), time.Minute)
				defer cancel()
				insert := func(tx interface {
					QueryRow(context.Context, string, ...any) pgx.Row
				}, account, marker string) uint64 {
					t.Helper()
					var id uint64
					err := tx.QueryRow(ctx, `INSERT INTO source_bucket.logs(id,ledger,type,date,data) VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA','2024-01-01T00:00:00Z',jsonb_build_object('targetType','ACCOUNT','targetId',$1::text,'metadata',jsonb_build_object('k',$2::text))) RETURNING id`, account, marker).Scan(&id)
					require.NoError(t, err)

					return id
				}
				require.Equal(t, uint64(1), insert(observer, "base", "0"))

				logger := logging.FromContext(ctx)
				meters := noop.NewMeterProvider()
				store, err := dal.NewStore(t.TempDir(), logger, meters.Meter("test"), dal.DefaultConfig())
				require.NoError(t, err)
				seedMirrorAuditKey(t, store)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				attrs := attributes.New()
				c, err := cache.New(100, meters.Meter("test"))
				require.NoError(t, err)
				registry := state.NewStateRegistry(c, attrs)
				machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store, dal.NewSentinelFactory(store, false), meters, keystore.NewKeyStore(), state.NewSharedState(), signal.NewNotifications(), nil, "test-cluster", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
				require.NoError(t, err)
				writeMirrorMetadataPolicy(t, store, mirrorMetadataPolicy())
				require.NoError(t, state.NewRecovery(machine, store).RecoverState())
				key := domain.LedgerKey{Name: "mirrored"}
				batch := store.OpenWriteSession()
				_, _, err = registry.Ledgers.PutWithCache(batch, 0, key.Bytes(), &commonpb.LedgerInfo{Name: key.Name, Id: 1, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR})
				require.NoError(t, err)
				_, _, err = registry.Boundaries.PutWithCache(batch, 0, key.Bytes(), &raftcmdpb.LedgerBoundaries{NextTransactionId: 1})
				require.NoError(t, err)
				require.NoError(t, batch.Commit())
				tracker := node.NewIndexTracker(1)
				builder := plan.NewBuilder(tracker, c, attrs, store, nil, logger, 0)
				var applied []*raftcmdpb.MirrorLogEntry
				proposer := &applyingMirrorProposer{t: t, machine: machine, store: store, tracker: tracker, beforeApply: func(cmd *raftcmdpb.Proposal) {
					for _, order := range cmd.GetOrders() {
						applied = append(applied, order.GetLedgerScoped().GetMirrorIngest().GetEntry().CloneVT())
					}
				}}
				newWorker := func() (*Worker, *v2.PostgresSource) {
					source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, source.Close()) })

					return NewWorker(key.Name, pageSize, source, nil, store, proposer, builder, logger, meters), source
				}
				worker, source := newWorker()
				more, err := worker.processBatch(ctx)
				require.NoError(t, err)
				require.False(t, more)
				require.Equal(t, uint64(1), worker.lastAppliedV2LogID)

				connectionA, err := pgx.Connect(ctx, dsn)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, connectionA.Close(context.Background())) })
				transactionA, err := connectionA.Begin(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					if err := transactionA.Rollback(context.Background()); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
						require.NoError(t, err)
					}
				})
				require.Equal(t, uint64(2), insert(transactionA, "alpha", "A"))
				transactionB, err := observer.Begin(ctx)
				require.NoError(t, err)
				require.Equal(t, uint64(3), insert(transactionB, "beta", "B"))
				require.NoError(t, transactionB.Commit(ctx))

				worker.backoff = -time.Nanosecond
				worker.processLogs(ctx)
				status, err := query.ReadMirrorStatus(store, key.Name)
				require.NoError(t, err)
				require.Contains(t, status.GetMessage(), "SQLSTATE 55P03", "outer retry path must persist the lock-conflict diagnostic")
				require.False(t, worker.boundariesLoaded, "failed ingestion must invalidate the cached source boundary")
				require.Len(t, applied, 1, "neither a fabricated gap nor the higher source row may be proposed")
				require.Equal(t, uint64(1), worker.lastAppliedV2LogID)
				boundaries, err := builder.ReadBoundaries(key.Name)
				require.NoError(t, err)
				require.Equal(t, uint64(1), boundaries.GetLastMirrorV2LogId())
				if rollback {
					require.NoError(t, transactionA.Rollback(ctx))
				} else {
					require.NoError(t, transactionA.Commit(ctx))
				}

				_, err = worker.processBatch(ctx)
				require.NoError(t, err)
				// Drop speculative prefetch and the entire worker/source. The replacement
				// must recover its position exclusively from the FSM's persisted boundary.
				worker.drainPrefetch(worker.prefetchCh)
				worker.prefetchCh = nil
				require.NoError(t, source.Close())
				worker, _ = newWorker()
				for range 3 {
					more, err = worker.processBatch(ctx)
					require.NoError(t, err)
					if !more {
						break
					}
				}
				boundaries, err = builder.ReadBoundaries(key.Name)
				require.NoError(t, err)
				require.Equal(t, uint64(3), boundaries.GetLastMirrorV2LogId())
				require.Len(t, applied, 3, "restart must not replay an already applied source log")
				for i, entry := range applied {
					require.Equal(t, uint64(i+1), entry.GetV2LogId())
				}
				if rollback {
					require.NotNil(t, applied[1].GetFillGap())
				} else {
					require.NotNil(t, applied[1].GetSavedMetadata())
				}
				require.NotNil(t, applied[2].GetSavedMetadata())
				for account, marker := range map[string]string{"base": "0", "beta": "B"} {
					metadata, err := attrs.Metadata.Get(store, (domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: key.Name, Account: account}, Key: "k"}).Bytes())
					require.NoError(t, err)
					require.Equal(t, marker, metadata.GetStringValue())
				}
				alpha, err := attrs.Metadata.Get(store, (domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: key.Name, Account: "alpha"}, Key: "k"}).Bytes())
				require.NoError(t, err)
				if rollback {
					require.Nil(t, alpha)
				} else {
					require.Equal(t, "A", alpha.GetStringValue())
				}
			})
		}
	}
}
