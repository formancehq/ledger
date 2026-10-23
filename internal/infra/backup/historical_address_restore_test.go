package backup

import (
	"context"
	"encoding/binary"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/application/indexbuilder"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func TestBackup_HistoricalAddressSearchAfterPurgeDelta(t *testing.T) {
	t.Parallel()
	const ledger, address, bucket = "history", "clearing:1", "history-backup"
	ctx := context.Background()
	storage := newInMemoryBackupStorage()
	sourceStore := newBackupTestStore(t)
	seedBackupTestAuditKey(t, sourceStore)
	source := newScriptedNode(t, sourceStore)
	source.applyClusterPolicy(&commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes, MetadataMaxValueBytes: domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes: domain.DefaultMetadataMaxEntityBytes, MetadataMaxCommandBytes: domain.DefaultMetadataMaxCommandBytes,
	})
	source.apply(actions.CreateLedgerAction(ledger, nil),
		actions.AddEphemeralAccountTypeAction(ledger, "clearing", "clearing:{id}"),
		actions.CreateAddressIndexAction(ledger, commonpb.AddressRole_ADDRESS_ROLE_ANY),
		actions.CreateAddressIndexAction(ledger, commonpb.AddressRole_ADDRESS_ROLE_SOURCE),
		actions.CreateAddressIndexAction(ledger, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION))
	source.apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{
		actions.NewPosting("world", address, big.NewInt(100), "USD"),
	}, nil, nil))
	volumeKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: ledger, Account: address}, Asset: "USD"}
	before, err := attributes.New().Volume.Get(sourceStore, volumeKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, before, "the checkpoint must contain the funded account")
	require.Equal(t, "100", before.GetInput().ToBigInt().String())
	require.NoError(t, sourceStore.Flush())
	_, err = RunBackup(ctx, testLogger(), sourceStore, storage, bucket, "full")
	require.NoError(t, err)

	// This post-checkpoint committed drain changes both primary residency and
	// historical search. Its real logs, audit and AppliedProposal are exported.
	source.apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{
		actions.NewPosting(address, "world", big.NewInt(100), "USD"),
	}, nil, nil))
	require.NoError(t, sourceStore.Flush())
	inc, err := RunIncrementalBackup(ctx, testLogger(), sourceStore, storage, bucket, 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported)
	require.Positive(t, inc.AuditEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucket))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.NotEmpty(t, manifest.Exports)
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	auditKeyCheckerClean(t, restored)

	for _, pair := range []struct {
		name  string
		store *dal.Store
	}{{"source", sourceStore}, {"restored", restored}} {
		t.Run(pair.name, func(t *testing.T) {
			volume, err := attributes.New().Volume.Get(pair.store, volumeKey.Bytes())
			require.NoError(t, err)
			require.Nil(t, volume, "the post-checkpoint purge must remove the checkpoint volume")
			rs := buildHistoricalAddressProjection(t, pair.store, ledger)
			for _, reverse := range []bool{false, true} {
				for _, tc := range []struct {
					role commonpb.AddressRole
					want []uint64
				}{
					{commonpb.AddressRole_ADDRESS_ROLE_ANY, []uint64{1, 2}},
					{commonpb.AddressRole_ADDRESS_ROLE_SOURCE, []uint64{2}},
					{commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, []uint64{1}},
				} {
					want := slices.Clone(tc.want)
					if reverse {
						slices.Reverse(want)
					}
					require.Equal(t, want, restoredHistoricalAddressIDs(t, pair.store, rs, ledger, address, tc.role, reverse))
				}
			}
		})
	}
}

func buildHistoricalAddressProjection(t *testing.T, store *dal.Store, ledger string) *readstore.Store {
	t.Helper()
	rs, err := readstore.New(t.TempDir(), testLogger(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rs.Close()) })
	builder := indexbuilder.NewBuilder(store, rs, attributes.New(), testLogger(), noop.NewMeterProvider().Meter("test"), 100)
	builder.SetNotifications(signal.NewNotifications())
	builder.Start()
	t.Cleanup(builder.Stop)
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	last, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	require.Eventually(t, func() bool {
		if builder.LastIndexedSequence() != last {
			return false
		}
		for _, index := range []commonpb.TransactionBuiltinIndex{
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
		} {
			version, found, err := rs.ReadIndexVersionState(ledger, indexes.Canonical(indexes.TxBuiltinID(index)))
			if err != nil || !found || version.CurrentVersion == 0 {
				return false
			}
		}

		return true
	}, 10*time.Second, 10*time.Millisecond)

	return rs
}

func restoredHistoricalAddressIDs(t *testing.T, main *dal.Store, rs *readstore.Store,
	ledger, address string, role commonpb.AddressRole, reverse bool,
) []uint64 {
	t.Helper()
	attrs := attributes.New()
	handle, err := main.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	snapshot := rs.NewSnapshot()
	defer func() { require.NoError(t, snapshot.Close()) }()
	info, err := query.GetLedgerByName(t.Context(), handle, ledger)
	require.NoError(t, err)
	pin, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	registry := query.NewPebbleIndexReader(attrs.Index, handle)
	resolver := rs.PinnedVersionResolver(snapshot, ledger, pin)
	filter := actions.AddressExactRoleFilter(address, role)
	var iterator interface {
		Next() bool
		Current() []byte
		Err() error
		Close()
	}
	if reverse {
		iterator, err = query.CompileReverse(snapshot, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledger, nil, nil, info, registry, resolver, nil, handle, pin)
	} else {
		iterator, err = query.Compile(snapshot, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledger, nil, nil, info, registry, resolver, nil, handle, pin)
	}
	require.NoError(t, err)
	defer iterator.Close()
	var ids []uint64
	for iterator.Next() {
		ids = append(ids, binary.BigEndian.Uint64(iterator.Current()))
	}
	require.NoError(t, iterator.Err())

	return ids
}
