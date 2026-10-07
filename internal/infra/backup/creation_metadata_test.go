package backup

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func metadataCreationOrder(name string, mode commonpb.LedgerMode, metadata map[string]*commonpb.MetadataValue) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: name, Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
			Mode: mode, Metadata: metadata,
			InitialSchema: []*commonpb.SetMetadataFieldTypeCommand{{TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER, Key: "owner", Type: commonpb.MetadataType_METADATA_TYPE_INT64}},
		}},
	}}}
}

// Creation reads only the ledger row. Metadata writes need no preload/read.
func applyMetadataCreation(t *testing.T, machine *state.Machine, store *dal.Store, index uint64, orders ...*raftcmdpb.Order) []state.ApplyResult {
	t.Helper()
	plan := &raftcmdpb.ExecutionPlan{}
	for i, order := range orders {
		id, tag := attributes.MakeKey(domain.LedgerKey{Name: order.GetLedgerScoped().GetLedger()}.Bytes())
		plan.Attributes = append(plan.Attributes, &raftcmdpb.AttributeCoverage{Id: &raftcmdpb.AttributeID{Id: id[:], Tag: tag}, AttrCode: uint32(dal.SubAttrLedger)})
		bits := make([]byte, (len(orders)+7)/8)
		bits[i/8] |= 1 << (i % 8)
		order.Technical = &raftcmdpb.OrderTechnical{CoverageBits: bits}
	}
	proposal := &raftcmdpb.Proposal{Id: index, Orders: orders, Date: &commonpb.Timestamp{Data: 1700000000 + index},
		ExecutionPlan: plan, CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterConfig)}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	result, err := machine.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
	require.NoError(t, err)

	return result.Results
}

func readCreationLedger(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger string) *commonpb.LedgerInfo {
	t.Helper()
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	info, err := query.GetLedgerByName(t.Context(), handle, ledger)
	require.NoError(t, err)
	require.NoError(t, query.EnrichLedgerMetadata(handle, attrs, info))

	return info
}

func creationIntegrityFindings(t *testing.T, store *dal.Store, attrs *attributes.Attributes) []*servicepb.CheckStoreError {
	t.Helper()
	var findings []*servicepb.CheckStoreError
	checker := check.NewChecker(store, attrs, nil, testLogger())
	require.NoError(t, checker.Check(t.Context(), func(event *servicepb.CheckStoreEvent) {
		if e := event.GetError(); e != nil {
			findings = append(findings, e)
		}
	}))

	return findings
}

func TestBackup_CreateLedgerMetadataRestoreParity(t *testing.T) {
	t.Parallel()
	for _, mode := range []commonpb.LedgerMode{commonpb.LedgerMode_LEDGER_MODE_NORMAL, commonpb.LedgerMode_LEDGER_MODE_MIRROR} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			src, store, attrs := newSigningParityMachine(t)
			prefix := metadataCreationOrder("prefix", commonpb.LedgerMode_LEDGER_MODE_NORMAL, map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("before")})
			require.NoError(t, applyMetadataCreation(t, src, store, 1, prefix)[0].Error)
			storage := newInMemoryBackupStorage()
			full, err := RunBackup(t.Context(), testLogger(), store, storage, "bucket", "full")
			require.NoError(t, err)
			require.Positive(t, full.LastLogSequence)
			dstDir := t.TempDir()
			require.NoError(t, store.Checkpoint(filepath.Join(dstDir, "live")))

			metadata := map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("after"), "count": commonpb.NewUintValue(9007199254740993), "enabled": commonpb.NewBoolValue(true), "missing": commonpb.NewNullValue("unconverted")}
			order := metadataCreationOrder("delta", mode, metadata)
			before := order.CloneVT()
			results := applyMetadataCreation(t, src, store, 2, order)
			require.NoError(t, results[0].Error)
			require.True(t, before.GetLedgerScoped().EqualVT(order.GetLedgerScoped()), "accepted business payload must stay immutable")
			require.Equal(t, metadata, readCreationLedger(t, store, attrs, "delta").GetMetadata())
			require.Empty(t, creationIntegrityFindings(t, store, attrs))

			inc, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, "bucket", 0)
			require.NoError(t, err)
			require.Positive(t, inc.LogEntriesExported, "creation must be in a non-empty delta")
			manifest, err := ReadManifest(t.Context(), storage, ManifestKey("bucket"))
			require.NoError(t, err)
			require.NotEmpty(t, manifest.Checkpoint.Files)
			dst, err := dal.NewStore(dstDir, testLogger(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dst.Close()) })
			require.NoError(t, ApplyExportsAndRebuild(context.Background(), testLogger(), storage, dst, manifest))
			dstAttrs := attributes.New()
			require.Equal(t, readCreationLedger(t, store, attrs, "prefix"), readCreationLedger(t, dst, dstAttrs, "prefix"))
			require.Equal(t, readCreationLedger(t, store, attrs, "delta"), readCreationLedger(t, dst, dstAttrs, "delta"))
			require.Empty(t, creationIntegrityFindings(t, dst, dstAttrs))

			batch := dst.OpenWriteSession()
			_, err = dstAttrs.LedgerMetadata.Set(batch, domain.LedgerMetadataKey{LedgerName: "delta", Key: "owner"}.Bytes(), commonpb.NewStringValue("tampered"))
			require.NoError(t, err)
			require.NoError(t, batch.Commit())
			findings := creationIntegrityFindings(t, dst, dstAttrs)
			require.Len(t, findings, 1)
			require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH, findings[0].GetErrorType())
		})
	}
}

func TestCreateLedger_InitialMetadataLimitRollback(t *testing.T) {
	t.Parallel()
	machine, store, attrs := newSigningParityMachine(t)
	policy := machine.State.ClusterPolicy.CloneVT()
	policy.MetadataMaxValueBytes = 4
	machine.State.UpdateClusterPolicy(policy)
	first := metadataCreationOrder("first", commonpb.LedgerMode_LEDGER_MODE_NORMAL, map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("ok")})
	second := metadataCreationOrder("second", commonpb.LedgerMode_LEDGER_MODE_NORMAL, map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("too large")})
	results := applyMetadataCreation(t, machine, store, 1, first, second)
	require.Error(t, results[0].Error)
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	for _, ledger := range []string{"first", "second"} {
		_, err := query.GetLedgerByName(t.Context(), handle, ledger)
		require.ErrorIs(t, err, domain.ErrNotFound)
	}
	rows, err := attrs.LedgerMetadata.ComputeAllForPrefix(handle, nil)
	require.NoError(t, err)
	require.Empty(t, rows, "neither earlier valid nor later invalid creation may leave metadata")
	nextID, err := query.ReadNextLedgerID(handle)
	require.NoError(t, err)
	require.EqualValues(t, 1, nextID)
}
