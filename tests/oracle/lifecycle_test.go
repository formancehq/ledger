package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func createLifecycleLedger(mode commonpb.LedgerMode) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "L", Mode: mode}}}
}

func TestGlobalState_LifecycleCreation(t *testing.T) {
	t.Parallel()
	base := NewGlobalState()
	created := base.Apply(bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL)))
	require.True(t, created.OK)
	require.NotEqual(t, base.Fingerprint(), created.State.Fingerprint())
	require.Empty(t, created.State.Ledger("L").LogIDs())
	duplicate := created.State.Apply(bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL)))
	require.False(t, duplicate.OK)
	require.Equal(t, domain.ErrReasonLedgerAlreadyExists, duplicate.Reason)
	require.Empty(t, base.Ledgers())
}

func TestGlobalState_LifecycleDeletion(t *testing.T) {
	t.Parallel()
	created := NewGlobalState().Apply(bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL), oracletest.TxReq("world", "a:1", "USD", 5)))
	require.True(t, created.OK)
	before := created.State.Fingerprint()
	del := &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}}
	rolledBack := created.State.Apply(bulkOf(del, createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL)))
	require.False(t, rolledBack.OK)
	require.Equal(t, domain.ErrReasonLedgerDeleted, rolledBack.Reason)
	require.Equal(t, before, rolledBack.State.Fingerprint())
	deleted := created.State.Apply(keyedBulk("delete", del))
	require.True(t, deleted.OK)
	require.NotContains(t, deleted.State.Ledgers(), "L")
	lc, exists := deleted.State.Lifecycle("L")
	require.True(t, exists)
	require.True(t, lc.Deleted)
	require.Zero(t, deleted.State.Ledger("L").Txs().Len())
	require.Equal(t, before, created.State.Fingerprint())
	require.Equal(t, 1, created.State.Ledger("L").Txs().Len())
	rejected := deleted.State.Apply(bulkOf(oracletest.TxReq("world", "a:2", "USD", 1)))
	require.Equal(t, domain.ErrReasonLedgerDeleted, rejected.Reason)
	replay := deleted.State.Apply(keyedBulk("delete", del))
	require.True(t, replay.OK)
	require.Equal(t, deleted.State.Fingerprint(), replay.State.Fingerprint())
}

func TestGlobalState_LifecyclePromotion(t *testing.T) {
	t.Parallel()
	req := createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_MIRROR)
	req.GetCreateLedger().MirrorSource = &commonpb.MirrorSourceConfig{BatchSize: 17}
	created := NewGlobalState().Apply(bulkOf(req))
	require.True(t, created.OK)
	before := created.State.Fingerprint()
	denied := created.State.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)))
	require.False(t, denied.OK)
	require.Equal(t, domain.ErrReasonLedgerInMirrorMode, denied.Reason)
	schema := created.State.Apply(bulkOf(oracletest.AddTypeReq("T")))
	require.True(t, schema.OK)
	promote := &servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: "L"}}}
	promoted := created.State.Apply(bulkOf(promote))
	require.True(t, promoted.OK)
	lc, _ := promoted.State.Lifecycle("L")
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_NORMAL, lc.Mode)
	require.Nil(t, lc.MirrorSource)
	require.Empty(t, promoted.State.Ledger("L").LogIDs())
	require.Equal(t, before, created.State.Fingerprint())
	prior, _ := created.State.Lifecycle("L")
	require.Equal(t, uint32(17), prior.MirrorSource.GetBatchSize())
	prior.MirrorSource.BatchSize = 99
	require.Equal(t, before, created.State.Fingerprint())
	twice := promoted.State.Apply(bulkOf(promote))
	require.Equal(t, domain.ErrReasonLedgerNotInMirrorMode, twice.Reason)
	require.True(t, promoted.State.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5))).OK)
}

func TestMirrorSafeRequestAllowsMaintenanceConfiguration(t *testing.T) {
	t.Parallel()

	for _, req := range []*servicepb.Request{
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "region", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.RemoveFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "region"),
		oracletest.CreateIndexReq(nil),
		oracletest.DropIndexReq(nil),
	} {
		require.True(t, mirrorSafeRequest(req), "%T must be mirror-safe", req.GetApply().GetAction().GetData())
	}
}

func TestGlobalState_RejectsPromotionOfDeletedMirrorLedger(t *testing.T) {
	t.Parallel()

	create := createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_MIRROR)
	created := NewGlobalState().Apply(bulkOf(create)).State
	deleted := created.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{
		DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"},
	}})).State
	promoted := deleted.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_PromoteLedger{
		PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: "L"},
	}}))

	require.False(t, promoted.OK)
	require.Equal(t, domain.ErrReasonLedgerDeleted, promoted.Reason)
	require.Equal(t, deleted.Fingerprint(), promoted.State.Fingerprint())
}

func TestGlobalState_LifecycleMaintenanceCommitOrder(t *testing.T) {
	t.Parallel()
	base := NewGlobalState()
	enabled := base.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: true}}}))
	require.True(t, enabled.OK)
	require.True(t, enabled.State.MaintenanceMode())
	require.False(t, base.MaintenanceMode())
	require.Empty(t, enabled.State.Ledgers())
	require.NotEqual(t, base.Fingerprint(), enabled.State.Fingerprint())
	denied := enabled.State.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)))
	require.False(t, denied.OK)
	require.Equal(t, domain.ErrReasonMaintenanceMode, denied.Reason)
	mixed := enabled.State.Apply(bulkOf(
		&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: false}}},
		oracletest.TxReq("world", "a:1", "USD", 5),
	))
	require.False(t, mixed.OK)
	require.Equal(t, domain.ErrReasonMaintenanceMode, mixed.Reason)
	disabled := enabled.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: false}}}))
	require.True(t, disabled.OK)
	require.Equal(t, base.Fingerprint(), disabled.State.Fingerprint())
}

func TestGlobalState_MaintenanceGatesBeforeIdempotency(t *testing.T) {
	t.Parallel()

	write := Bulk{IdempotencyKey: "write", Requests: []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 5)}}
	committed := NewGlobalState().Apply(write)
	require.True(t, committed.OK)
	enabled := committed.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: true}}}))
	replay := enabled.State.Apply(write)
	require.False(t, replay.OK)
	require.Equal(t, domain.ErrReasonMaintenanceMode, replay.Reason)

	blocked := Bulk{IdempotencyKey: "blocked", Requests: []*servicepb.Request{oracletest.TxReq("world", "a:2", "USD", 5)}}
	require.Equal(t, domain.ErrReasonMaintenanceMode, enabled.State.Apply(blocked).Reason)
	disabled := enabled.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: false}}}))
	require.True(t, disabled.State.Apply(blocked).OK, "maintenance rejection must not freeze an idempotency outcome")
}

func TestGlobalState_MaintenanceGatesBeforeValidation(t *testing.T) {
	t.Parallel()

	enabled := NewGlobalState().Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{
		SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: true},
	}})).State
	emptyTransaction := bulkOf(oracletest.TxReqMulti(false))

	require.Equal(t, domain.ErrReasonMaintenanceMode, enabled.Apply(emptyTransaction).Reason)
	require.Equal(t, domain.ErrReasonValidation, NewGlobalState().Apply(emptyTransaction).Reason)
}

func TestGlobalState_MaintenanceAllowsEmptyBulk(t *testing.T) {
	t.Parallel()

	enabled := NewGlobalState().Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{
		SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: true},
	}})).State

	require.True(t, enabled.Apply(Bulk{}).OK)
}

func TestGlobalState_LifecycleInitialConfiguration(t *testing.T) {
	t.Parallel()
	req := createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL)
	req.GetCreateLedger().AccountTypes = map[string]*commonpb.AccountType{"cash": {Name: "ignored", Pattern: "cash:{id}"}}
	req.GetCreateLedger().InitialSchema = []*commonpb.SetMetadataFieldTypeCommand{{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "region", Type: commonpb.MetadataType_METADATA_TYPE_STRING}}
	created := NewGlobalState().Apply(bulkOf(req))
	require.True(t, created.OK)
	typ, exists := created.State.Ledger("L").Types().Get("cash")
	require.True(t, exists)
	require.Equal(t, "cash", typ.Name)
	require.Equal(t, "cash:{id}", typ.Pattern)
	require.Equal(t, "ignored", req.GetCreateLedger().GetAccountTypes()["cash"].GetName())
	field, exists := created.State.Ledger("L").AccountFieldTypes().Get("region")
	require.True(t, exists)
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, field)
	require.Empty(t, created.State.Ledger("L").LogIDs())
}

func TestGlobalState_LifecycleDeleteDiscardsSameBulkWrites(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().Apply(bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL))).State
	result := base.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5), &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}}))
	require.True(t, result.OK)
	require.NotContains(t, result.State.Ledgers(), "L")
	lifecycle, exists := result.State.Lifecycle("L")
	require.True(t, exists)
	require.True(t, lifecycle.Deleted)
	require.Contains(t, base.Ledgers(), "L")
	require.Empty(t, base.Ledger("L").LogIDs())
}

func TestGlobalState_LifecycleReplayedStream(t *testing.T) {
	t.Parallel()
	stream := []Bulk{
		bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_MIRROR)),
		bulkOf(&servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: "L"}}}),
		bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)),
		bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}}),
	}
	original, replayed := NewGlobalState(), NewGlobalState()
	for _, bulk := range stream {
		result := original.Apply(bulk)
		require.True(t, result.OK)
		original = result.State
		requests := make([]*servicepb.Request, 0, len(bulk.Requests))
		for _, req := range bulk.Requests {
			data, err := req.MarshalVT()
			require.NoError(t, err)
			decoded := new(servicepb.Request)
			require.NoError(t, decoded.UnmarshalVT(data))
			requests = append(requests, decoded)
		}
		replay := replayed.Apply(Bulk{Requests: requests})
		require.True(t, replay.OK)
		replayed = replay.State
		require.Equal(t, original.Fingerprint(), replayed.Fingerprint())
	}
}

func TestGlobalState_TombstoneMetadataIsRejected(t *testing.T) {
	t.Parallel()
	created := NewGlobalState().Apply(bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL)))
	deleted := created.State.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}}))
	state := deleted.State
	for _, request := range []*servicepb.Request{
		{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: "L", Metadata: map[string]*commonpb.MetadataValue{"hidden": commonpb.NewStringValue("yes")}}}},
		{Type: &servicepb.Request_DeleteLedgerMetadata{DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{Ledger: "L", Key: "hidden"}}},
	} {
		result := state.Apply(bulkOf(request))
		require.False(t, result.OK)
		require.Equal(t, domain.ErrReasonLedgerDeleted, result.Reason)
		require.Equal(t, state.Fingerprint(), result.State.Fingerprint())
	}
}

func TestGlobalState_DeletionDoesNotBypassTransientValidation(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().Apply(bulkOf(createLifecycleLedger(commonpb.LedgerMode_LEDGER_MODE_NORMAL))).State
	result := base.Apply(bulkOf(
		oracletest.AddTypeReqP("temp", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
		oracletest.TxReq("world", "temp:1", "USD/2", 1),
		&servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}},
	))
	require.False(t, result.OK)
	require.Equal(t, domain.ErrReasonTransientAccountNonZero, result.Reason)
	require.Equal(t, base.Fingerprint(), result.State.Fingerprint())
}
