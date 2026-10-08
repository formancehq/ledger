package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func createLifecycleLedger(mode ledgerpb.LedgerMode) *ledgerpb.Request {
	return &ledgerpb.Request{Type: &ledgerpb.Request_CreateLedger{CreateLedger: &ledgerpb.CreateLedgerRequest{Name: "L", Mode: mode}}}
}

func TestGlobalState_LifecycleCreation(t *testing.T) {
	t.Parallel()
	base := NewGlobalState()
	created := base.Apply(bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL)))
	require.True(t, created.OK)
	require.NotEqual(t, base.Fingerprint(), created.State.Fingerprint())
	require.Empty(t, created.State.Ledger("L").LogIDs())
	duplicate := created.State.Apply(bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL)))
	require.False(t, duplicate.OK)
	require.Equal(t, domain.ErrReasonLedgerAlreadyExists, duplicate.Reason)
	require.Empty(t, base.Ledgers())
}

func TestGlobalState_ImplicitLedgerLifecycleDefaultsToNormalMode(t *testing.T) {
	t.Parallel()

	base := NewGlobalState()
	base.ledgers["L"] = NewLedgerState()
	promoted := base.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_PromoteLedger{
		PromoteLedger: &ledgerpb.PromoteLedgerRequest{Ledger: "L"},
	}}))
	require.False(t, promoted.OK)
	require.Equal(t, domain.ErrReasonLedgerNotInMirrorMode, promoted.Reason)

	deleted := base.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{
		DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"},
	}}))
	require.True(t, deleted.OK)
	lifecycle, exists := deleted.State.Lifecycle("L")
	require.True(t, exists)
	require.True(t, lifecycle.Deleted)
	require.Equal(t, ledgerpb.LedgerMode_LEDGER_MODE_NORMAL, lifecycle.Mode)
}

func TestGlobalState_LifecycleDeletion(t *testing.T) {
	t.Parallel()
	created := NewGlobalState().Apply(bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL), oracletest.TxReq("world", "a:1", "USD", 5)))
	require.True(t, created.OK)
	before := created.State.Fingerprint()
	del := &ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"}}}
	rolledBack := created.State.Apply(bulkOf(del, createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL)))
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
	req := createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_MIRROR)
	req.GetCreateLedger().MirrorSource = &ledgerpb.MirrorSourceConfig{BatchSize: 17}
	created := NewGlobalState().Apply(bulkOf(req))
	require.True(t, created.OK)
	before := created.State.Fingerprint()
	denied := created.State.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)))
	require.False(t, denied.OK)
	require.Equal(t, domain.ErrReasonLedgerInMirrorMode, denied.Reason)
	schema := created.State.Apply(bulkOf(oracletest.AddTypeReq("T")))
	require.True(t, schema.OK)
	promote := &ledgerpb.Request{Type: &ledgerpb.Request_PromoteLedger{PromoteLedger: &ledgerpb.PromoteLedgerRequest{Ledger: "L"}}}
	promoted := created.State.Apply(bulkOf(promote))
	require.True(t, promoted.OK)
	lc, _ := promoted.State.Lifecycle("L")
	require.Equal(t, ledgerpb.LedgerMode_LEDGER_MODE_NORMAL, lc.Mode)
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

	for _, req := range []*ledgerpb.Request{
		oracletest.SetFieldTypeReq(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "region", ledgerpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.RemoveFieldTypeReq(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "region"),
		oracletest.CreateIndexReq(nil),
		oracletest.DropIndexReq(nil),
		actions.SaveLedgerMetadataAction("L", map[string]string{"region": "eu"}),
		actions.DeleteLedgerMetadataAction("L", "region"),
	} {
		require.True(t, mirrorSafeRequest(req), "%T must be mirror-safe", req.GetApply().GetAction().GetData())
	}
}

func TestGlobalState_RejectsPromotionOfDeletedMirrorLedger(t *testing.T) {
	t.Parallel()

	create := createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_MIRROR)
	created := NewGlobalState().Apply(bulkOf(create)).State
	deleted := created.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{
		DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"},
	}})).State
	promoted := deleted.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_PromoteLedger{
		PromoteLedger: &ledgerpb.PromoteLedgerRequest{Ledger: "L"},
	}}))

	require.False(t, promoted.OK)
	require.Equal(t, domain.ErrReasonLedgerDeleted, promoted.Reason)
	require.Equal(t, deleted.Fingerprint(), promoted.State.Fingerprint())
}

func TestGlobalState_LifecycleMaintenanceCommitOrder(t *testing.T) {
	t.Parallel()
	base := NewGlobalState()
	enabled := base.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: true}}}))
	require.True(t, enabled.OK)
	require.True(t, enabled.State.MaintenanceMode())
	require.False(t, base.MaintenanceMode())
	require.Empty(t, enabled.State.Ledgers())
	require.NotEqual(t, base.Fingerprint(), enabled.State.Fingerprint())
	denied := enabled.State.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)))
	require.False(t, denied.OK)
	require.Equal(t, domain.ErrReasonMaintenanceMode, denied.Reason)
	mixed := enabled.State.Apply(bulkOf(
		&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: false}}},
		oracletest.TxReq("world", "a:1", "USD", 5),
	))
	require.False(t, mixed.OK)
	require.Equal(t, domain.ErrReasonMaintenanceMode, mixed.Reason)
	disabled := enabled.State.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: false}}}))
	require.True(t, disabled.OK)
	require.Equal(t, base.Fingerprint(), disabled.State.Fingerprint())
}

func TestGlobalState_MaintenanceGatesBeforeIdempotency(t *testing.T) {
	t.Parallel()

	write := Bulk{IdempotencyKey: "write", Requests: []*ledgerpb.Request{oracletest.TxReq("world", "a:1", "USD", 5)}}
	committed := NewGlobalState().Apply(write)
	require.True(t, committed.OK)
	enabled := committed.State.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: true}}}))
	replay := enabled.State.Apply(write)
	require.False(t, replay.OK)
	require.Equal(t, domain.ErrReasonMaintenanceMode, replay.Reason)

	blocked := Bulk{IdempotencyKey: "blocked", Requests: []*ledgerpb.Request{oracletest.TxReq("world", "a:2", "USD", 5)}}
	require.Equal(t, domain.ErrReasonMaintenanceMode, enabled.State.Apply(blocked).Reason)
	disabled := enabled.State.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: false}}}))
	require.True(t, disabled.State.Apply(blocked).OK, "maintenance rejection must not freeze an idempotency outcome")
}

func TestGlobalState_MaintenanceGatesBeforeValidation(t *testing.T) {
	t.Parallel()

	enabled := NewGlobalState().Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{
		SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: true},
	}})).State
	emptyTransaction := bulkOf(oracletest.TxReqMulti(false))

	require.Equal(t, domain.ErrReasonMaintenanceMode, enabled.Apply(emptyTransaction).Reason)
	require.Equal(t, domain.ErrReasonValidation, NewGlobalState().Apply(emptyTransaction).Reason)
}

func TestGlobalState_MaintenanceAllowsEmptyBulk(t *testing.T) {
	t.Parallel()

	enabled := NewGlobalState().Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{
		SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: true},
	}})).State

	require.True(t, enabled.Apply(Bulk{}).OK)
}

func TestGlobalState_LifecycleInitialConfiguration(t *testing.T) {
	t.Parallel()
	req := createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL)
	req.GetCreateLedger().AccountTypes = map[string]*ledgerpb.AccountType{"cash": {Name: "ignored", Pattern: "cash:{id}"}}
	req.GetCreateLedger().InitialSchema = []*ledgerpb.SetMetadataFieldTypeCommand{{TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "region", Type: ledgerpb.MetadataType_METADATA_TYPE_STRING}}
	created := NewGlobalState().Apply(bulkOf(req))
	require.True(t, created.OK)
	typ, exists := created.State.Ledger("L").Types().Get("cash")
	require.True(t, exists)
	require.Equal(t, "cash", typ.Name)
	require.Equal(t, "cash:{id}", typ.Pattern)
	require.Equal(t, "ignored", req.GetCreateLedger().GetAccountTypes()["cash"].GetName())
	field, exists := created.State.Ledger("L").AccountFieldTypes().Get("region")
	require.True(t, exists)
	require.Equal(t, ledgerpb.MetadataType_METADATA_TYPE_STRING, field)
	require.Empty(t, created.State.Ledger("L").LogIDs())
}

func TestGlobalState_LifecycleDeleteDiscardsSameBulkWrites(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().Apply(bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL))).State
	result := base.Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5), &ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"}}}))
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
		bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_MIRROR)),
		bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_PromoteLedger{PromoteLedger: &ledgerpb.PromoteLedgerRequest{Ledger: "L"}}}),
		bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)),
		bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"}}}),
	}
	original, replayed := NewGlobalState(), NewGlobalState()
	for _, bulk := range stream {
		result := original.Apply(bulk)
		require.True(t, result.OK)
		original = result.State
		requests := make([]*ledgerpb.Request, 0, len(bulk.Requests))
		for _, req := range bulk.Requests {
			data, err := req.MarshalVT()
			require.NoError(t, err)
			decoded := new(ledgerpb.Request)
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
	created := NewGlobalState().Apply(bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL)))
	deleted := created.State.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"}}}))
	state := deleted.State
	for _, request := range []*ledgerpb.Request{
		{Type: &ledgerpb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &ledgerpb.SaveLedgerMetadataRequest{Ledger: "L", Metadata: map[string]*ledgerpb.MetadataValue{"hidden": ledgerpb.NewStringValue("yes")}}}},
		{Type: &ledgerpb.Request_DeleteLedgerMetadata{DeleteLedgerMetadata: &ledgerpb.DeleteLedgerMetadataRequest{Ledger: "L", Key: "hidden"}}},
	} {
		result := state.Apply(bulkOf(request))
		require.False(t, result.OK)
		require.Equal(t, domain.ErrReasonLedgerDeleted, result.Reason)
		require.Equal(t, state.Fingerprint(), result.State.Fingerprint())
	}
}

func TestGlobalState_DeletionDoesNotBypassTransientValidation(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().Apply(bulkOf(createLifecycleLedger(ledgerpb.LedgerMode_LEDGER_MODE_NORMAL))).State
	result := base.Apply(bulkOf(
		oracletest.AddTypeReqP("temp", ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
		oracletest.TxReq("world", "temp:1", "USD/2", 1),
		&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"}}},
	))
	require.False(t, result.OK)
	require.Equal(t, domain.ErrReasonTransientAccountNonZero, result.Reason)
	require.Equal(t, base.Fingerprint(), result.State.Fingerprint())
}
