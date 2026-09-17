//go:build scenario

package operationslifecycle

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/formancehq/ledger/v3/tests/scenarios/scenariotest"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The same request stream crosses the real Apply RPC and the independent oracle.
// This guards against encoding plausible, but incorrect, lifecycle semantics.
func TestLifecycleModelMatchesService(t *testing.T) {
	sc := scenariotest.SetupSingleNode(t)
	state := oracle.NewGlobalState()
	apply := func(reqs ...*servicepb.Request) {
		t.Helper()
		predicted := state.Apply(oracle.Bulk{Requests: reqs})
		response, err := sc.Client.Apply(sc.Ctx(), servicepb.UnsignedApplyRequest("", reqs...))
		if predicted.OK {
			require.NoError(t, err)
			require.Len(t, response.GetLogs(), len(reqs))
			state = predicted.State
		} else {
			require.Error(t, err)
			var reason string
			for _, detail := range status.Convert(err).Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok {
					reason = info.GetReason()
				}
			}
			require.Equal(t, predicted.Reason, reason)
		}
	}
	apply(actions.CreateLedgerAction("L", nil), actions.AddAccountTypeAction("L", "cash", "cash:{id}"))
	apply(oracletest.TxReqRefL("L", "original", "world", "cash:1", "USD/2", 17))
	apply(oracletest.RevertReqL("L", 1, false))
	apply(actions.DeleteLedgerAction("L"))
	require.NotContains(t, state.Ledgers(), "L")
	_, err := sc.Client.GetLedger(sc.Ctx(), &servicepb.GetLedgerRequest{Ledger: "L"})
	require.Equal(t, codes.NotFound, status.Code(err))
	apply(actions.CreateLedgerAction("L", nil))
	apply(oracletest.TxReqL("L", "world", "cash:1", "USD/2", 1))
	apply(actions.CreateLedgerAction("transient-delete", nil))
	transientBefore, err := actions.GetLedger(sc.Ctx(), sc.Client, "transient-delete")
	require.NoError(t, err)
	transientStatsBefore, err := actions.GetLedgerStats(sc.Ctx(), sc.Client, "transient-delete")
	require.NoError(t, err)
	transientAccountsBefore, err := actions.ListAccountsFiltered(sc.Ctx(), sc.Client, "transient-delete", 100, "", nil)
	require.NoError(t, err)
	transientTransactionsBefore, err := actions.ListTransactionsFiltered(sc.Ctx(), sc.Client, "transient-delete", 100, 0, nil)
	require.NoError(t, err)
	apply(actions.AddAccountTypeWithPersistenceAction("transient-delete", "temp", "temp:{id}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT), oracletest.TxReqL("transient-delete", "world", "temp:1", "USD/2", 1), actions.DeleteLedgerAction("transient-delete"))
	transientAfter, err := actions.GetLedger(sc.Ctx(), sc.Client, "transient-delete")
	require.NoError(t, err)
	transientStatsAfter, err := actions.GetLedgerStats(sc.Ctx(), sc.Client, "transient-delete")
	require.NoError(t, err)
	transientAccountsAfter, err := actions.ListAccountsFiltered(sc.Ctx(), sc.Client, "transient-delete", 100, "", nil)
	require.NoError(t, err)
	transientTransactionsAfter, err := actions.ListTransactionsFiltered(sc.Ctx(), sc.Client, "transient-delete", 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, transientBefore, transientAfter)
	require.Equal(t, transientStatsBefore, transientStatsAfter)
	require.Equal(t, transientAccountsBefore, transientAccountsAfter)
	require.Equal(t, transientTransactionsBefore, transientTransactionsAfter)
	// Repeated delete still operates on the retained LedgerInfo tombstone.
	apply(actions.DeleteLedgerAction("L"))

	apply(&servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "unconfigured"}}}}, actions.AddAccountTypeAction("mirror", "cash", "cash:{id}"))
	mirrorAccountsBefore, err := actions.ListAccountsFiltered(sc.Ctx(), sc.Client, "mirror", 100, "", nil)
	require.NoError(t, err)
	mirrorTransactionsBefore, err := actions.ListTransactionsFiltered(sc.Ctx(), sc.Client, "mirror", 100, 0, nil)
	require.NoError(t, err)
	apply(oracletest.TxReqL("mirror", "world", "cash:1", "USD/2", 1))
	mirrorAccountsAfter, err := actions.ListAccountsFiltered(sc.Ctx(), sc.Client, "mirror", 100, "", nil)
	require.NoError(t, err)
	mirrorTransactionsAfter, err := actions.ListTransactionsFiltered(sc.Ctx(), sc.Client, "mirror", 100, 0, nil)
	require.NoError(t, err)
	require.Equal(t, mirrorAccountsBefore, mirrorAccountsAfter)
	require.Equal(t, mirrorTransactionsBefore, mirrorTransactionsAfter)
	promote := &servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: "mirror"}}}
	apply(promote)
	apply(promote)
	apply(oracletest.TxReqL("mirror", "world", "cash:1", "USD/2", 1))
	info, err := sc.Client.GetLedger(sc.Ctx(), &servicepb.GetLedgerRequest{Ledger: "mirror"})
	require.NoError(t, err)
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_NORMAL, info.GetMode())
	require.Nil(t, info.GetMirrorSource())

	t.Cleanup(func() {
		_, cleanupErr := sc.Client.Apply(sc.Ctx(), servicepb.UnsignedApplyRequest("", actions.SetMaintenanceModeAction(false)))
		require.NoError(t, cleanupErr)
	})
	apply(actions.SetMaintenanceModeAction(true))
	require.True(t, state.MaintenanceMode())
	_, err = sc.Client.GetLedger(sc.Ctx(), &servicepb.GetLedgerRequest{Ledger: "mirror"})
	require.NoError(t, err)
	_, err = sc.Client.Apply(sc.Ctx(), servicepb.UnsignedApplyRequest("", actions.SaveLedgerMetadataAction("mirror", map[string]string{"k": "blocked"})))
	require.Error(t, err)
	found := false
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() == domain.ErrReasonMaintenanceMode {
			found = true
		}
	}
	require.True(t, found, "maintenance rejection must carry the specific business reason")
	info, err = sc.Client.GetLedger(sc.Ctx(), &servicepb.GetLedgerRequest{Ledger: "mirror"})
	require.NoError(t, err)
	require.NotContains(t, info.GetMetadata(), "k", "maintenance-rejected metadata must not commit")
	apply(actions.SetMaintenanceModeAction(false))
	apply(actions.SaveLedgerMetadataAction("mirror", map[string]string{"k": "recovered"}))
	info, err = sc.Client.GetLedger(sc.Ctx(), &servicepb.GetLedgerRequest{Ledger: "mirror"})
	require.NoError(t, err)
	require.Equal(t, "recovered", info.GetMetadata()["k"].GetStringValue())
}
