package check

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestCheckerFSMRejectsSurplusIndexRegistration adds an unaudited ledger-scoped
// SubAttrIndex row next to an FSM-written registration. The surplus row must be
// reported whether or not its value carries an Id.
func TestCheckerFSMRejectsSurplusIndexRegistration(t *testing.T) {
	t.Parallel()

	audited := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	surplus := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	surplusKey := domain.IndexKey{LedgerName: "ledger", Canonical: indexes.Canonical(surplus)}

	for _, tc := range []struct {
		name  string
		value *commonpb.Index
	}{
		{name: "with id", value: &commonpb.Index{Ledger: "ledger", Id: surplus}},
		{name: "without id", value: &commonpb.Index{Ledger: "ledger"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			machine, store, attrs := newFSMPurgeMachine(t)
			applyFSMPurgeProposal(t, machine, store, attrs, 1,
				fsmPurgeCreateLedgerOrder(commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL))
			applyFSMPurgeProposal(t, machine, store, attrs, 2, fsmCreateIndexOrder(audited))
			require.NoError(t, store.Flush())
			require.Empty(t, runFSMPurgeCheck(t, store, attrs), "the FSM-produced registry must pass")

			batch := store.OpenWriteSession()
			_, err := attrs.Index.Set(batch, surplusKey.Bytes(), tc.value)
			require.NoError(t, err)
			require.NoError(t, batch.Commit())

			findings := runFSMPurgeCheck(t, store, attrs)
			require.Len(t, findings, 1, "only the surplus registration may be reported")
			require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
				findings[0].GetErrorType())
			require.Equal(t, "ledger", findings[0].GetLedger())
			require.True(t, strings.Contains(findings[0].GetMessage(), "no matching CreateIndex"),
				"unexpected finding: %s", findings[0].GetMessage())
		})
	}
}

// TestCheckerFSMRejectsAuditedIndexRegistrationWithoutID strips the Id from an
// FSM-written registration. The row still occupies the audited key, so it is
// reported as identity drift rather than as a missing registration.
func TestCheckerFSMRejectsAuditedIndexRegistrationWithoutID(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	key := domain.IndexKey{LedgerName: "ledger", Canonical: indexes.Canonical(id)}.Bytes()

	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1,
		fsmPurgeCreateLedgerOrder(commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL))
	applyFSMPurgeProposal(t, machine, store, attrs, 2, fsmCreateIndexOrder(id))
	require.NoError(t, store.Flush())
	require.Empty(t, runFSMPurgeCheck(t, store, attrs), "the FSM-produced registry must pass")

	batch := store.OpenWriteSession()
	_, err := attrs.Index.Set(batch, key, &commonpb.Index{Ledger: "ledger"})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	findings := runFSMPurgeCheck(t, store, attrs)
	require.Len(t, findings, 1, "only the stripped registration may be reported")
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		findings[0].GetErrorType())
	require.Equal(t, "ledger", findings[0].GetLedger())
	require.True(t, strings.Contains(findings[0].GetMessage(), "stored Id diverges"),
		"unexpected finding: %s", findings[0].GetMessage())
}
