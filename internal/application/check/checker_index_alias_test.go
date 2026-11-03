package check

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func fsmCreateIndexOrder(id *commonpb.IndexID) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{Id: id}},
			}},
		},
	}}
}

// TestCheckerFSMRejectsNoncanonicalIndexKeyAlias moves an FSM-written
// registration to a key whose ledger-name padding carries a nonzero byte after
// the first NUL. The alias decodes to the same IndexKey, but canonical point
// lookups miss it, so the audited registration is effectively absent.
func TestCheckerFSMRejectsNoncanonicalIndexKeyAlias(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	canonical := domain.IndexKey{LedgerName: "ledger", Canonical: indexes.Canonical(id)}.Bytes()

	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1,
		fsmPurgeCreateLedgerOrder(commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL))
	applyFSMPurgeProposal(t, machine, store, attrs, 2, fsmCreateIndexOrder(id))
	require.NoError(t, store.Flush())
	require.Empty(t, runFSMPurgeCheck(t, store, attrs), "the FSM-produced registry must pass")

	alias := bytes.Clone(canonical)
	alias[len("ledger")+1] = 'x'

	var decoded domain.IndexKey
	require.NoError(t, decoded.Unmarshal(alias))
	require.Equal(t, domain.IndexKey{LedgerName: "ledger", Canonical: indexes.Canonical(id)}, decoded,
		"the alias must decode to the canonical registration's logical key")

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	row, err := attrs.Index.Get(handle, canonical)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.NoError(t, handle.Close())

	batch := store.OpenWriteSession()
	require.NoError(t, attrs.Index.Delete(batch, canonical))
	_, err = attrs.Index.Set(batch, alias, row)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	handle, err = store.NewDirectReadHandle()
	require.NoError(t, err)
	missing, err := attrs.Index.Get(handle, canonical)
	require.NoError(t, err)
	require.Nil(t, missing, "the canonical point lookup must miss the moved registration")
	require.NoError(t, handle.Close())

	mismatches := errorsOfType(runFSMPurgeCheck(t, store, attrs),
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH)
	require.NotEmpty(t, mismatches, "a noncanonical alias must not satisfy the audited registration")

	var aliasFlagged, canonicalMissing bool
	for _, m := range mismatches {
		require.Equal(t, "ledger", m.GetLedger())
		switch {
		case strings.Contains(m.GetMessage(), "noncanonical"):
			aliasFlagged = true
		case strings.Contains(m.GetMessage(), "registry has no matching row"):
			canonicalMissing = true
		}
	}
	require.True(t, aliasFlagged, "the alias row must be reported")
	require.True(t, canonicalMissing, "the missing canonical registration must be reported")
}
