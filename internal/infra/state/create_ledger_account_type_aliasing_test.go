package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// createLedgerOrderWithAccountTypes builds a CreateLedger order whose embedded
// AccountType.Name values deliberately do NOT match their map keys (one blank,
// one mismatched), so the test can prove the FSM preserves the submitted bytes
// verbatim while canonicalizing derived state to the map key.
func createLedgerOrderWithAccountTypes(name string, accountTypes map[string]*commonpb.AccountType) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{
						AccountTypes: accountTypes,
					},
				},
			},
		},
	}
}

func readAuditItemsForSequence(t *testing.T, ctx context.Context, store *dal.Store, seq uint64) []*auditpb.AuditItem {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	items, err := query.ReadAuditItems(ctx, handle, seq)
	require.NoError(t, err)

	return items
}

// TestCreateLedger_AuditPreservesSubmittedAccountTypeNames pins EN-1533 at the
// FSM/audit layer: with an idempotency key on the batch, the audited order bytes
// must preserve the SUBMITTED embedded account-type names (blank / mismatched),
// while the derived created-ledger log canonicalizes each name to its map key.
// A replay of the identical keyed proposal must NOT surface an idempotency
// conflict — the pre-processing frozen outcome hash (over the accepted order)
// stays consistent with what the audit chain records.
func TestCreateLedger_AuditPreservesSubmittedAccountTypeNames(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "aliasing"

	order := createLedgerOrderWithAccountTypes(ledgerName, map[string]*commonpb.AccountType{
		"canonical-a": {Name: "", Pattern: "a:{id}"},
		"canonical-b": {Name: "wrong-embedded-name", Pattern: "b:{id}"},
	})

	proposal := makeProposal(2, order)
	proposal.Idempotency = &commonpb.Idempotency{Key: "create-key"}

	r, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, proposal))
	require.NoError(t, err)
	require.Len(t, r.Results, 1)
	require.NoError(t, r.Results[0].Error)
	require.Len(t, r.Results[0].Logs, 1)

	// Derived state: the created-ledger log canonicalizes each name to the map
	// key (LedgerInfo is built from the same canonical map).
	createLog := r.Results[0].Logs[0].GetCreatedLog().GetPayload().GetCreateLedger()
	require.NotNil(t, createLog)
	require.Equal(t, "canonical-a", createLog.GetAccountTypes()["canonical-a"].GetName())
	require.Equal(t, "canonical-b", createLog.GetAccountTypes()["canonical-b"].GetName())

	// The in-memory order we submitted must be untouched (no aliasing back
	// through the CreateLedgerOrder we handed the machine).
	require.Equal(t, "", order.GetLedgerScoped().GetCreateLedger().GetAccountTypes()["canonical-a"].GetName())
	require.Equal(t, "wrong-embedded-name", order.GetLedgerScoped().GetCreateLedger().GetAccountTypes()["canonical-b"].GetName())

	// A second, distinct apply flushes the first proposal's audit entry (and its
	// items) to the durable read path.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, makeProposal(3, createLedgerOrder("other"))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	// Audited order bytes: the SerializedOrder captured into the hash chain must
	// unmarshal back to the exact submitted embedded names — proving the accepted
	// order was never mutated before audit capture.
	items := readAuditItemsForSequence(t, ctx, dataStore, 1)
	require.Len(t, items, 1)

	var audited raftcmdpb.Order
	require.NoError(t, audited.UnmarshalVT(items[0].GetSerializedOrder()))
	auditedTypes := audited.GetLedgerScoped().GetCreateLedger().GetAccountTypes()
	require.Equal(t, "", auditedTypes["canonical-a"].GetName(),
		"audited order must preserve the submitted blank embedded name")
	require.Equal(t, "wrong-embedded-name", auditedTypes["canonical-b"].GetName(),
		"audited order must preserve the submitted mismatched embedded name")

	// Replaying the identical keyed proposal must replay cleanly, with no
	// idempotency conflict: the frozen outcome hash re-derived from the accepted
	// order agrees with what the chain recorded.
	replayOrder := createLedgerOrderWithAccountTypes(ledgerName, map[string]*commonpb.AccountType{
		"canonical-a": {Name: "", Pattern: "a:{id}"},
		"canonical-b": {Name: "wrong-embedded-name", Pattern: "b:{id}"},
	})
	replay := makeProposal(4, replayOrder)
	replay.Idempotency = &commonpb.Idempotency{Key: "create-key"}

	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 3, replay))
	require.NoError(t, err)
	require.Len(t, r.Results, 1)
	require.NoError(t, r.Results[0].Error, "identical keyed replay must not surface an idempotency conflict")
}
