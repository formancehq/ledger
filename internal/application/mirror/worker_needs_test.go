package mirror

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// mirrorOrder wraps a MirrorLogEntry into a LedgerScoped MirrorIngest order for
// ledger "L".
func mirrorOrder(entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: entry},
				},
			},
		},
	}
}

func hasAccountNeed(needs *plan.Needs, account string) bool {
	_, ok := needs.Accounts[domain.AccountKey{LedgerName: "L", Account: account}]

	return ok
}

// TestExtractMirrorNeeds_DeclaresAccountMarkerNeeds is the coverage-gate binding
// test for EN-1276 Option B (PR #564 finding [A]). The mirror apply path writes
// the per-account existence marker via the gated Scope.GetAccount/PutAccount, so
// extractMirrorNeeds MUST declare a SubAttrAccount need for exactly the accounts
// that path marks — CreatedTransaction postings + account-metadata accounts, and
// a SavedMetadata account target — or apply hits a coverage-gate miss (a loud FSM
// failure on every node, invariants #6/#9). Reverts touch only pre-existing
// accounts (already marked by the original ingest), so they declare none.
func TestExtractMirrorNeeds_DeclaresAccountMarkerNeeds(t *testing.T) {
	t.Parallel()

	w := &Worker{ledgerName: "L"}

	t.Run("created transaction: postings + account metadata", func(t *testing.T) {
		t.Parallel()

		order := mirrorOrder(&raftcmdpb.MirrorLogEntry{
			Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
				CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
					TransactionId: 1,
					Postings: []*commonpb.Posting{
						{Source: "world", Destination: "users:001", Asset: "USD", Amount: commonpb.NewUint256FromUint64(100)},
					},
					AccountMetadata: map[string]*commonpb.MetadataMap{
						"users:002": {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}},
					},
				},
			},
		})

		aggregate, _ := w.extractMirrorNeeds(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{order}})

		require.True(t, hasAccountNeed(aggregate, "users:001"), "posting destination must be declared")
		require.True(t, hasAccountNeed(aggregate, "world"), "posting source must be declared (over-declaring world is harmless)")
		require.True(t, hasAccountNeed(aggregate, "users:002"), "account-metadata-only account must be declared")
	})

	t.Run("saved metadata: account target", func(t *testing.T) {
		t.Parallel()

		order := mirrorOrder(&raftcmdpb.MirrorLogEntry{
			Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
				SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
					Target: &commonpb.Target{Target: &commonpb.Target_Account{
						Account: &commonpb.TargetAccount{Addr: "users:003"},
					}},
					Metadata: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")},
				},
			},
		})

		aggregate, _ := w.extractMirrorNeeds(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{order}})

		require.True(t, hasAccountNeed(aggregate, "users:003"), "saved-metadata account target must be declared")
	})

	t.Run("reverted transaction: no account needs", func(t *testing.T) {
		t.Parallel()

		order := mirrorOrder(&raftcmdpb.MirrorLogEntry{
			Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
				RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
					RevertedTransactionId: 1,
					NewTransactionId:      2,
					ReversePostings: []*commonpb.Posting{
						{Source: "users:001", Destination: "world", Asset: "USD", Amount: commonpb.NewUint256FromUint64(100)},
					},
				},
			},
		})

		aggregate, _ := w.extractMirrorNeeds(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{order}})

		require.Empty(t, aggregate.Accounts, "revert apply marks no account, so no account need is declared")
	})
}
