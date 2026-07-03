package replication

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
)

// dropWorker removes a ":worker:<digits>" segment from an address, the canonical
// lock-avoidance segment we want stripped when mirroring.
var dropWorkerRule = ledger.AddressRewriteRule{Pattern: `(:worker:\d+)`, Replacement: ""}

func newRewriter(t *testing.T, rules ...ledger.AddressRewriteRule) *AddressRewriter {
	t.Helper()
	rewriter, err := NewAddressRewriter(rules)
	require.NoError(t, err)
	return rewriter
}

func TestNewAddressRewriter(t *testing.T) {
	t.Parallel()

	t.Run("no rules is a nil pass-through", func(t *testing.T) {
		t.Parallel()
		rewriter, err := NewAddressRewriter(nil)
		require.NoError(t, err)
		require.Nil(t, rewriter)
	})

	t.Run("invalid regexp errors", func(t *testing.T) {
		t.Parallel()
		_, err := NewAddressRewriter([]ledger.AddressRewriteRule{{Pattern: "("}})
		require.Error(t, err)
	})
}

func TestAddressRewriterNilPassthrough(t *testing.T) {
	t.Parallel()

	var rewriter *AddressRewriter
	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "payments:acme:worker:001:main", "USD", big.NewInt(100)),
		),
	})

	out, err := rewriter.Apply(log)
	require.NoError(t, err)
	require.Equal(t, log, out)
}

func TestAddressRewriterCreatedTransaction(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, dropWorkerRule)

	tx := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "payments:acme:worker:001:main", "USD", big.NewInt(100)),
		).
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"world":                              {"USD": ledger.NewVolumesInt64(0, 100)},
			"payments:acme:worker:001:main": {"USD": ledger.NewVolumesInt64(100, 0)},
		}).
		WithPostCommitEffectiveVolumes(ledger.PostCommitVolumes{
			"world":                              {"USD": ledger.NewVolumesInt64(0, 100)},
			"payments:acme:worker:001:main": {"USD": ledger.NewVolumesInt64(100, 0)},
		})

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: tx,
		AccountMetadata: ledger.AccountMetadata{
			"payments:acme:worker:001:main": metadata.Metadata{"kind": "bank"},
		},
	})

	out, err := rewriter.Apply(log)
	require.NoError(t, err)

	data := out.Data.(ledger.CreatedTransaction)
	require.Equal(t, "payments:acme:main", data.Transaction.Postings[0].Destination)
	require.Equal(t, "world", data.Transaction.Postings[0].Source)

	require.Contains(t, data.Transaction.PostCommitVolumes, "payments:acme:main")
	require.NotContains(t, data.Transaction.PostCommitVolumes, "payments:acme:worker:001:main")
	require.Contains(t, data.Transaction.PostCommitEffectiveVolumes, "payments:acme:main")

	require.Contains(t, data.AccountMetadata, "payments:acme:main")
	require.Equal(t, metadata.Metadata{"kind": "bank"}, data.AccountMetadata["payments:acme:main"])

	// Marshaling exercises Transaction.MarshalJSON -> SubtractPostings, which looks
	// up the (rewritten) posting addresses in the volume maps. If the volume keys
	// were left stale this would panic; assert it does not.
	require.NotPanics(t, func() {
		_, err := json.Marshal(out.Data)
		require.NoError(t, err)
	})
}

func TestAddressRewriterDoesNotMutateOriginal(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, dropWorkerRule)

	original := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "payments:acme:worker:001:main", "USD", big.NewInt(100))).
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"payments:acme:worker:001:main": {"USD": ledger.NewVolumesInt64(100, 0)},
		})
	log := ledger.NewLog(ledger.CreatedTransaction{Transaction: original})

	_, err := rewriter.Apply(log)
	require.NoError(t, err)

	// The source log must be untouched — it backs the pull cursor.
	require.Equal(t, "payments:acme:worker:001:main", original.Postings[0].Destination)
	require.Contains(t, original.PostCommitVolumes, "payments:acme:worker:001:main")
}

func TestAddressRewriterRevertedTransaction(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, dropWorkerRule)

	reverted := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "payments:acme:worker:001:main", "USD", big.NewInt(100)),
	)
	revert := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("payments:acme:worker:001:main", "world", "USD", big.NewInt(100)),
	)

	log := ledger.NewLog(ledger.RevertedTransaction{
		RevertedTransaction: reverted,
		RevertTransaction:   revert,
	})

	out, err := rewriter.Apply(log)
	require.NoError(t, err)

	data := out.Data.(ledger.RevertedTransaction)
	require.Equal(t, "payments:acme:main", data.RevertedTransaction.Postings[0].Destination)
	require.Equal(t, "payments:acme:main", data.RevertTransaction.Postings[0].Source)
}

func TestAddressRewriterSavedMetadata(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, dropWorkerRule)

	t.Run("account target is rewritten", func(t *testing.T) {
		t.Parallel()
		log := ledger.NewLog(ledger.SavedMetadata{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   "payments:acme:worker:001:main",
			Metadata:   metadata.Metadata{"k": "v"},
		})
		out, err := rewriter.Apply(log)
		require.NoError(t, err)
		require.Equal(t, "payments:acme:main", out.Data.(ledger.SavedMetadata).TargetID)
	})

	t.Run("transaction target is untouched", func(t *testing.T) {
		t.Parallel()
		log := ledger.NewLog(ledger.SavedMetadata{
			TargetType: ledger.MetaTargetTypeTransaction,
			TargetID:   uint64(42),
			Metadata:   metadata.Metadata{"k": "v"},
		})
		out, err := rewriter.Apply(log)
		require.NoError(t, err)
		require.Equal(t, uint64(42), out.Data.(ledger.SavedMetadata).TargetID)
	})
}

func TestAddressRewriterDeletedMetadata(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, dropWorkerRule)

	log := ledger.NewLog(ledger.DeletedMetadata{
		TargetType: ledger.MetaTargetTypeAccount,
		TargetID:   "payments:acme:worker:001:main",
		Key:        "kind",
	})
	out, err := rewriter.Apply(log)
	require.NoError(t, err)
	require.Equal(t, "payments:acme:main", out.Data.(ledger.DeletedMetadata).TargetID)
}

func TestAddressRewriterRename(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, ledger.AddressRewriteRule{Pattern: `^payments:`, Replacement: "psp:"})

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "payments:acme:main", "USD", big.NewInt(100)),
		),
	})
	out, err := rewriter.Apply(log)
	require.NoError(t, err)
	require.Equal(t, "psp:acme:main", out.Data.(ledger.CreatedTransaction).Transaction.Postings[0].Destination)
}

func TestAddressRewriterCollisionMerges(t *testing.T) {
	t.Parallel()

	rewriter := newRewriter(t, dropWorkerRule)

	// worker:001 and worker:002 collapse onto the same account.
	tx := ledger.NewTransaction().
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"payments:acme:worker:001:main": {"USD": ledger.NewVolumesInt64(100, 10)},
			"payments:acme:worker:002:main": {"USD": ledger.NewVolumesInt64(50, 5)},
		})

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: tx,
		AccountMetadata: ledger.AccountMetadata{
			"payments:acme:worker:001:main": metadata.Metadata{"kind": "bank", "shard": "001"},
			"payments:acme:worker:002:main": metadata.Metadata{"shard": "002"},
		},
	})

	out, err := rewriter.Apply(log)
	require.NoError(t, err)
	data := out.Data.(ledger.CreatedTransaction)

	// Volumes are summed per asset.
	merged := data.Transaction.PostCommitVolumes["payments:acme:main"]["USD"]
	require.Equal(t, big.NewInt(150), merged.Input)
	require.Equal(t, big.NewInt(15), merged.Output)

	// Metadata is merged; on a key conflict the lexicographically-smallest original
	// address (worker:001) wins.
	require.Equal(t, "bank", data.AccountMetadata["payments:acme:main"]["kind"])
	require.Equal(t, "001", data.AccountMetadata["payments:acme:main"]["shard"])
}

func TestAddressRewriterInvalidResultErrors(t *testing.T) {
	t.Parallel()

	// A rule that drops every character produces an empty (invalid) address.
	rewriter := newRewriter(t, ledger.AddressRewriteRule{Pattern: `.+`, Replacement: ""})

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "payments:acme:main", "USD", big.NewInt(100)),
		),
	})
	_, err := rewriter.Apply(log)
	require.Error(t, err)
}
