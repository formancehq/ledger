package celrewrite

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func rules(rs ...*commonpb.MirrorRewriteRule) []*commonpb.MirrorRewriteRule {
	return rs
}

func rule(match, cel string, stop bool) *commonpb.MirrorRewriteRule {
	return &commonpb.MirrorRewriteRule{Match: match, Cel: cel, Stop: stop}
}

func createdEntry(logID uint64, txID uint64, postings []*commonpb.Posting, meta map[string]string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{
		V2LogId: logID,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId: txID,
				Postings:      postings,
				Metadata:      stringsToMetadata(meta),
			},
		},
	}
}

func posting(src, dst, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      src,
		Destination: dst,
		Asset:       asset,
		Amount:      &commonpb.Uint256{V0: amount},
	}
}

func TestNewRewriter_NilOnEmpty(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(nil)
	require.NoError(t, err)
	require.Nil(t, r)

	// nil-safe Apply
	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 100)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Same(t, entry, out)
}

func TestRewriteAddress(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress(":worker:\\d+", "")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{
		posting("payments:acme:worker:001:main", "bank:worker:042", "USD", 100),
	}, nil)

	out, err := r.Apply(entry)
	require.NoError(t, err)

	p := out.GetCreatedTransaction().GetPostings()[0]
	require.Equal(t, "payments:acme:main", p.GetSource())
	require.Equal(t, "bank", p.GetDestination())
	// amount/asset untouched
	require.Equal(t, "USD", p.GetAsset())
	require.Equal(t, uint64(100), p.GetAmount().GetV0())
}

func TestMatchSelectsAndSetMetadata(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule(`tx.metadata["type"] == "payout"`, `tx.setMetadata("category", "external")`, false),
	))
	require.NoError(t, err)

	// matching
	e1 := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, map[string]string{"type": "payout"})
	o1, err := r.Apply(e1)
	require.NoError(t, err)
	require.Equal(t, "external", o1.GetCreatedTransaction().GetMetadata()["category"].GetStringValue())

	// non-matching: metadata unchanged
	e2 := createdEntry(2, 2, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, map[string]string{"type": "fee"})
	o2, err := r.Apply(e2)
	require.NoError(t, err)
	_, has := o2.GetCreatedTransaction().GetMetadata()["category"]
	require.False(t, has)
}

func TestSequentialChainingAndStop(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `tx.setMetadata("a", "1")`, false),
		rule(`tx.metadata["a"] == "1"`, `tx.setMetadata("b", "2")`, true), // stop
		rule("true", `tx.setMetadata("c", "3")`, false),                   // must not run
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)

	md := out.GetCreatedTransaction().GetMetadata()
	require.Equal(t, "1", md["a"].GetStringValue())
	require.Equal(t, "2", md["b"].GetStringValue())
	_, hasC := md["c"]
	require.False(t, hasC, "rule after stop must not run")
}

func TestDropBecomesFillGapWithTxID(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule(`tx.metadata["drop"] == "1"`, `tx.drop()`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(7, 42, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, map[string]string{"drop": "1"})
	out, err := r.Apply(entry)
	require.NoError(t, err)

	gap := out.GetFillGap()
	require.NotNil(t, gap, "dropped tx must become a fill-gap")
	require.Equal(t, uint64(7), out.GetV2LogId())
	require.Equal(t, []uint64{42}, gap.GetSkippedTransactionIds())
}

func TestInvalidAddressRejected(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		// rewrite that produces an empty/invalid address
		rule("true", `tx.rewriteAddress("^world$", "bad address with spaces")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
}

func TestAccountMetadataCollisionMerge(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress(":worker:\\d+", "")`, false),
	))
	require.NoError(t, err)

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 1,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId: 1,
				Postings:      []*commonpb.Posting{posting("world", "bank", "USD", 1)},
				AccountMetadata: map[string]*commonpb.MetadataMap{
					"acct:worker:001": {Values: stringsToMetadata(map[string]string{"k": "v1"})},
					"acct:worker:002": {Values: stringsToMetadata(map[string]string{"k": "v2", "x": "y"})},
				},
			},
		},
	}

	out, err := r.Apply(entry)
	require.NoError(t, err)

	am := out.GetCreatedTransaction().GetAccountMetadata()
	require.Len(t, am, 1)
	require.Contains(t, am, "acct")
	merged := am["acct"].GetValues()
	// last writer wins deterministically (sorted: 001 then 002 -> 002 wins on "k")
	require.Equal(t, "v2", merged["k"].GetStringValue())
	require.Equal(t, "y", merged["x"].GetStringValue())
}

func TestNonDeterministicFunctionRejected(t *testing.T) {
	t.Parallel()

	// `now` / timestamp-of-current-time is not registered; compilation must fail.
	_, err := NewRewriter(rules(
		rule(`now() > timestamp("2020-01-01T00:00:00Z")`, `tx`, false),
	))
	require.Error(t, err)
}

func TestBadExpressionRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(rule("this is not cel", `tx`, false)))
	require.Error(t, err)

	// match must be boolean
	_, err = NewRewriter(rules(rule(`"a string"`, `tx`, false)))
	require.Error(t, err)

	// cel must return a transaction
	_, err = NewRewriter(rules(rule("true", `"not a tx"`, false)))
	require.Error(t, err)
}

func TestMatchOnMissingKeyDoesNotStall(t *testing.T) {
	t.Parallel()

	// A match indexing a metadata key the tx doesn't have must NOT fail the
	// batch (which would stall the mirror). The rule simply doesn't fire.
	r, err := NewRewriter(rules(
		rule(`tx.metadata["skip"] == "yes"`, `tx.drop()`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, map[string]string{"other": "x"})
	out, err := r.Apply(entry)
	require.NoError(t, err)
	// Not dropped, unchanged.
	require.NotNil(t, out.GetCreatedTransaction())
	require.Equal(t, uint64(1), out.GetCreatedTransaction().GetTransactionId())
}

func TestPostingCountChangeRejected(t *testing.T) {
	t.Parallel()

	// A hand-built TxView literal that changes the posting count must be
	// rejected rather than silently mis-aligning addresses with the wrong
	// amounts. An empty literal has zero postings.
	r, err := NewRewriter(rules(
		rule("true", `celrewrite.TxView{}`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "posting count")
}

func TestEmptyRegexPatternRejected(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress("", "x")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
}

func TestDeterministicOutput(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress(":worker:\\d+", "").setMetadata("mirrored", "true")`, false),
	))
	require.NoError(t, err)

	build := func() *raftcmdpb.MirrorLogEntry {
		return createdEntry(1, 1, []*commonpb.Posting{
			posting("a:worker:001", "b:worker:002", "USD", 5),
		}, map[string]string{"k": "v"})
	}

	o1, err := r.Apply(build())
	require.NoError(t, err)
	o2, err := r.Apply(build())
	require.NoError(t, err)

	// Semantic equality: identical input yields the identical rewritten entry.
	// (Byte-equality is deliberately not asserted — vtproto marshals proto maps
	// in Go map-iteration order, so raw bytes vary for equal maps. Determinism
	// across nodes holds because the leader marshals each proposal exactly once;
	// followers apply those bytes verbatim.)
	require.True(t, o1.EqualVT(o2))
}
