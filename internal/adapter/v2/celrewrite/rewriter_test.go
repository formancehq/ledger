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

func TestViewConstructionRejected(t *testing.T) {
	t.Parallel()

	// Constructing the internal view types in CEL is rejected at compile time —
	// a rule must derive its result from tx via the helpers. This closes the
	// literal-based bypass of the posting-count, account-target and metadata
	// guarantees.
	cases := []string{
		`celrewrite.TxView{}`,                                 // zero postings
		`celrewrite.TxView{metadata: {"bad key": "v"}}`,       // unvalidated metadata
		`celrewrite.TxView{postings: [celrewrite.Posting{}]}`, // nested Posting literal
	}

	for _, expr := range cases {
		_, err := NewRewriter(rules(rule("true", expr, false)))
		require.Error(t, err, expr)
		require.Contains(t, err.Error(), "is not allowed", expr)
	}
}

func TestEmptyRegexPatternRejectedAtRuntime(t *testing.T) {
	t.Parallel()

	// A computed (non-literal) empty pattern can't be caught at compile time, so
	// it must fail loudly when the rule runs.
	r, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress("x".substring(1), "y")`, false), // "" at runtime
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
}

func savedMetadataEntry(logID uint64, account string, meta map[string]string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{
		V2LogId: logID,
		Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
			SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
				Target: &commonpb.Target{
					Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}},
				},
				Metadata: stringsToMetadata(meta),
			},
		},
	}
}

func TestEmptyRewrittenAccountTargetRejected(t *testing.T) {
	t.Parallel()

	// Rewriting an account target down to "" must fail the batch, not be
	// silently treated as an absent (transaction-level) target.
	r, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress(".+", "")`, false),
	))
	require.NoError(t, err)

	entry := savedMetadataEntry(1, "users:acme", map[string]string{"k": "v"})
	_, err = r.Apply(entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target")
}

func TestInvalidLiteralRegexRejectedAtCompile(t *testing.T) {
	t.Parallel()

	// A malformed literal regex must be rejected when the rule is compiled
	// (admission), not deferred to a runtime batch failure.
	_, err := NewRewriter(rules(
		rule("true", `tx.rewriteAddress("(", "")`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "rewriteAddress pattern")

	// Empty literal pattern is likewise rejected up-front.
	_, err = NewRewriter(rules(
		rule("true", `tx.rewriteAddress("", "x")`, false),
	))
	require.Error(t, err)
}

func TestSetMetadataValidatesKeyAndValue(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `tx.setMetadata("bad key", "v")`, false), // space is not a valid key char
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata key")
}

func TestSetAccountMetadataValidatesValue(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", "tx.setAccountMetadata(\"users:001\", \"k\", \"bad\\x00value\")", false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "users:001", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata value")
}

func TestAnnotateAccounts(t *testing.T) {
	t.Parallel()

	// Capture the last segment of matching acquirer addresses into account
	// metadata: acquirer:Stripe_NL:worker:001:bank -> acquirer-type=bank.
	r, err := NewRewriter(rules(
		rule("true", `tx.annotateAccounts("^acquirer:Stripe_NL:worker:\\d+:([^:]+)$", "acquirer-type", "$1")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{
		posting("world", "acquirer:Stripe_NL:worker:001:bank", "USD", 100),
		posting("acquirer:Stripe_NL:worker:002:fees", "users:alice", "USD", 5),
		posting("world", "users:bob", "USD", 1), // no match — untouched
	}, nil)

	out, err := r.Apply(entry)
	require.NoError(t, err)

	am := out.GetCreatedTransaction().GetAccountMetadata()
	require.Equal(t, "bank", am["acquirer:Stripe_NL:worker:001:bank"].GetValues()["acquirer-type"].GetStringValue())
	require.Equal(t, "fees", am["acquirer:Stripe_NL:worker:002:fees"].GetValues()["acquirer-type"].GetStringValue())
	// Non-matching accounts are not annotated.
	require.NotContains(t, am, "users:bob")
	require.NotContains(t, am, "world")
}

func TestAnnotateAccounts_InvalidKeyRejectedAtRuntime(t *testing.T) {
	t.Parallel()

	// A metadata key with a disallowed character ('/') must fail the rule.
	r, err := NewRewriter(rules(
		rule("true", `tx.annotateAccounts("^(.+)$", "bad/key", "$1")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata key")
}

func TestAnnotateAccounts_InvalidLiteralRegexRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `tx.annotateAccounts("(", "k", "$1")`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "annotateAccounts pattern")
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
