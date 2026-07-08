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

func posting(src, dst, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      src,
		Destination: dst,
		Asset:       asset,
		Amount:      &commonpb.Uint256{V0: amount},
	}
}

func createdEntry(logID, txID uint64, postings []*commonpb.Posting, meta map[string]string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{
		V2LogId: logID,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId: txID,
				Postings:      postings,
				Metadata:      stringsToMetadata(meta, nil),
			},
		},
	}
}

func revertedEntry(logID, revertedID, newID uint64, postings []*commonpb.Posting, meta map[string]string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{
		V2LogId: logID,
		Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
			RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
				RevertedTransactionId: revertedID,
				NewTransactionId:      newID,
				ReversePostings:       postings,
				Metadata:              stringsToMetadata(meta, nil),
			},
		},
	}
}

func savedMetadataEntry(logID uint64, account string, meta map[string]string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{
		V2LogId: logID,
		Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
			SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
				Target:   &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}}},
				Metadata: stringsToMetadata(meta, nil),
			},
		},
	}
}

func deletedMetadataEntry(logID uint64, account, key string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{
		V2LogId: logID,
		Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{
			DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{
				Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}}},
				Key:    key,
			},
		},
	}
}

func TestNewRewriter_NilOnEmpty(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(nil)
	require.NoError(t, err)
	require.Nil(t, r)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 100)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Same(t, entry, out)
}

func TestRewriteAddress(t *testing.T) {
	t.Parallel()

	// rewriteAddress is a cross-cutting op on log (works on any variant).
	r, err := NewRewriter(rules(
		rule("true", `log.rewriteAddress(":worker:\\d+", "")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("orders:worker:42", "bank", "USD", 100)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Equal(t, "orders", out.GetCreatedTransaction().GetPostings()[0].GetSource())
}

func TestMatchSelectsAndSetMetadata(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule(`has(log.created) && "kind" in log.created.metadata && log.created.metadata["kind"] == "payout"`,
			`log.withCreated(log.created.setMetadata("category", "external"))`, false),
	))
	require.NoError(t, err)

	match := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, map[string]string{"kind": "payout"})
	out, err := r.Apply(match)
	require.NoError(t, err)
	require.Equal(t, "external", out.GetCreatedTransaction().GetMetadata()["category"].GetStringValue())

	noMatch := createdEntry(2, 2, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, map[string]string{"kind": "fee"})
	out2, err := r.Apply(noMatch)
	require.NoError(t, err)
	require.NotContains(t, out2.GetCreatedTransaction().GetMetadata(), "category")
}

func TestSequentialChainingAndStop(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("a", "1"))`, false),
		rule("true", `log.withCreated(log.created.setMetadata("b", "2"))`, true),
		rule("true", `log.withCreated(log.created.setMetadata("c", "3"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)

	md := out.GetCreatedTransaction().GetMetadata()
	require.Equal(t, "1", md["a"].GetStringValue())
	require.Equal(t, "2", md["b"].GetStringValue())
	require.NotContains(t, md, "c") // stopped after rule 2
}

func TestDropBecomesFillGapWithTxID(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.drop()`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(7, 42, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)

	gap := out.GetFillGap()
	require.NotNil(t, gap)
	require.Equal(t, uint64(7), out.GetV2LogId())
	require.Equal(t, []uint64{42}, gap.GetSkippedTransactionIds())
}

func TestInvalidAddressRejected(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.rewriteAddress("bank", "bad address")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
}

func TestAccountMetadataCollisionMerge(t *testing.T) {
	t.Parallel()

	// Two accounts collapsing to the same address merge deterministically.
	r, err := NewRewriter(rules(
		rule("true", `log.rewriteAddress(":shard:\\d+", "")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	entry.GetCreatedTransaction().AccountMetadata = map[string]*commonpb.MetadataMap{
		"acct:shard:1": {Values: stringsToMetadata(map[string]string{"a": "1"}, nil)},
		"acct:shard:2": {Values: stringsToMetadata(map[string]string{"b": "2"}, nil)},
	}

	out, err := r.Apply(entry)
	require.NoError(t, err)
	am := out.GetCreatedTransaction().GetAccountMetadata()
	require.Contains(t, am, "acct")
	require.NotContains(t, am, "acct:shard:1")
}

func TestUnknownFunctionRejected(t *testing.T) {
	t.Parallel()

	// No non-deterministic function is registered; an unknown call fails compile.
	_, err := NewRewriter(rules(rule("true", `log.withCreated(log.created.setMetadata("t", string(now())))`, false)))
	require.Error(t, err)
}

func TestBadExpressionRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(rule("true", `log.(`, false)))
	require.Error(t, err)

	// cel that does not evaluate to a log entry.
	_, err = NewRewriter(rules(rule("true", `"a string"`, false)))
	require.Error(t, err)

	// match that is not a bool.
	_, err = NewRewriter(rules(rule(`log`, `log`, false)))
	require.Error(t, err)
}

func TestMatchOnMissingKeyDoesNotStall(t *testing.T) {
	t.Parallel()

	// Indexing a missing metadata key errors at runtime; the entry is skipped,
	// not the whole batch.
	r, err := NewRewriter(rules(
		rule(`has(log.created) && log.created.metadata["missing"] == "x"`,
			`log.withCreated(log.created.setMetadata("touched", "yes"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.NotContains(t, out.GetCreatedTransaction().GetMetadata(), "touched")
}

func TestViewConstructionRejected(t *testing.T) {
	t.Parallel()

	// Constructing the native view types is rejected — a rule must derive its
	// result from log via the helpers, never fabricate an entry or variant.
	cases := []string{
		`celrewrite.Log{}`,
		`celrewrite.CreatedView{}`,
		`celrewrite.Log{created: celrewrite.CreatedView{}}`,
		`log.withCreated(celrewrite.CreatedView{})`,
	}

	for _, expr := range cases {
		_, err := NewRewriter(rules(rule("true", expr, false)))
		require.Error(t, err, expr)
		require.Contains(t, err.Error(), "is not allowed", expr)
	}
}

func TestNonConstantRegexPatternRejected(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `log.rewriteAddress("x".substring(1), "y")`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "pattern must be a constant")
}

func TestInvalidLiteralRegexRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(rule("true", `log.rewriteAddress("(", "")`, false)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "rewriteAddress pattern")

	_, err = NewRewriter(rules(rule("true", `log.rewriteAddress("", "x")`, false)))
	require.Error(t, err)
}

func TestEmptyRewrittenAccountTargetRejected(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.rewriteAddress(".+", "")`, false),
	))
	require.NoError(t, err)

	entry := savedMetadataEntry(1, "users:acme", map[string]string{"k": "v"})
	_, err = r.Apply(entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target")
}

func TestSetMetadataLiteralKeyRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("bad key", "v"))`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata key")
}

func TestSetAccountMetadataLiteralValueRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", "log.withCreated(log.created.setAccountMetadata(\"users:001\", \"k\", \"bad\\x00value\"))", false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata value")
}

func TestSetAccountMetadataFromAddress(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadataFromAddress("^acquirer:acme:worker:\\d+:([^:]+)$", "acquirer-type", "$1"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{
		posting("world", "acquirer:acme:worker:001:bank", "USD", 100),
		posting("acquirer:acme:worker:002:fees", "users:alice", "USD", 5),
		posting("world", "users:bob", "USD", 1),
	}, nil)

	out, err := r.Apply(entry)
	require.NoError(t, err)

	am := out.GetCreatedTransaction().GetAccountMetadata()
	require.Equal(t, "bank", am["acquirer:acme:worker:001:bank"].GetValues()["acquirer-type"].GetStringValue())
	require.Equal(t, "fees", am["acquirer:acme:worker:002:fees"].GetValues()["acquirer-type"].GetStringValue())
	require.NotContains(t, am, "users:bob")
}

func TestSetAccountMetadataFromAddress_InvalidKeyRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadataFromAddress("^(.+)$", "bad key", "$1"))`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata key")
}

func TestSetAccountMetadataFromAddress_InvalidLiteralRegexRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadataFromAddress("(", "k", "$1"))`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "setAccountMetadataFromAddress pattern")
}

func TestTypedMetadata(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("count", "42", "int64"))`, false),
		rule("true", `log.withCreated(log.created.setMetadata("flag", "true", "bool"))`, false),
		rule("true", `log.withCreated(log.created.setMetadata("name", "acme", "string"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)

	md := out.GetCreatedTransaction().GetMetadata()
	require.Equal(t, int64(42), md["count"].GetIntValue())
	require.True(t, md["flag"].GetBoolValue())
	require.Equal(t, "acme", md["name"].GetStringValue())
}

func TestSetAccountMetadataFromAddressTyped(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadataFromAddress("^acquirer:acme:worker:(\\d+):.*$", "worker-id", "$1", "int64"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "acquirer:acme:worker:007:bank", "USD", 100)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)

	v := out.GetCreatedTransaction().GetAccountMetadata()["acquirer:acme:worker:007:bank"].GetValues()["worker-id"]
	require.Equal(t, int64(7), v.GetIntValue())
}

func TestInvalidTypeTokenRejectedAtCompile(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("k", "v", "integer"))`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "setMetadata type")
}

func TestNonConstantTypeTokenRejected(t *testing.T) {
	t.Parallel()

	_, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("k", "v", log.created.metadata["t"]))`, false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a constant")
}

func TestUntypedOverwriteResetsType(t *testing.T) {
	t.Parallel()

	// A typed write followed by an untyped overwrite reverts to string.
	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("n", "42", "int64"))`, false),
		rule("true", `log.withCreated(log.created.setMetadata("n", "hello"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Equal(t, "hello", out.GetCreatedTransaction().GetMetadata()["n"].GetStringValue())
}

func TestRewriteAddressPreservesAccountMetadataType(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadata("acct:1", "n", "5", "int64"))`, false),
		rule("true", `log.rewriteAddress("acct:1", "acct:one")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Equal(t, int64(5), out.GetCreatedTransaction().GetAccountMetadata()["acct:one"].GetValues()["n"].GetIntValue())
}

func TestMapAddress_ReverseSegments(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.mapAddress(a, a.split(":").reverse().join(":"))`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("a:b:c:d", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Equal(t, "d:c:b:a", out.GetCreatedTransaction().GetPostings()[0].GetSource())
	require.Equal(t, "bank", out.GetCreatedTransaction().GetPostings()[0].GetDestination())
}

func TestMapAddress_CoversTargetAndAccountMetadata(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.mapAddress(a, "x:" + a)`, false),
	))
	require.NoError(t, err)

	entry := savedMetadataEntry(1, "users:alice", map[string]string{"k": "v"})
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Equal(t, "x:users:alice", out.GetSavedMetadata().GetTarget().GetAccount().GetAddr())
}

func TestAddressWriteback_NotDirectlyCallable(t *testing.T) {
	t.Parallel()

	// mapAddress is the only address writer; the private writeback is un-typeable.
	for _, expr := range []string{
		`log.setAddresses([])`,
		`log.mapAddress~apply(log.addresses())`,
	} {
		_, err := NewRewriter(rules(rule("true", expr, false)))
		require.Error(t, err, "expr %q must be rejected at compile time", expr)
	}
}

func TestMapAddress_InvalidResultRejected(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.mapAddress(a, a + " bad")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	_, err = r.Apply(entry)
	require.Error(t, err)
}

// --- cross-variant safety: compile-time (façade) ----------------------------

func TestCrossVariantMisuseIsCompileError(t *testing.T) {
	t.Parallel()

	// The variant type system rejects a helper the variant cannot persist.
	cases := []string{
		`log.withReverted(log.reverted.setAccountMetadata("a", "k", "v"))`, // reverted has no setAccountMetadata
		`log.withCreated(log.deletedMetadata.setMetadata("k", "v"))`,       // deletedMetadata has no setMetadata
		`log.withCreated(log.reverted)`,                                    // wrap wrong variant
		`log.withReverted(log.created)`,                                    // wrap wrong variant
	}

	for _, expr := range cases {
		_, err := NewRewriter(rules(rule("true", expr, false)))
		require.Error(t, err, expr)
	}
}

// --- cross-variant safety: runtime single-variant invariant -----------------

func TestUnguardedForeignVariantRejectedAtRuntime(t *testing.T) {
	t.Parallel()

	// Accessing a foreign variant without a has() guard fabricates a zero view;
	// wrapping it produces a two-variant entry, rejected loudly at commit.
	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setMetadata("k", "v"))`, false),
	))
	require.NoError(t, err)

	reverted := revertedEntry(1, 1, 2, []*commonpb.Posting{posting("bank", "world", "USD", 1)}, nil)
	_, err = r.Apply(reverted)
	require.Error(t, err)
	require.Contains(t, err.Error(), "may only transform the source reverted variant")
}

func TestGuardedCrossVariantRuleApplies(t *testing.T) {
	t.Parallel()

	// Scoped with has(log.created): applies to created, no-op elsewhere.
	r, err := NewRewriter(rules(
		rule("true", `has(log.created) ? log.withCreated(log.created.setMetadata("seen", "1")) : log`, false),
	))
	require.NoError(t, err)

	created := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(created)
	require.NoError(t, err)
	require.Equal(t, "1", out.GetCreatedTransaction().GetMetadata()["seen"].GetStringValue())

	reverted := revertedEntry(2, 1, 2, []*commonpb.Posting{posting("bank", "world", "USD", 1)}, nil)
	out2, err := r.Apply(reverted)
	require.NoError(t, err)
	require.NotNil(t, out2.GetRevertedTransaction())
}

func TestSetMetadataOnRevertedAndSaved(t *testing.T) {
	t.Parallel()

	// setMetadata is valid on the variants that carry a metadata field.
	r, err := NewRewriter(rules(
		rule("has(log.reverted)", `log.withReverted(log.reverted.setMetadata("r", "1"))`, false),
		rule("has(log.savedMetadata)", `log.withSavedMetadata(log.savedMetadata.setMetadata("s", "1"))`, false),
	))
	require.NoError(t, err)

	reverted := revertedEntry(1, 1, 2, []*commonpb.Posting{posting("bank", "world", "USD", 1)}, nil)
	out, err := r.Apply(reverted)
	require.NoError(t, err)
	require.Equal(t, "1", out.GetRevertedTransaction().GetMetadata()["r"].GetStringValue())

	saved := savedMetadataEntry(2, "users:alice", map[string]string{"a": "b"})
	out2, err := r.Apply(saved)
	require.NoError(t, err)
	require.Equal(t, "1", out2.GetSavedMetadata().GetMetadata()["s"].GetStringValue())
}

func TestDeleteMetadataEntryPassThrough(t *testing.T) {
	t.Parallel()

	// A deleteMetadata entry exposes no metadata helpers; only its address can be
	// rewritten. An identity rule passes it through.
	r, err := NewRewriter(rules(
		rule("true", `log`, false),
	))
	require.NoError(t, err)

	entry := deletedMetadataEntry(1, "users:alice", "old")
	out, err := r.Apply(entry)
	require.NoError(t, err)
	require.Equal(t, "old", out.GetDeletedMetadata().GetKey())
	require.Equal(t, "users:alice", out.GetDeletedMetadata().GetTarget().GetAccount().GetAddr())
}

// TestAccountMetadataCollisionTypeFollowsValue guards the value/type re-key
// lockstep: when two accounts collapse onto one and the winning value is untyped,
// the committed value must be a plain string, not a null coercion inheriting the
// loser's declared type.
func TestAccountMetadataCollisionTypeFollowsValue(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadata("acct:1", "n", "5", "int64"))`, false),
		rule("true", `log.withCreated(log.created.setAccountMetadata("acct:2", "n", "hello"))`, false),
		rule("true", `log.rewriteAddress(":\\d$", "")`, false),
	))
	require.NoError(t, err)

	entry := createdEntry(1, 1, []*commonpb.Posting{posting("world", "bank", "USD", 1)}, nil)
	out, err := r.Apply(entry)
	require.NoError(t, err)

	// Sorted order acct:1, acct:2 → acct:2 ("hello", untyped) wins the "n"
	// collision, so the type must revert to string, not stay int64.
	v := out.GetCreatedTransaction().GetAccountMetadata()["acct"].GetValues()["n"]
	require.Equal(t, "hello", v.GetStringValue())
}

// TestUnguardedVariantReadInPredicate documents (and pins) the has()-guard
// contract: reading a foreign variant without a has() guard yields a zero view
// (CEL oneof semantics), so a predicate over it silently holds. The result is
// still a valid single-variant entry — no corruption — but the rule fires on
// kinds the author may not have intended. Authors must guard variant access.
func TestUnguardedVariantReadInPredicate(t *testing.T) {
	t.Parallel()

	// On a reverted entry, log.created is a zero view, so metadata.size()==0 is
	// true and the reverted branch runs. checkSingleVariant accepts it (only the
	// reverted variant is set); this is a footgun, not corruption.
	r, err := NewRewriter(rules(
		rule("true", `log.created.metadata.size() == 0 ? log.withReverted(log.reverted.setMetadata("flag", "x")) : log`, false),
	))
	require.NoError(t, err)

	reverted := revertedEntry(1, 1, 2, []*commonpb.Posting{posting("bank", "world", "USD", 1)}, nil)
	out, err := r.Apply(reverted)
	require.NoError(t, err)
	require.NotNil(t, out.GetRevertedTransaction())
	require.Nil(t, out.GetCreatedTransaction())
	require.Equal(t, "x", out.GetRevertedTransaction().GetMetadata()["flag"].GetStringValue())
}

func TestMatchesLiteralRegexRejectedAtCompile(t *testing.T) {
	t.Parallel()

	// The CEL built-in matches() with a malformed literal pattern is rejected at
	// admission (in both match and cel positions) rather than stalling at run time.
	_, err := NewRewriter(rules(rule(`"x".matches("(")`, `log`, false)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "matches pattern")

	_, err = NewRewriter(rules(rule("true", `("x".matches("(")) ? log : log`, false)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "matches pattern")

	// A valid literal matches() still compiles; a data-derived pattern is left
	// alone (validated best-effort at run time).
	_, err = NewRewriter(rules(rule(`has(log.created) && log.created.postings.exists(p, p.source.matches("^world"))`, `log`, false)))
	require.NoError(t, err)
}

func TestSetAccountMetadataFromAddressLiteralReplacementRejected(t *testing.T) {
	t.Parallel()

	// A NUL byte in the literal replacement is rejected at admission.
	_, err := NewRewriter(rules(
		rule("true", "log.withCreated(log.created.setAccountMetadataFromAddress(\"^(.+)$\", \"k\", \"bad\\x00\"))", false),
	))
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata value")

	// A group-ref replacement stays valid (never over-rejected).
	_, err = NewRewriter(rules(
		rule("true", `log.withCreated(log.created.setAccountMetadataFromAddress("^(.+)$", "k", "$1"))`, false),
	))
	require.NoError(t, err)
}

// --- determinism guard ------------------------------------------------------

func TestMapIterationDeterminismGuard(t *testing.T) {
	t.Parallel()

	rejected := []string{
		`log.withCreated(log.created.setMetadata("keys", log.created.metadata.map(k, k).join(",")))`,
		`log.withCreated(log.created.setMetadata("keys", log.created.metadata.filter(k, k != "x").join(",")))`,
	}
	for _, expr := range rejected {
		_, err := NewRewriter(rules(rule("true", expr, false)))
		require.Error(t, err, expr)
		require.Contains(t, err.Error(), "order-sensitive iteration over a map", expr)
	}

	allowed := []string{
		`has(log.created) && log.created.metadata.exists(k, k == "type")`,
		`has(log.created) && log.created.accountMetadata.all(a, a != "")`,
		`has(log.created) && log.created.postings.map(p, p.source).size() > 0`,
	}
	for _, match := range allowed {
		_, err := NewRewriter(rules(rule(match, `log`, false)))
		require.NoError(t, err, match)
	}
}

func TestDeterministicOutput(t *testing.T) {
	t.Parallel()

	r, err := NewRewriter(rules(
		rule("true", `log.mapAddress(a, a.split(":").reverse().join(":"))`, false),
		rule("true", `log.withCreated(log.created.setMetadata("k", "v", "string"))`, false),
	))
	require.NoError(t, err)

	build := func() *raftcmdpb.MirrorLogEntry {
		e := createdEntry(1, 1, []*commonpb.Posting{posting("a:b:c", "x:y:z", "USD", 1)}, nil)
		e.GetCreatedTransaction().AccountMetadata = map[string]*commonpb.MetadataMap{
			"m:1": {Values: stringsToMetadata(map[string]string{"a": "1"}, nil)},
			"m:2": {Values: stringsToMetadata(map[string]string{"b": "2"}, nil)},
		}

		return e
	}

	first, err := r.Apply(build())
	require.NoError(t, err)

	for range 20 {
		out, err := r.Apply(build())
		require.NoError(t, err)
		require.True(t, first.EqualVT(out), "rewrite output must be identical across runs")
	}
}
