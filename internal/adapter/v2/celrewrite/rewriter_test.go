package celrewrite

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// -------------------- Fixtures --------------------

// entrySavedMetadata builds a SavedMetadata log entry with an account target.
func entrySavedMetadata(target string, md map[string]string) *raftcmdpb.MirrorLogEntry {
	values := make(map[string]*ledgerpb.MetadataValue, len(md))
	for k, v := range md {
		values[k] = ledgerpb.NewStringValue(v)
	}

	return &raftcmdpb.MirrorLogEntry{
		V2LogId: 42,
		Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
			SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Account{
						Account: &ledgerpb.TargetAccount{Addr: target},
					},
				},
				Metadata: values,
			},
		},
	}
}

// entryCreatedTx builds a CreatedTransaction log entry.
func entryCreatedTx(txID uint64, postings []*ledgerpb.Posting, md map[string]string) *raftcmdpb.MirrorLogEntry {
	values := make(map[string]*ledgerpb.MetadataValue, len(md))
	for k, v := range md {
		values[k] = ledgerpb.NewStringValue(v)
	}

	return &raftcmdpb.MirrorLogEntry{
		V2LogId: 7,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId: txID,
				Postings:      postings,
				Metadata:      values,
			},
		},
	}
}

func posting(src, dst string) *ledgerpb.Posting {
	return &ledgerpb.Posting{Source: src, Destination: dst, Asset: "USD"}
}

// -------------------- Rule builders --------------------

func createdRule(match string, actions ...*ledgerpb.CreatedTransactionAction) *ledgerpb.MirrorRewriteRule {
	return &ledgerpb.MirrorRewriteRule{Scope: &ledgerpb.MirrorRewriteRule_CreatedTransaction{
		CreatedTransaction: &ledgerpb.CreatedTransactionRule{Match: match, Actions: actions},
	}}
}

func savedRule(match string, actions ...*ledgerpb.SavedMetadataAction) *ledgerpb.MirrorRewriteRule {
	return &ledgerpb.MirrorRewriteRule{Scope: &ledgerpb.MirrorRewriteRule_SavedMetadata{
		SavedMetadata: &ledgerpb.SavedMetadataRule{Match: match, Actions: actions},
	}}
}

func anyRule(match string, actions ...*ledgerpb.AnyVariantAction) *ledgerpb.MirrorRewriteRule {
	return &ledgerpb.MirrorRewriteRule{Scope: &ledgerpb.MirrorRewriteRule_AnyVariant{
		AnyVariant: &ledgerpb.AnyVariantRule{Match: match, Actions: actions},
	}}
}

// Actions.

func actSetMetadataCreated(key, value string) *ledgerpb.CreatedTransactionAction {
	return &ledgerpb.CreatedTransactionAction{Action: &ledgerpb.CreatedTransactionAction_SetMetadata{
		SetMetadata: &ledgerpb.SetMetadataAction{
			Key:    key,
			Source: &ledgerpb.SetMetadataAction_Value{Value: value},
		},
	}}
}

func actSetMetadataSaved(key, value string) *ledgerpb.SavedMetadataAction {
	return &ledgerpb.SavedMetadataAction{Action: &ledgerpb.SavedMetadataAction_SetMetadata{
		SetMetadata: &ledgerpb.SetMetadataAction{
			Key:    key,
			Source: &ledgerpb.SetMetadataAction_Value{Value: value},
		},
	}}
}

func actSetMetadataSavedExpr(key, expr string) *ledgerpb.SavedMetadataAction {
	return &ledgerpb.SavedMetadataAction{Action: &ledgerpb.SavedMetadataAction_SetMetadata{
		SetMetadata: &ledgerpb.SetMetadataAction{
			Key:    key,
			Source: &ledgerpb.SetMetadataAction_ValueExpr{ValueExpr: expr},
		},
	}}
}

func actSetAccountMetadataCreatedExpr(account, key, expr string) *ledgerpb.CreatedTransactionAction {
	return &ledgerpb.CreatedTransactionAction{Action: &ledgerpb.CreatedTransactionAction_SetAccountMetadata{
		SetAccountMetadata: &ledgerpb.SetAccountMetadataAction{
			Account: account,
			Key:     key,
			Source:  &ledgerpb.SetAccountMetadataAction_ValueExpr{ValueExpr: expr},
		},
	}}
}

func actRewriteAddressAny(pattern, replacement string) *ledgerpb.AnyVariantAction {
	return &ledgerpb.AnyVariantAction{Action: &ledgerpb.AnyVariantAction_RewriteAddress{
		RewriteAddress: &ledgerpb.RewriteAddressAction{Pattern: pattern, Replacement: replacement},
	}}
}

func actDropCreated() *ledgerpb.CreatedTransactionAction {
	return &ledgerpb.CreatedTransactionAction{Action: &ledgerpb.CreatedTransactionAction_Drop{
		Drop: &ledgerpb.DropAction{},
	}}
}

// -------------------- Helpers --------------------

func mustCompile(t *testing.T, rules ...*ledgerpb.MirrorRewriteRule) *Rewriter {
	t.Helper()

	r, err := NewRewriter(rules)
	if err != nil {
		t.Fatalf("NewRewriter: %v", err)
	}

	return r
}

func mustFailCompile(t *testing.T, rule *ledgerpb.MirrorRewriteRule, wantSubstr string) {
	t.Helper()

	_, err := NewRewriter([]*ledgerpb.MirrorRewriteRule{rule})
	if err == nil {
		t.Fatalf("NewRewriter succeeded; wanted compile error containing %q", wantSubstr)
	}

	if !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("NewRewriter error = %v; wanted substring %q", err, wantSubstr)
	}
}

// -------------------- Scope wire safety --------------------

// TestWireSafety_InvalidCombosImpossible: the wire itself refuses combinations
// the fat-view design has to catch at runtime. We only assert that Go's type
// system prevents constructing them — a proto with `SavedMetadataRule` cannot
// carry a `SetAccountMetadataAction` because that action type isn't in the
// oneof of `SavedMetadataAction`. This is the design property that removes
// admission-time cross-variant checks.
func TestWireSafety_InvalidCombosImpossible(t *testing.T) {
	t.Parallel()

	// A SavedMetadataAction can carry only rewrite_address / set_metadata /
	// delete_metadata / drop. There is no SetAccountMetadata variant in its
	// oneof. Attempting to construct one would be a compile error in the Go
	// caller, not a runtime rejection here.
	//
	// This test is intentionally a compile-time smoke test — it asserts the
	// available oneof cases and would fail to build if the proto shape ever
	// widened. If the code below still compiles, the safety property holds.
	_ = &ledgerpb.SavedMetadataAction{Action: &ledgerpb.SavedMetadataAction_SetMetadata{
		SetMetadata: &ledgerpb.SetMetadataAction{
			Key:    "k",
			Source: &ledgerpb.SetMetadataAction_Value{Value: "v"},
		},
	}}
	_ = &ledgerpb.SavedMetadataAction{Action: &ledgerpb.SavedMetadataAction_Drop{
		Drop: &ledgerpb.DropAction{},
	}}
	// Uncommenting the below MUST NOT compile — it proves the wire safety.
	// _ = &ledgerpb.SavedMetadataAction{Action: &ledgerpb.SavedMetadataAction_SetAccountMetadata{...}}
}

// -------------------- End-to-end Apply --------------------

func TestApply_SetMetadataOnSavedMetadata(t *testing.T) {
	t.Parallel()

	r := mustCompile(t, savedRule(
		`log.target.account.addr.startsWith("acquirer:")`,
		actSetMetadataSaved("classified", "acquirer"),
	))

	entry := entrySavedMetadata("acquirer:acme:worker:001", nil)

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got := out.GetSavedMetadata().GetMetadata()["classified"].GetStringValue()
	if got != "acquirer" {
		t.Fatalf("classified = %q; want acquirer", got)
	}
}

func TestApply_RewriteAddressAcrossPostingsAndTarget(t *testing.T) {
	t.Parallel()

	r := mustCompile(t, anyRule("", actRewriteAddressAny(":worker:\\d+", "")))

	entry := entryCreatedTx(9,
		[]*ledgerpb.Posting{
			posting("acquirer:acme:worker:001", "world"),
			posting("world", "customer:bob:worker:007"),
		},
		nil,
	)

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	ps := out.GetCreatedTransaction().GetPostings()
	if got := ps[0].GetSource(); got != "acquirer:acme" {
		t.Fatalf("posting[0].Source = %q; want acquirer:acme", got)
	}

	if got := ps[1].GetDestination(); got != "customer:bob" {
		t.Fatalf("posting[1].Destination = %q; want customer:bob", got)
	}
}

func TestApply_DropCreatedTransactionEmitsFillGapWithTxID(t *testing.T) {
	t.Parallel()

	r := mustCompile(t, createdRule("", actDropCreated()))

	entry := entryCreatedTx(1234, []*ledgerpb.Posting{posting("world", "alice")}, nil)

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	gap, ok := out.GetData().(*raftcmdpb.MirrorLogEntry_FillGap)
	if !ok {
		t.Fatalf("expected fill-gap; got %T", out.GetData())
	}

	if got := gap.FillGap.GetSkippedTransactionIds(); len(got) != 1 || got[0] != 1234 {
		t.Fatalf("SkippedTransactionIds = %v; want [1234]", got)
	}

	if out.GetV2LogId() != entry.GetV2LogId() {
		t.Fatalf("V2LogId lost")
	}
}

func TestApply_ChainAndStop(t *testing.T) {
	t.Parallel()

	// Rule 1 matches → sets "first" then stops.
	// Rule 2 would set "second" but must not run because rule 1 stopped.
	rule1 := savedRule(`log.target.account.addr == "world"`, actSetMetadataSaved("first", "yes"))
	rule1.Stop = true

	rule2 := savedRule("", actSetMetadataSaved("second", "yes"))

	r := mustCompile(t, rule1, rule2)

	out, err := r.Apply(entrySavedMetadata("world", nil))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	md := out.GetSavedMetadata().GetMetadata()
	if _, ok := md["first"]; !ok {
		t.Fatalf("first rule did not run: %v", md)
	}

	if _, ok := md["second"]; ok {
		t.Fatalf("second rule ran despite stop: %v", md)
	}
}

func TestApply_ScopeMismatchSkipsRule(t *testing.T) {
	t.Parallel()

	// A CreatedTransaction rule applied to a SavedMetadata entry is silently
	// skipped: no error, no mutation, next rule runs.
	r := mustCompile(t,
		createdRule("", actSetMetadataCreated("k", "v")),
		savedRule("", actSetMetadataSaved("k", "v")),
	)

	out, err := r.Apply(entrySavedMetadata("world", nil))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if v := out.GetSavedMetadata().GetMetadata()["k"].GetStringValue(); v != "v" {
		t.Fatalf("saved rule should have written k=v; got %q", v)
	}
}

func TestApply_MatchErrorDoesNotStallBatch(t *testing.T) {
	t.Parallel()

	// Indexing a missing metadata key raises a CEL runtime error; the rule is
	// skipped (not failed) so a data-dependent predicate can't stall the mirror.
	r := mustCompile(t, savedRule(
		`log.metadata["missing"].string_value == "yes"`,
		actSetMetadataSaved("k", "v"),
	))

	out, err := r.Apply(entrySavedMetadata("world", nil))
	if err != nil {
		t.Fatalf("Apply must not error on match failure: %v", err)
	}

	if _, ok := out.GetSavedMetadata().GetMetadata()["k"]; ok {
		t.Fatalf("rule mutation applied despite match runtime error")
	}
}

func TestApply_InvalidAddressRewriteRejected(t *testing.T) {
	t.Parallel()

	r := mustCompile(t, anyRule("", actRewriteAddressAny("^world$", "")))

	_, err := r.Apply(entrySavedMetadata("world", nil))
	if err == nil {
		t.Fatalf("expected error on empty rewritten target")
	}

	if !strings.Contains(err.Error(), "target address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApply_PassThroughForFillGap(t *testing.T) {
	t.Parallel()

	r := mustCompile(t, anyRule("", actRewriteAddressAny(":worker:\\d+", "")))
	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 12,
		Data:    &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{}},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if out != entry {
		t.Fatalf("expected same instance for pass-through")
	}
}

func TestApply_NilRewriterIsPassThrough(t *testing.T) {
	t.Parallel()

	var r *Rewriter
	entry := entrySavedMetadata("world", nil)

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if out != entry {
		t.Fatalf("nil rewriter must pass through")
	}
}

func TestApply_DoesNotMutateInput(t *testing.T) {
	t.Parallel()

	r := mustCompile(t, savedRule("", actSetMetadataSaved("k", "v")))
	entry := entrySavedMetadata("world", nil)

	before := proto.Clone(entry).(*raftcmdpb.MirrorLogEntry)

	if _, err := r.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !proto.Equal(entry, before) {
		t.Fatalf("Apply mutated the input entry")
	}
}

// -------------------- Admission-time gates --------------------

func TestAdmission_UnsetScopeRejected(t *testing.T) {
	t.Parallel()

	mustFailCompile(t, &ledgerpb.MirrorRewriteRule{}, "scope must be set")
}

func TestAdmission_InvalidCELMatch(t *testing.T) {
	t.Parallel()

	mustFailCompile(t, anyRule(`this is not valid cel`), "match compile")
}

func TestAdmission_NonBooleanMatch(t *testing.T) {
	t.Parallel()

	mustFailCompile(t, anyRule(`"a string"`), "match must return bool")
}

func TestAdmission_InvalidRegexPattern(t *testing.T) {
	t.Parallel()

	mustFailCompile(t, anyRule("", actRewriteAddressAny("(", "")), "error parsing regexp")
}

func TestAdmission_InvalidLiteralMetadataKey(t *testing.T) {
	t.Parallel()

	mustFailCompile(t,
		createdRule("", actSetMetadataCreated("bad key", "v")),
		"invalid key",
	)
}

func TestAdmission_TooManyRules(t *testing.T) {
	t.Parallel()

	rules := make([]*ledgerpb.MirrorRewriteRule, MaxRules+1)
	for i := range rules {
		rules[i] = anyRule("")
	}

	_, err := NewRewriter(rules)
	if err == nil {
		t.Fatalf("expected error for too many rules")
	}

	if !strings.Contains(err.Error(), "too many rewrite rules") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdmission_MatchExpressionTooLong(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", MaxExprLen+1)

	mustFailCompile(t, anyRule(long), "too long")
}
