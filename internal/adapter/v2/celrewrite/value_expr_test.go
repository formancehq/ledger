package celrewrite

import (
	"strings"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestValueExpr_CopiesReferenceIntoMetadata is the load-bearing scenario for
// value_expr: it derives a metadata value from another field of the current
// variant, which no other action can express without expression evaluation.
func TestValueExpr_CopiesReferenceIntoMetadata(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "original_ref",
							Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: `log.reference`},
						},
					},
				}},
			},
		}},
	)

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 1,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId: 42,
				Reference:     "invoice-2026-07-08",
				Postings:      []*commonpb.Posting{posting("world", "acme")},
			},
		},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got := out.GetCreatedTransaction().GetMetadata()["original_ref"].GetStringValue()
	if got != "invoice-2026-07-08" {
		t.Fatalf("metadata[original_ref] = %q; want invoice-2026-07-08", got)
	}
}

// TestValueExpr_ReadsMutatedStateFromPreviousAction confirms the action-chain
// semantics: value_expr sees the log AFTER any previous action has mutated it.
func TestValueExpr_ReadsMutatedStateFromPreviousAction(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{
					// (1) write a literal metadata key
					{Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "seed",
							Source: &commonpb.SetMetadataAction_Value{Value: "hello"},
						},
					}},
					// (2) read what (1) wrote via value_expr
					{Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "echo",
							Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: `log.metadata["seed"].string_value`},
						},
					}},
				},
			},
		}},
	)

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 1,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{TransactionId: 1},
		},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := out.GetCreatedTransaction().GetMetadata()["echo"].GetStringValue(); got != "hello" {
		t.Fatalf("echo = %q; want hello", got)
	}
}

// TestValueExpr_WrongOutputType is a compile-time rejection: value_expr must
// return a string. Returning an int fails at admission — not at commit time.
func TestValueExpr_WrongOutputType(t *testing.T) {
	t.Parallel()

	rule := &commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
		CreatedTransaction: &commonpb.CreatedTransactionRule{
			Actions: []*commonpb.CreatedTransactionAction{{
				Action: &commonpb.CreatedTransactionAction_SetMetadata{
					SetMetadata: &commonpb.SetMetadataAction{
						Key:    "k",
						Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: `42`},
					},
				},
			}},
		},
	}}

	_, err := NewRewriter([]*commonpb.MirrorRewriteRule{rule})
	if err == nil {
		t.Fatalf("expected compile error on non-string value_expr")
	}

	if !strings.Contains(err.Error(), "must return string") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValueExpr_ForeignVariantFieldRejectedAtAdmission proves the value_expr's
// env is typed per-variant: a SavedMetadata rule cannot reference `log.reference`
// because MirrorSavedMetadata has no such field. CEL's type checker rejects
// the expression at admission — no runtime failure surfaces later.
func TestValueExpr_ForeignVariantFieldRejectedAtAdmission(t *testing.T) {
	t.Parallel()

	rule := &commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_SavedMetadata{
		SavedMetadata: &commonpb.SavedMetadataRule{
			Actions: []*commonpb.SavedMetadataAction{{
				Action: &commonpb.SavedMetadataAction_SetMetadata{
					SetMetadata: &commonpb.SetMetadataAction{
						Key:    "k",
						Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: `log.reference`},
					},
				},
			}},
		},
	}}

	_, err := NewRewriter([]*commonpb.MirrorRewriteRule{rule})
	if err == nil {
		t.Fatalf("expected compile error; MirrorSavedMetadata has no reference field")
	}
	// The exact CEL error phrasing may vary across versions ("undefined field
	// reference" / "no such field"), so match on the presence of a compile
	// tag rather than a fixed substring.
	if !strings.Contains(err.Error(), "compile") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValueExpr_RuntimeErrorFailsBatch: unlike a `match` runtime error (which
// silently skips the rule), a value_expr runtime error fails the batch loudly.
// This is deliberate — value_expr participates in mutation and a silent skip
// would leave the metadata write unapplied without telling the operator.
func TestValueExpr_RuntimeErrorFailsBatch(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					// Indexing a missing metadata key raises "no such key" at runtime.
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "k",
							Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: `log.metadata["missing"].string_value`},
						},
					},
				}},
			},
		}},
	)

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 1,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{TransactionId: 1},
		},
	}

	_, err := r.Apply(entry)
	if err == nil {
		t.Fatalf("expected runtime error to fail the batch")
	}

	if !strings.Contains(err.Error(), "value_expr") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValueExpr_ProducesInvalidValueRejected: a value_expr that returns a
// string containing e.g. control characters fails runtime validation. The
// admission-time literal check doesn't catch it (value is computed), so this
// is the only chance to reject the write.
func TestValueExpr_ProducesInvalidValueRejected(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					// A metadata value containing a NUL byte fails validateValue.
					// The CEL literal uses  so the produced string carries
					// an actual NUL — which the admission-time literal check
					// couldn't spot because the value is computed.
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "k",
							Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: "\"a\\u0000b\""},
						},
					},
				}},
			},
		}},
	)

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 1,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{TransactionId: 1},
		},
	}

	_, err := r.Apply(entry)
	if err == nil {
		t.Fatalf("expected invalid produced value to fail the batch")
	}

	if !strings.Contains(err.Error(), "invalid value") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValueExpr_OnSetAccountMetadata verifies the same wiring on
// SetAccountMetadata (Created only). Reads the reference into a per-account
// key.
func TestValueExpr_OnSetAccountMetadata(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{
					actSetAccountMetadataCreatedExpr("world", "invoice", "log.reference"),
				},
			},
		}},
	)

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 1,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId: 42,
				Reference:     "inv-42",
				Postings:      []*commonpb.Posting{posting("world", "acme")},
			},
		},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	acc := out.GetCreatedTransaction().GetAccountMetadata()["world"]
	if acc == nil {
		t.Fatalf("account metadata not written for world")
	}

	if got := acc.GetValues()["invoice"].GetStringValue(); got != "inv-42" {
		t.Fatalf("world/invoice = %q; want inv-42", got)
	}
}

// TestValueExpr_SavedMetadataReadsTarget confirms the SavedMetadata env exposes
// its own fields (target, metadata) via value_expr — nothing else.
func TestValueExpr_SavedMetadataReadsTarget(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadataRule{
				Actions: []*commonpb.SavedMetadataAction{
					actSetMetadataSavedExpr("target_copy", "log.target.account.addr"),
				},
			},
		}},
	)

	out, err := r.Apply(entrySavedMetadata("world:alice", nil))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := out.GetSavedMetadata().GetMetadata()["target_copy"].GetStringValue(); got != "world:alice" {
		t.Fatalf("target_copy = %q; want world:alice", got)
	}
}
