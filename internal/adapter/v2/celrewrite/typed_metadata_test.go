package celrewrite

import (
	"strings"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// -------------------- Typed metadata on setMetadata --------------------

func TestTypedMetadata_LiteralInt64(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "shard",
							Source: &commonpb.SetMetadataAction_Value{Value: "42"},
							Type:   "int64",
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

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	mv := out.GetCreatedTransaction().GetMetadata()["shard"]
	if mv == nil {
		t.Fatalf("shard metadata missing")
	}

	if _, ok := mv.GetType().(*commonpb.MetadataValue_IntValue); !ok {
		t.Fatalf("shard is not an int64 typed value: %T", mv.GetType())
	}
}

func TestTypedMetadata_ValueExprAsInt64(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "seq",
							Source: &commonpb.SetMetadataAction_ValueExpr{ValueExpr: `log.reference`},
							Type:   "int64",
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
				TransactionId: 1,
				Reference:     "123",
			},
		},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	mv := out.GetCreatedTransaction().GetMetadata()["seq"]
	if _, ok := mv.GetType().(*commonpb.MetadataValue_IntValue); !ok {
		t.Fatalf("seq is not an int64 typed value: %T", mv.GetType())
	}
}

func TestTypedMetadata_UnparsableProducesNull(t *testing.T) {
	t.Parallel()

	// Per platform semantics, a value that doesn't parse as the declared type
	// becomes a null value preserving the original string. Not a batch failure.
	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    "shard",
							Source: &commonpb.SetMetadataAction_Value{Value: "not-a-number"},
							Type:   "int64",
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

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	mv := out.GetCreatedTransaction().GetMetadata()["shard"]
	if _, ok := mv.GetType().(*commonpb.MetadataValue_NullValue); !ok {
		t.Fatalf("expected null value on failed coercion; got %T", mv.GetType())
	}
}

func TestTypedMetadata_UnknownTypeRejectedAtAdmission(t *testing.T) {
	t.Parallel()

	rule := &commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
		CreatedTransaction: &commonpb.CreatedTransactionRule{
			Actions: []*commonpb.CreatedTransactionAction{{
				Action: &commonpb.CreatedTransactionAction_SetMetadata{
					SetMetadata: &commonpb.SetMetadataAction{
						Key:    "k",
						Source: &commonpb.SetMetadataAction_Value{Value: "v"},
						Type:   "float128", // not a known metadata type
					},
				},
			}},
		},
	}}

	_, err := NewRewriter([]*commonpb.MirrorRewriteRule{rule})
	if err == nil {
		t.Fatalf("expected admission rejection for unknown metadata type token")
	}

	if !strings.Contains(err.Error(), "invalid metadata type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// -------------------- Typed metadata on setAccountMetadata --------------------

func TestTypedMetadata_OnSetAccountMetadata(t *testing.T) {
	t.Parallel()

	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetAccountMetadata{
						SetAccountMetadata: &commonpb.SetAccountMetadataAction{
							Account: "world",
							Key:     "flag",
							Source:  &commonpb.SetAccountMetadataAction_Value{Value: "true"},
							Type:    "bool",
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
				TransactionId: 1,
				Postings:      []*commonpb.Posting{posting("world", "acme")},
			},
		},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	mv := out.GetCreatedTransaction().GetAccountMetadata()["world"].GetValues()["flag"]
	if _, ok := mv.GetType().(*commonpb.MetadataValue_BoolValue); !ok {
		t.Fatalf("world/flag not a bool value: %T", mv.GetType())
	}
}

// -------------------- Multi-replacements on setAccountMetadataFromAddress --------------------

func TestMultiReplacements_TwoGroupsFromOneMatch(t *testing.T) {
	t.Parallel()

	// Single pattern captures both the acquirer name and the worker id in one
	// match; each is written to a distinct metadata key, and the worker id is
	// typed as int64. Wire example the user asked to support:
	//   pattern: "^acquirer:([^:]+):worker:(\\d+):.*$"
	//   replacements:
	//     - key: acquirer  replacement: $1
	//     - key: worker_id replacement: $2  type: int64
	r := mustCompile(t,
		&commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetAccountMetadataFromAddress{
						SetAccountMetadataFromAddress: &commonpb.SetAccountMetadataFromAddressAction{
							Pattern: `^acquirer:([^:]+):worker:(\d+):.*$`,
							Replacements: []*commonpb.SetAccountMetadataFromAddressReplacement{
								{Key: "acquirer", Replacement: "$1"},
								{Key: "worker_id", Replacement: "$2", Type: "int64"},
							},
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
				TransactionId: 1,
				Postings: []*commonpb.Posting{
					posting("acquirer:acme:worker:001:bank", "world"),
				},
			},
		},
	}

	out, err := r.Apply(entry)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	addr := "acquirer:acme:worker:001:bank"
	acc := out.GetCreatedTransaction().GetAccountMetadata()[addr]
	if acc == nil {
		t.Fatalf("no account metadata written for %s", addr)
	}

	acquirer := acc.GetValues()["acquirer"]
	if acquirer.GetStringValue() != "acme" {
		t.Fatalf("acquirer = %q; want acme", acquirer.GetStringValue())
	}

	workerID := acc.GetValues()["worker_id"]
	if _, ok := workerID.GetType().(*commonpb.MetadataValue_IntValue); !ok {
		t.Fatalf("worker_id not int64: %T", workerID.GetType())
	}
}

func TestMultiReplacements_EmptyRejectedAtAdmission(t *testing.T) {
	t.Parallel()

	rule := &commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
		CreatedTransaction: &commonpb.CreatedTransactionRule{
			Actions: []*commonpb.CreatedTransactionAction{{
				Action: &commonpb.CreatedTransactionAction_SetAccountMetadataFromAddress{
					SetAccountMetadataFromAddress: &commonpb.SetAccountMetadataFromAddressAction{
						Pattern: `^acquirer:.*$`,
						// no replacements — nothing to write
					},
				},
			}},
		},
	}}

	_, err := NewRewriter([]*commonpb.MirrorRewriteRule{rule})
	if err == nil {
		t.Fatalf("expected admission rejection for empty replacements list")
	}

	if !strings.Contains(err.Error(), "at least one replacement") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMultiReplacements_InvalidReplacementKeyRejected(t *testing.T) {
	t.Parallel()

	rule := &commonpb.MirrorRewriteRule{Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
		CreatedTransaction: &commonpb.CreatedTransactionRule{
			Actions: []*commonpb.CreatedTransactionAction{{
				Action: &commonpb.CreatedTransactionAction_SetAccountMetadataFromAddress{
					SetAccountMetadataFromAddress: &commonpb.SetAccountMetadataFromAddressAction{
						Pattern: `^.*$`,
						Replacements: []*commonpb.SetAccountMetadataFromAddressReplacement{
							{Key: "bad key", Replacement: "$0"},
						},
					},
				},
			}},
		},
	}}

	_, err := NewRewriter([]*commonpb.MirrorRewriteRule{rule})
	if err == nil {
		t.Fatalf("expected admission rejection for invalid key")
	}

	if !strings.Contains(err.Error(), "invalid key") {
		t.Fatalf("unexpected error: %v", err)
	}
}
