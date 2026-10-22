package admission

import "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

// Small builder helpers for constructing MirrorRewriteRule protos in tests.
// The proto oneof nesting is verbose; these keep the test tables readable.

func anyRuleWithMatch(match string) *ledgerpb.MirrorRewriteRule {
	return &ledgerpb.MirrorRewriteRule{
		Scope: &ledgerpb.MirrorRewriteRule_AnyVariant{
			AnyVariant: &ledgerpb.AnyVariantRule{Match: match},
		},
	}
}

func anyRuleRewriteAddress(pattern, replacement string) *ledgerpb.MirrorRewriteRule {
	return &ledgerpb.MirrorRewriteRule{
		Scope: &ledgerpb.MirrorRewriteRule_AnyVariant{
			AnyVariant: &ledgerpb.AnyVariantRule{
				Actions: []*ledgerpb.AnyVariantAction{{
					Action: &ledgerpb.AnyVariantAction_RewriteAddress{
						RewriteAddress: &ledgerpb.RewriteAddressAction{Pattern: pattern, Replacement: replacement},
					},
				}},
			},
		},
	}
}

func createdRuleSetMetadata(match, key, value string, stop bool) *ledgerpb.MirrorRewriteRule {
	return &ledgerpb.MirrorRewriteRule{
		Scope: &ledgerpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &ledgerpb.CreatedTransactionRule{
				Match: match,
				Actions: []*ledgerpb.CreatedTransactionAction{{
					Action: &ledgerpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &ledgerpb.SetMetadataAction{
							Key:    key,
							Source: &ledgerpb.SetMetadataAction_Value{Value: value},
						},
					},
				}},
			},
		},
		Stop: stop,
	}
}
