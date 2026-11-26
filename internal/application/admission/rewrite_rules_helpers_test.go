package admission

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// Small builder helpers for constructing MirrorRewriteRule protos in tests.
// The proto oneof nesting is verbose; these keep the test tables readable.

func anyRuleWithMatch(match string) *commonpb.MirrorRewriteRule {
	return &commonpb.MirrorRewriteRule{
		Scope: &commonpb.MirrorRewriteRule_AnyVariant{
			AnyVariant: &commonpb.AnyVariantRule{Match: match},
		},
	}
}

func anyRuleRewriteAddress(pattern, replacement string) *commonpb.MirrorRewriteRule {
	return &commonpb.MirrorRewriteRule{
		Scope: &commonpb.MirrorRewriteRule_AnyVariant{
			AnyVariant: &commonpb.AnyVariantRule{
				Actions: []*commonpb.AnyVariantAction{{
					Action: &commonpb.AnyVariantAction_RewriteAddress{
						RewriteAddress: &commonpb.RewriteAddressAction{Pattern: pattern, Replacement: replacement},
					},
				}},
			},
		},
	}
}

func createdRuleSetMetadata(match, key, value string, stop bool) *commonpb.MirrorRewriteRule {
	return &commonpb.MirrorRewriteRule{
		Scope: &commonpb.MirrorRewriteRule_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransactionRule{
				Match: match,
				Actions: []*commonpb.CreatedTransactionAction{{
					Action: &commonpb.CreatedTransactionAction_SetMetadata{
						SetMetadata: &commonpb.SetMetadataAction{
							Key:    key,
							Source: &commonpb.SetMetadataAction_Value{Value: value},
						},
					},
				}},
			},
		},
		Stop: stop,
	}
}
