package indexes

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// indexTypeOptions is the canonical set of --type values accepted by the
// indexes create/drop commands. It backs both the interactive selector and
// shell completion so the two never drift. Every entry must have a matching
// case in runCreateIndex/runDropIndex; do not add a value here before the
// command handles it, or both the menu and completion will steer users into an
// "invalid index type" error.
var indexTypeOptions = []string{
	"address",
	"source-address",
	"dest-address",
	"metadata",
	"reference",
	"timestamp",
	"inserted-at",
	"account-asset",
}

// txBuiltinIndexID constructs an IndexID for a transaction builtin field.
func txBuiltinIndexID(b commonpb.TransactionBuiltinIndex) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: b}}
}

// accountBuiltinIndexID constructs an IndexID for an account builtin field.
func accountBuiltinIndexID(b commonpb.AccountBuiltinIndex) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: b}}
}

// metadataIndexID constructs an IndexID for a metadata key on the given target.
func metadataIndexID(target commonpb.TargetType, key string) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
		Target: target,
		Key:    key,
	}}}
}
