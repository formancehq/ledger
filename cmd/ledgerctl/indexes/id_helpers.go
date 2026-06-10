package indexes

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// txBuiltinIndexID constructs an IndexID for a transaction builtin field.
func txBuiltinIndexID(b commonpb.TransactionBuiltinIndex) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: b}}
}

// metadataIndexID constructs an IndexID for a metadata key on the given target.
func metadataIndexID(target commonpb.TargetType, key string) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
		Target: target,
		Key:    key,
	}}}
}
