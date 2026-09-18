package indexes

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// indexTypeOptions is the canonical set of --type values accepted by the
// indexes create/drop commands. It backs both the interactive selector and
// shell completion so the two never drift. Every entry must have a matching
// handler in builtinIndex or the metadata branch; do not add a value here before the
// command handles it, or both the menu and completion will steer users into an
// "invalid index type" error.
var indexTypeOptions = []string{
	"address",
	"source-address",
	"destination-address",
	"metadata",
	"reference",
	"timestamp",
	"inserted-at",
	"reverted-at",
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

// ParseDefinition parses an initial --index declaration using the same type
// names as indexes create/drop. Metadata keys may themselves contain colons.
func ParseDefinition(definition string) (*commonpb.IndexID, error) {
	indexType, rest, hasFields := strings.Cut(definition, ":")
	if indexType == "metadata" {
		targetName, key, ok := strings.Cut(rest, ":")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid index %q: expected metadata:<account|transaction>:<key>", definition)
		}
		target, err := cmdutil.ParseTargetType(targetName)
		if err != nil {
			return nil, err
		}
		if target != commonpb.TargetType_TARGET_TYPE_ACCOUNT && target != commonpb.TargetType_TARGET_TYPE_TRANSACTION {
			return nil, fmt.Errorf("invalid metadata index target %q: expected account or transaction", targetName)
		}

		return metadataIndexID(target, key), nil
	}
	if hasFields {
		return nil, fmt.Errorf("index type %q does not accept metadata fields", indexType)
	}
	id, _, err := builtinIndex(indexType)

	return id, err
}

// builtinIndex is shared by initial declarations and standalone create/drop.
func builtinIndex(indexType string) (*commonpb.IndexID, string, error) {
	switch indexType {
	case "address":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS), "address (any role)", nil
	case "source-address":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS), indexType, nil
	case "destination-address":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS), indexType, nil
	case "reference":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE), indexType, nil
	case "timestamp":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP), indexType, nil
	case "inserted-at":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT), indexType, nil
	case "reverted-at":
		return txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT), indexType, nil
	case "account-asset":
		return accountBuiltinIndexID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET), "account has-asset", nil
	default:
		return nil, "", fmt.Errorf("invalid index type %q: must be %s", indexType, strings.Join(indexTypeOptions, ", "))
	}
}
