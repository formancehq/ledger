package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	flagIndexKind   = "kind"
	flagTargetType  = "target-type"
	flagMetadataKey = "metadata-key"
	flagBuiltin     = "builtin"
	flagInspectMode = "mode"
)

func indexIdentityFlags() []sdk.Flag {
	kind := stringFlag(flagIndexKind, "Index kind: metadata, tx-builtin, account-builtin or log-builtin")
	kind.Required = true
	kind.Completion = staticCompletion("metadata", "tx-builtin", "account-builtin", "log-builtin")

	target := stringFlag(flagTargetType, "Metadata target: account, transaction or ledger")
	target.Completion = staticCompletion("account", "transaction", "ledger")

	builtin := stringFlag(flagBuiltin, "Built-in field name, required for every builtin kind")
	builtin.Completion = staticCompletion(
		"reference", "timestamp", "id", "address", "source-address", "destination-address",
		"inserted-at", "reverted-at", "asset", "date",
	)

	return []sdk.Flag{
		kind,
		target,
		stringFlag(flagMetadataKey, "Metadata key, required for kind metadata"),
		builtin,
	}
}

// indexesSpecs covers the four `indexes` product commands.
//
// `list` binds ListIndexes with the per-ledger scope, which the server serves
// under ledger:LedgerRead; the bucket-wide and cross-ledger scopes are operator
// visibility and stay out of the business facet. It does not call
// GetIndexStatus per row the way ledgerctl does: a per-row enrichment call
// turns one page into an unbounded request fan-out, and the per-replica build
// status is available through `indexes inspect`.
func indexesSpecs() []spec {
	return []spec{
		{
			path:       []string{"indexes", "create"},
			aliases:    [][]string{nil, {"c"}},
			summary:    "Create an index",
			long:       "Declare a metadata or built-in field index on a ledger.",
			example:    "indexes create main --kind metadata --target-type transaction --metadata-key category",
			risk:       sdk.RiskMutation,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      append(indexIdentityFlags(), writeFlags()...),
			operations: []operation{opApplyCreateIndex},
		},
		{
			path:       []string{"indexes", "drop"},
			aliases:    [][]string{nil, {"d"}},
			summary:    "Drop an index",
			long:       "Remove an index declaration from a ledger.",
			example:    "indexes drop main --kind metadata --target-type transaction --metadata-key category",
			risk:       sdk.RiskMutation,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      append(indexIdentityFlags(), writeFlags()...),
			operations: []operation{opApplyDropIndex},
		},
		{
			path:      []string{"indexes", "inspect"},
			aliases:   [][]string{nil, {"i"}},
			summary:   "Inspect an index",
			long:      "Read distinct values, facets or a summary from one metadata index.",
			example:   "indexes inspect main --target-type transaction --metadata-key category --mode facets",
			risk:      sdk.RiskRead,
			arguments: []sdk.Argument{ledgerArgument()},
			flags: func() []sdk.Flag {
				target := stringFlag(flagTargetType, "Metadata target: account, transaction or ledger")
				target.Required = true
				target.Completion = staticCompletion("account", "transaction", "ledger")
				key := stringFlag(flagMetadataKey, "Metadata key")
				key.Required = true
				mode := stringFlag(flagInspectMode, "Inspection mode: distinct-values, facets or summary")
				mode.HasDefault = true
				mode.DefaultValue = "summary"
				mode.Completion = staticCompletion("distinct-values", "facets", "summary")
				return []sdk.Flag{
					target,
					key,
					mode,
					{Name: flagPageSize, Usage: "Maximum values per page", Type: sdk.FlagInt32, HasDefault: true, DefaultValue: "20"},
					stringFlag(flagCursor, "Opaque cursor returned by a previous page"),
					stringFlag(flagCheckpointID, "Read from this query checkpoint id (unsigned 64-bit decimal)"),
				}
			}(),
			operations: []operation{opInspectIndex},
		},
		{
			path:       []string{"indexes", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List a ledger's indexes",
			long:       "Stream the index registry entries declared on a ledger.",
			example:    "indexes list main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			operations: []operation{opListIndexes},
			collection: true,
		},
	}
}
