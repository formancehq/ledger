package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	flagCollapseColors    = "collapse-colors"
	flagGroupBy           = "group-by"
	flagMaxPrecision      = "use-max-precision"
	flagVariableThreshold = "variable-threshold"
)

// accountsSpecs covers the six `accounts` product commands.
func accountsSpecs() []spec {
	return []spec{
		{
			path:      []string{"accounts", "aggregate-volumes"},
			aliases:   [][]string{nil, {"agg"}},
			summary:   "Aggregate account volumes",
			long:      "Sum balances across the accounts a filter selects, optionally grouped by address prefix depth.",
			example:   "accounts aggregate-volumes main --filter \"address like 'users:%'\"",
			risk:      sdk.RiskRead,
			arguments: []sdk.Argument{ledgerArgument()},
			flags: append([]sdk.Flag{
				stringFlag(flagFilter, "Account filter expression"),
				stringArrayFlag(flagGroupBy, "Group results by this address prefix"),
				boolFlag(flagMaxPrecision, "Return volumes at maximum precision"),
				boolFlag(flagCollapseColors, "Merge colored balances into their base asset"),
			}, readFlags()...),
			operations: []operation{opAggregateVolumes},
		},
		{
			path:       []string{"accounts", "analyze"},
			aliases:    [][]string{nil, {"analyse"}},
			summary:    "Analyze account address structure",
			long:       "Stream the address segment analysis used to propose account types for a ledger.",
			example:    "accounts analyze main --variable-threshold 8",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      []sdk.Flag{{Name: flagVariableThreshold, Usage: "Distinct values above which a segment is treated as variable", Type: sdk.FlagInt32}},
			operations: []operation{opAnalyzeAccounts},
			collection: true,
		},
		{
			path:    []string{"accounts", "delete-metadata"},
			aliases: [][]string{nil, {"del-meta", "dm", "rm-meta"}},
			summary: "Delete one account metadata key",
			long:    "Remove a single metadata key from an account.",
			example: "accounts delete-metadata main users:1234 category",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argAddress, "Account address"),
				requiredStringArgument(argKey, "Metadata key to delete"),
			},
			flags:      writeFlags(),
			operations: []operation{opApplyDeleteMetadata},
		},
		{
			path:    []string{"accounts", "get"},
			aliases: [][]string{nil, {"g", "show", "describe"}},
			summary: "Show one account",
			long:    "Read a single account with its volumes and metadata.",
			example: "accounts get main users:1234",
			risk:    sdk.RiskRead,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argAddress, "Account address"),
			},
			flags: []sdk.Flag{
				boolFlag(flagCollapseColors, "Merge colored balances into their base asset"),
				stringFlag(flagCheckpointID, "Read from this query checkpoint id (unsigned 64-bit decimal)"),
			},
			operations: []operation{opGetAccount},
		},
		{
			path:       []string{"accounts", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List accounts",
			long:       "Stream one page of accounts, ordered by address.",
			example:    "accounts list main --filter \"address like 'users:%'\"",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      listFlags("Account filter expression"),
			operations: []operation{opListAccounts},
			paginated:  true,
			collection: true,
		},
		{
			path:    []string{"accounts", "set-metadata"},
			aliases: [][]string{nil, {"set-meta", "sm"}},
			summary: "Set account metadata",
			long:    "Merge metadata entries into an account.",
			example: "accounts set-metadata main users:1234 --metadata category=premium",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argAddress, "Account address"),
			},
			flags: append([]sdk.Flag{
				{Name: flagMetadata, Usage: "Metadata entry as key=value; repeat for several keys", Type: sdk.FlagStringArray, Required: true},
			}, writeFlags()...),
			operations: []operation{opApplyAddMetadata},
		},
	}
}
