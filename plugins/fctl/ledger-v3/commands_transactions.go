package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	argTransactionID    = "transaction-id"
	flagPosting         = "posting"
	flagReference       = "reference"
	flagTimestamp       = "timestamp"
	flagForce           = "force"
	flagAtEffectiveDate = "at-effective-date"
	flagScriptVar       = "script-var"
)

// transactionsSpecs covers the seven `transactions` product commands.
func transactionsSpecs() []spec {
	return []spec{
		{
			path:       []string{"transactions", "analyze"},
			aliases:    [][]string{nil, {"analyse"}},
			summary:    "Analyze transaction structure",
			long:       "Stream the structural analysis of a ledger's transactions.",
			example:    "transactions analyze main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      []sdk.Flag{{Name: flagVariableThreshold, Usage: "Distinct values above which a segment is treated as variable", Type: sdk.FlagInt32}},
			operations: []operation{opAnalyzeTransactions},
		},
		{
			path:      []string{"transactions", "create"},
			aliases:   [][]string{nil, {"new", "add"}},
			summary:   "Create a transaction",
			long:      "Commit a transaction from explicit postings or from a Numscript source.",
			example:   "transactions create main --posting world:USD/2:100:users:1234",
			risk:      sdk.RiskMutation,
			arguments: []sdk.Argument{ledgerArgument()},
			flags: append([]sdk.Flag{
				stringArrayFlag(flagPosting, "Posting as source:asset:amount:destination; repeat for several postings"),
				stringFlag(flagScript, "Numscript source, read from a file or from stdin"),
				stringArrayFlag(flagScriptVar, "Numscript variable as name=value; repeat for several variables"),
				stringFlag(flagReference, "Unique transaction reference"),
				stringFlag(flagTimestamp, "RFC 3339 effective timestamp"),
				stringArrayFlag(flagMetadata, "Transaction metadata as key=value; repeat for several keys"),
				boolFlag(flagForce, "Commit even when balance checks would reject the transaction"),
			}, writeFlags()...),
			operations: []operation{opApplyCreateTransaction},
			artifacts: []sdk.InputArtifactSpec{{
				FlagName:   flagScript,
				MediaTypes: []string{"text/plain"},
				MaxBytes:   maxNumscriptBytes,
				AllowFile:  true,
				AllowStdin: true,
			}},
		},
		{
			path:    []string{"transactions", "delete-metadata"},
			aliases: [][]string{nil, {"del-meta", "dm", "rm-meta"}},
			summary: "Delete one transaction metadata key",
			long:    "Remove a single metadata key from a transaction.",
			example: "transactions delete-metadata main 42 category",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argTransactionID, "Transaction id (unsigned 64-bit decimal)"),
				requiredStringArgument(argKey, "Metadata key to delete"),
			},
			flags:      writeFlags(),
			operations: []operation{opApplyDeleteMetadata},
		},
		{
			path:    []string{"transactions", "get"},
			aliases: [][]string{nil, {"g", "show", "describe"}},
			summary: "Show one transaction",
			long:    "Read a single transaction with its postings, metadata and post-commit volumes.",
			example: "transactions get main 42",
			risk:    sdk.RiskRead,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argTransactionID, "Transaction id (unsigned 64-bit decimal)"),
			},
			flags:      []sdk.Flag{stringFlag(flagCheckpointID, "Read from this query checkpoint id (unsigned 64-bit decimal)")},
			operations: []operation{opGetTransaction},
		},
		{
			path:       []string{"transactions", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List transactions",
			long:       "Stream one page of transactions.",
			example:    "transactions list main --filter \"reference = 'payout-7'\"",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      listFlags("Transaction filter expression"),
			operations: []operation{opListTransactions},
			paginated:  true,
			collection: true,
		},
		{
			path:    []string{"transactions", "revert"},
			aliases: [][]string{nil, {"undo", "reverse"}},
			summary: "Revert a transaction",
			long:    "Commit the inverse of an existing transaction.",
			example: "transactions revert main 42",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argTransactionID, "Transaction id (unsigned 64-bit decimal)"),
			},
			flags: append([]sdk.Flag{
				boolFlag(flagForce, "Revert even when balance checks would reject the reversal"),
				boolFlag(flagAtEffectiveDate, "Date the reversal at the original transaction's effective date"),
				stringArrayFlag(flagMetadata, "Reversal metadata as key=value; repeat for several keys"),
			}, writeFlags()...),
			operations: []operation{opApplyRevertTransaction},
		},
		{
			path:    []string{"transactions", "set-metadata"},
			aliases: [][]string{nil, {"set-meta", "sm"}},
			summary: "Set transaction metadata",
			long:    "Merge metadata entries into a transaction.",
			example: "transactions set-metadata main 42 --metadata category=payout",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argTransactionID, "Transaction id (unsigned 64-bit decimal)"),
			},
			flags: append([]sdk.Flag{
				{Name: flagMetadata, Usage: "Metadata entry as key=value; repeat for several keys", Type: sdk.FlagStringArray, Required: true},
			}, writeFlags()...),
			operations: []operation{opApplyAddMetadata},
		},
	}
}
