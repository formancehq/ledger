package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	flagPattern         = "pattern"
	flagPersistence     = "persistence"
	flagSegmentType     = "segment-type"
	flagEnforcementMode = "enforcement-mode"
)

func persistenceFlag() sdk.Flag {
	flag := stringFlag(flagPersistence, "Volume persistence: normal, ephemeral or transient")
	flag.HasDefault = true
	flag.DefaultValue = "normal"
	flag.Completion = staticCompletion("normal", "ephemeral", "transient")
	return flag
}

func enforcementModeFlag() sdk.Flag {
	flag := stringFlag(flagEnforcementMode, "Chart enforcement for unmatched accounts: strict or audit")
	flag.Completion = staticCompletion("strict", "audit")
	flag.Required = true
	return flag
}

// accountTypesSpecs covers the five `account-types` product commands.
//
// `get` and `list` read the chart of accounts from GetLedger rather than from
// ledgerctl's GetAllLedgersInfo helper. That helper reaches ListLedgers through
// cmdutil.DrainAllPages, an unbounded page drain the plugin is barred from
// reusing (preparation risk R3); requiring the ledger argument also removes the
// interactive ledger prompt ledgerctl falls back to (divergence D4 / open
// decision O3) without inventing a selection rule.
func accountTypesSpecs() []spec {
	return []spec{
		{
			path:    []string{"account-types", "add"},
			summary: "Add an account type to a ledger's chart of accounts",
			long:    "Declare one account type: a name, an address pattern and how its volumes are persisted.",
			example: "account-types add main user-checking --pattern 'users:{id}:checking'",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Account type name"),
			},
			flags: append([]sdk.Flag{
				{Name: flagPattern, Usage: "Address pattern, for example users:{id}:checking", Type: sdk.FlagString, Required: true},
				persistenceFlag(),
				stringArrayFlag(flagSegmentType, "Segment constraint as variable=regex:<pattern>, variable=uuid, variable=uint64 or variable=bytes"),
			}, writeFlags()...),
			operations: []operation{opApplyAddAccountType},
		},
		{
			path:    []string{"account-types", "get"},
			summary: "Show one account type",
			long:    "Read a single account type from the ledger's chart of accounts.",
			example: "account-types get main user-checking",
			risk:    sdk.RiskRead,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Account type name"),
			},
			flags:      readFlags(),
			operations: []operation{opGetLedger},
		},
		{
			path:       []string{"account-types", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List a ledger's account types",
			long:       "Read the complete chart of accounts declared on one ledger.",
			example:    "account-types list main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      readFlags(),
			operations: []operation{opGetLedger},
			collection: true,
		},
		{
			path:    []string{"account-types", "remove"},
			aliases: [][]string{nil, {"rm", "delete"}},
			summary: "Remove an account type",
			long:    "Delete one account type from the ledger's chart of accounts.",
			example: "account-types remove main user-checking",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Account type name"),
			},
			flags:      writeFlags(),
			operations: []operation{opApplyRemoveAccountType},
		},
		{
			path:       []string{"account-types", "set-default-enforcement"},
			summary:    "Set the default chart enforcement mode",
			long:       "Choose whether addresses matching no account type are rejected (strict) or only reported (audit).",
			example:    "account-types set-default-enforcement main --enforcement-mode audit",
			risk:       sdk.RiskMutation,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      append([]sdk.Flag{enforcementModeFlag()}, writeFlags()...),
			operations: []operation{opApplyDefaultEnforcement},
		},
	}
}

func staticCompletion(values ...string) sdk.CompletionSpec {
	candidates := make([]sdk.CompletionCandidate, 0, len(values))
	for _, value := range values {
		candidates = append(candidates, sdk.CompletionCandidate{Value: value})
	}
	return sdk.CompletionSpec{Kind: sdk.CompletionStatic, Candidates: candidates, NoFileCompletion: true}
}
