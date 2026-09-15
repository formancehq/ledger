package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	flagQueryTarget = "target"
	flagParameter   = "parameter"
	flagQueryMode   = "query-mode"
)

func queryTargetFlag(required bool) sdk.Flag {
	flag := stringFlag(flagQueryTarget, "Query target: accounts, transactions or logs")
	flag.Required = required
	if !required {
		flag.HasDefault = true
		flag.DefaultValue = "accounts"
	}
	flag.Completion = staticCompletion("accounts", "transactions", "logs")
	return flag
}

// queriesSpecs covers the five `queries` product commands.
//
// create, update and delete travel through Apply in the current release/v3.0
// API and therefore declare signing plus an idempotency key like other writes.
func queriesSpecs() []spec {
	return []spec{
		{
			path:    []string{"queries", "create"},
			summary: "Create a prepared query",
			long:    "Store a named, reusable filter against one query target.",
			example: "queries create main big-transfers --target transactions --filter \"amount >= 1000\"",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Prepared query name"),
			},
			flags: append([]sdk.Flag{
				queryTargetFlag(false),
				{Name: flagFilter, Usage: "Optional filter expression parsed against the query target", Type: sdk.FlagString},
			}, writeFlags()...),
			operations: []operation{opApplyCreatePreparedQuery},
		},
		{
			path:    []string{"queries", "delete"},
			aliases: [][]string{nil, {"rm"}},
			summary: "Delete a prepared query",
			long:    "Remove a stored prepared query from a ledger.",
			example: "queries delete main big-transfers",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Prepared query name"),
			},
			flags:      writeFlags(),
			operations: []operation{opApplyDeletePreparedQuery},
		},
		{
			path:    []string{"queries", "execute"},
			aliases: [][]string{nil, {"exec", "run"}},
			summary: "Execute a prepared query",
			long:    "Run a stored prepared query and return one page or the host-bounded all-pages collection.",
			example: "queries execute main big-transfers --parameter threshold=1000",
			risk:    sdk.RiskRead,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Prepared query name"),
			},
			flags: []sdk.Flag{
				stringArrayFlag(flagParameter, "Query parameter as name=value; repeat for several parameters"),
				{Name: flagPageSize, Usage: "Maximum items per page", Type: sdk.FlagInt32, HasDefault: true, DefaultValue: defaultPageSize},
				stringFlag(flagCursor, "Opaque cursor returned by a previous page"),
				func() sdk.Flag {
					mode := stringFlag(flagQueryMode, "Execution mode: list or aggregate-volumes")
					mode.HasDefault = true
					mode.DefaultValue = "list"
					mode.Completion = staticCompletion("list", "aggregate-volumes")
					return mode
				}(),
			},
			operations: []operation{opExecutePreparedQuery},
			paginated:  true,
			collection: true,
		},
		{
			path:       []string{"queries", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List prepared queries",
			long:       "Read every prepared query stored on a ledger.",
			example:    "queries list main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			operations: []operation{opListPreparedQueries},
			collection: true,
		},
		{
			path:    []string{"queries", "update"},
			summary: "Update a prepared query's filter",
			long:    "Replace the filter of a stored prepared query, keeping its name and target.",
			example: "queries update main big-transfers --filter \"amount >= 5000\"",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Prepared query name"),
			},
			flags: append([]sdk.Flag{
				{Name: flagFilter, Usage: "Replacement filter expression", Type: sdk.FlagString, Required: true},
			}, writeFlags()...),
			operations: []operation{opApplyUpdatePreparedQuery},
		},
	}
}
