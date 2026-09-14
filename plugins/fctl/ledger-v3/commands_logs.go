package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

// logsSpecs covers the two `logs` product commands. GetLog is bucket-wide and
// the server gates it on ledger:OpsRead, while ListLogs is per-ledger and gated
// on ledger:LedgerRead; the descriptors record that asymmetry rather than
// smoothing it over.
func logsSpecs() []spec {
	return []spec{
		{
			path:       []string{"logs", "get"},
			summary:    "Show one log entry",
			long:       "Read a single log entry by its global sequence.",
			example:    "logs get 90210",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{requiredStringArgument(argSequence, "Log sequence (unsigned 64-bit decimal)")},
			flags:      []sdk.Flag{stringFlag(flagCheckpointID, "Read from this query checkpoint id (unsigned 64-bit decimal)")},
			operations: []operation{opGetLog},
		},
		{
			path:       []string{"logs", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List a ledger's logs",
			long:       "Stream one page of the log stream for a ledger.",
			example:    "logs list main --page-size 100",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      listFlagsWithoutReverse("Log filter expression"),
			operations: []operation{opListLogs},
			paginated:  true,
			collection: true,
		},
	}
}
