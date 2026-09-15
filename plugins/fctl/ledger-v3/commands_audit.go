package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const argSequence = "sequence"

// auditSpecs covers the two `audit` product commands. The audit chain is
// bucket-wide, so neither command takes a ledger.
func auditSpecs() []spec {
	return []spec{
		{
			path:       []string{"audit", "get"},
			summary:    "Show one audit entry",
			long:       "Read a single entry of the cryptographically chained audit log by its global sequence.",
			example:    "audit get 4211",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{requiredStringArgument(argSequence, "Audit entry sequence (unsigned 64-bit decimal)")},
			operations: []operation{opGetAuditEntry},
		},
		{
			path:       []string{"audit", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List audit entries",
			long:       "Stream one page of the chained audit log.",
			example:    "audit list --page-size 100",
			risk:       sdk.RiskRead,
			flags:      listFlags("Audit filter expression"),
			operations: []operation{opListAuditEntries},
			paginated:  true,
			collection: true,
		},
	}
}
