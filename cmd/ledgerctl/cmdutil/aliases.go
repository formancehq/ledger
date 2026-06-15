package cmdutil

// Standardized command aliases shared across ledgerctl subcommands.
//
// See docs/ops/cli.md "Shared Flag Contract" for the contract.
var (
	ListAliases    = []string{"ls", "l"}
	GetAliases     = []string{"g", "show", "describe"}
	InspectAliases = []string{"i"}
)
