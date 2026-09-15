package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	flagVersion = "version"
	flagScript  = "script"
)

// maxNumscriptBytes bounds a submitted Numscript source document.
const maxNumscriptBytes int64 = 1 << 20

// numscriptsSpecs covers the four `numscripts` product commands.
func numscriptsSpecs() []spec {
	return []spec{
		{
			path:    []string{"numscripts", "get"},
			aliases: [][]string{nil, {"g", "show", "describe"}},
			summary: "Show a stored Numscript",
			long:    "Read one stored Numscript. Without a version the greatest stored semver is returned.",
			example: "numscripts get main payout --version 1.2.0",
			risk:    sdk.RiskRead,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Numscript name"),
			},
			flags:      append([]sdk.Flag{stringFlag(flagVersion, "Semantic version; empty selects the greatest stored version")}, readFlags()...),
			operations: []operation{opGetNumscript},
		},
		{
			path:       []string{"numscripts", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List stored Numscripts",
			long:       "Stream one page of the Numscripts stored on a ledger.",
			example:    "numscripts list main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      listFlagsWithoutFilter(),
			operations: []operation{opListNumscripts},
			paginated:  true,
			collection: true,
		},
		{
			path:    []string{"numscripts", "save"},
			summary: "Save a Numscript version",
			long:    "Store an immutable Numscript version and advance the latest pointer when it is the greatest stored semver.",
			example: "numscripts save main payout --version 1.2.0 --script ./payout.num",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Numscript name"),
			},
			flags: append([]sdk.Flag{
				{Name: flagVersion, Usage: "Semantic version to store", Type: sdk.FlagString, Required: true},
				{Name: flagScript, Usage: "Numscript source, read from a file or from stdin", Type: sdk.FlagString, Required: true},
			}, writeFlags()...),
			operations: []operation{opApplySaveNumscript},
			artifacts: []sdk.InputArtifactSpec{{
				FlagName:   flagScript,
				MediaTypes: []string{"text/plain"},
				MaxBytes:   maxNumscriptBytes,
				AllowFile:  true,
				AllowStdin: true,
			}},
		},
		{
			path:    []string{"numscripts", "versions"},
			summary: "List the versions of one Numscript",
			long:    "Read every stored version of a single Numscript together with the latest pointer.",
			example: "numscripts versions main payout",
			risk:    sdk.RiskRead,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argName, "Numscript name"),
			},
			flags:      readFlags(),
			operations: []operation{opListNumscriptVersions},
		},
	}
}
