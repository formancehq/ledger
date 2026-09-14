package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	flagConfiguration = "configuration"
	flagMetadataType  = "metadata-type"
)

// maxConfigurationBytes bounds a submitted declarative configuration document.
const maxConfigurationBytes int64 = 1 << 20

// configurationReadRequests bounds a compound configuration read: one
// GetLedger, one ListIndexes stream, one ListPreparedQueries call, and at most
// the host's one-hundred-page Numscript traversal.
const configurationReadRequests uint32 = 103

func metadataTypeFlag() sdk.Flag {
	flag := stringFlag(flagMetadataType, "Metadata value type")
	flag.Required = true
	flag.Completion = staticCompletion(
		"string", "int64", "bool", "uint64", "int8", "int16", "int32",
		"uint8", "uint16", "uint32", "datetime",
	)
	return flag
}

func targetTypeFlag() sdk.Flag {
	flag := stringFlag(flagTargetType, "Metadata target: account, transaction or ledger")
	flag.Required = true
	flag.Completion = staticCompletion("account", "transaction", "ledger")
	return flag
}

// ledgersSpecs covers the thirteen `ledgers` product commands.
//
// `ledgers promote` is deliberately absent: it moves a mirror ledger into
// normal mode, a replication-topology action classified operator in the
// preparation exclusions, so the `ledgers` family carries thirteen product
// commands rather than fourteen.
func ledgersSpecs() []spec {
	configurationReads := []operation{opGetLedger, opListIndexes, opListPreparedQueries, opListNumscripts}

	return []spec{
		{
			path:        []string{"ledgers", "configuration"},
			aliases:     [][]string{nil, {"config", "conf"}},
			summary:     "Show a ledger's declarative configuration",
			long:        "Read the schema, account types, indexes and prepared queries that make up a ledger's configuration.",
			example:     "ledgers configuration main",
			risk:        sdk.RiskRead,
			arguments:   []sdk.Argument{ledgerArgument()},
			operations:  configurationReads,
			maxRequests: configurationReadRequests,
		},
		{
			path:      []string{"ledgers", "configuration", "apply"},
			aliases:   [][]string{nil, {"config", "conf"}, nil},
			summary:   "Apply a declarative ledger configuration",
			long:      "Submit a configuration document as one ApplyBatch. The batch is a single proposal: the server accepts or rejects it whole.",
			example:   "ledgers configuration apply main --configuration ./ledger.yaml",
			risk:      sdk.RiskMutation,
			arguments: []sdk.Argument{ledgerArgument()},
			flags: append([]sdk.Flag{
				{Name: flagConfiguration, Usage: "Configuration document, read from a file or from stdin", Type: sdk.FlagString, Required: true},
			}, writeFlags()...),
			operations:    append(append([]operation(nil), configurationReads...), opApplyConfiguration),
			scopeOverride: configurationScopes,
			maxRequests:   configurationReadRequests + 1,
			artifacts: []sdk.InputArtifactSpec{{
				FlagName:   flagConfiguration,
				MediaTypes: []string{"application/json", "application/yaml"},
				MaxBytes:   maxConfigurationBytes,
				AllowFile:  true,
				AllowStdin: true,
			}},
		},
		{
			path:        []string{"ledgers", "configuration", "export"},
			aliases:     [][]string{nil, {"config", "conf"}, nil},
			summary:     "Export a ledger's declarative configuration",
			long:        "Read a ledger's configuration in the same shape `ledgers configuration apply` accepts.",
			example:     "ledgers configuration export main",
			risk:        sdk.RiskRead,
			arguments:   []sdk.Argument{ledgerArgument()},
			operations:  configurationReads,
			maxRequests: configurationReadRequests,
		},
		{
			path:      []string{"ledgers", "create"},
			aliases:   [][]string{nil, {"new", "add"}},
			summary:   "Create a ledger",
			long:      "Create a ledger, optionally seeding its metadata schema and chart enforcement mode.",
			example:   "ledgers create main",
			risk:      sdk.RiskMutation,
			arguments: []sdk.Argument{requiredStringArgument(argName, "Ledger name")},
			flags: append([]sdk.Flag{
				stringArrayFlag(flagMetadataType, "Initial schema entry as target:key:type; repeat for several entries"),
				func() sdk.Flag {
					flag := stringFlag(flagEnforcementMode, "Chart enforcement for unmatched accounts: strict or audit")
					flag.Completion = staticCompletion("strict", "audit")
					return flag
				}(),
			}, writeFlags()...),
			operations: []operation{opApplyCreateLedger},
		},
		{
			path:       []string{"ledgers", "delete"},
			aliases:    [][]string{nil, {"rm", "del", "remove"}},
			summary:    "Delete a ledger",
			long:       "Delete a ledger and schedule the deferred purge of its data.",
			example:    "ledgers delete main",
			risk:       sdk.RiskMutation,
			arguments:  []sdk.Argument{requiredStringArgument(argName, "Ledger name")},
			flags:      writeFlags(),
			operations: []operation{opApplyDeleteLedger},
		},
		{
			path:    []string{"ledgers", "delete-metadata"},
			aliases: [][]string{nil, {"del-meta", "dm", "rm-meta"}},
			summary: "Delete one ledger metadata key",
			long:    "Remove a single metadata key from the ledger itself.",
			example: "ledgers delete-metadata main owner",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argKey, "Metadata key to delete"),
			},
			flags:      writeFlags(),
			operations: []operation{opApplyDeleteLedgerMetadata},
		},
		{
			path:       []string{"ledgers", "get"},
			aliases:    [][]string{nil, {"g", "show", "describe"}},
			summary:    "Show one ledger",
			long:       "Read a ledger's mode, metadata schema, account types and boundaries.",
			example:    "ledgers get main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      readFlags(),
			operations: []operation{opGetLedger},
		},
		{
			path:       []string{"ledgers", "get-schema"},
			aliases:    [][]string{nil, {"schema", "gs"}},
			summary:    "Show a ledger's metadata schema status",
			long:       "Read the declared metadata field types and their per-replica status.",
			example:    "ledgers get-schema main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			operations: []operation{opGetMetadataSchema},
		},
		{
			path:       []string{"ledgers", "list"},
			aliases:    [][]string{nil, {"ls", "l"}},
			summary:    "List ledgers",
			long:       "Stream one page of the bucket's ledgers.",
			example:    "ledgers list",
			risk:       sdk.RiskRead,
			flags:      listFlagsWithoutFilter(),
			operations: []operation{opListLedgers},
			paginated:  true,
			collection: true,
		},
		{
			path:    []string{"ledgers", "remove-metadata-type"},
			aliases: [][]string{nil, {"rm-type", "rmt"}},
			summary: "Remove a metadata field type",
			long:    "Drop one declared metadata field type from a ledger's schema.",
			example: "ledgers remove-metadata-type main --target-type transaction category",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argKey, "Metadata key"),
			},
			flags:      append([]sdk.Flag{targetTypeFlag()}, writeFlags()...),
			operations: []operation{opApplyRemoveMetadataType},
		},
		{
			path:      []string{"ledgers", "set-metadata"},
			aliases:   [][]string{nil, {"set-meta", "sm"}},
			summary:   "Set ledger metadata",
			long:      "Merge metadata entries into the ledger itself.",
			example:   "ledgers set-metadata main --metadata owner=treasury",
			risk:      sdk.RiskMutation,
			arguments: []sdk.Argument{ledgerArgument()},
			flags: append([]sdk.Flag{
				{Name: flagMetadata, Usage: "Metadata entry as key=value; repeat for several keys", Type: sdk.FlagStringArray, Required: true},
			}, writeFlags()...),
			operations: []operation{opApplySaveLedgerMetadata},
		},
		{
			path:    []string{"ledgers", "set-metadata-type"},
			aliases: [][]string{nil, {"set-type", "smt"}},
			summary: "Declare a metadata field type",
			long:    "Declare the type of one metadata key so the ledger can validate and index it.",
			example: "ledgers set-metadata-type main --target-type transaction --metadata-type string category",
			risk:    sdk.RiskMutation,
			arguments: []sdk.Argument{
				ledgerArgument(),
				requiredStringArgument(argKey, "Metadata key"),
			},
			flags:      append([]sdk.Flag{targetTypeFlag(), metadataTypeFlag()}, writeFlags()...),
			operations: []operation{opApplySetMetadataFieldType},
		},
		{
			path:       []string{"ledgers", "stats"},
			aliases:    [][]string{nil, {"st"}},
			summary:    "Show a ledger's statistics",
			long:       "Read counters and boundaries for one ledger.",
			example:    "ledgers stats main",
			risk:       sdk.RiskRead,
			arguments:  []sdk.Argument{ledgerArgument()},
			flags:      []sdk.Flag{stringFlag(flagCheckpointID, "Read from this query checkpoint id (unsigned 64-bit decimal)")},
			operations: []operation{opGetLedgerStats},
		},
	}
}
