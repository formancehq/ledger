package ledgerv2

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

//go:embed inventory.json
var inventoryBytes []byte

type inventoryEntry struct {
	Aliases        []string `json:"aliases"`
	Destructive    bool     `json:"destructive"`
	HTTPMethod     string   `json:"http_method"`
	HTTPPath       string   `json:"http_path"`
	IdempotencyKey bool     `json:"idempotency_key"`
	Included       bool     `json:"included"`
	Mutation       bool     `json:"mutation"`
	OperationID    string   `json:"operation_id"`
	Paginated      bool     `json:"paginated"`
	Path           string   `json:"path"`
	Scopes         []string `json:"scopes"`
}

type embeddedInventory struct {
	Commands []inventoryEntry `json:"commands"`
}

var (
	commandsOnce sync.Once
	commands     []sdk.Command
)

// Commands returns the exact 22-command Ledger-v2 catalogue. The checked-in
// inventory remains the operation-binding oracle while grammarFor supplies the
// independently audited CLI grammar.
func Commands() []sdk.Command {
	commandsOnce.Do(func() {
		var document embeddedInventory
		if err := json.Unmarshal(inventoryBytes, &document); err != nil {
			panic(fmt.Sprintf("ledger-v2: decode embedded inventory: %v", err))
		}
		for _, entry := range document.Commands {
			if !entry.Included {
				continue
			}
			commands = append(commands, commandFromInventory(entry))
		}
	})
	return append([]sdk.Command(nil), commands...)
}

func commandFromInventory(entry inventoryEntry) sdk.Command {
	grammar, ok := grammarFor(entry.Path)
	if !ok {
		panic("ledger-v2: missing grammar for " + entry.Path)
	}
	flags := append([]sdk.Flag(nil), grammar.flags...)
	if entry.IdempotencyKey {
		flags = append(flags, idempotencyKeyFlag())
	}
	path := strings.Fields(entry.Path)
	aliases := make([][]string, len(path))
	aliases[len(aliases)-1] = append([]string(nil), entry.Aliases...)

	generated := &sdk.HTTPGeneratedClientPolicy{
		MaxRequestBytes: requestNoBodyBytes,
		RequestHeaders:  append([]string(nil), headersRead...),
		ResponseLimits: sdk.ResponseLimits{
			MaxMessageBytes:   responseSmallBytes,
			MaxMessages:       1,
			MaxAggregateBytes: responseSmallBytes,
		},
	}
	if strings.Contains(entry.HTTPPath, "{") {
		generated.PathTemplate = entry.HTTPPath
	}
	if entry.Mutation {
		generated.MaxRequestBytes = requestJSONBytes
		generated.RequestContentTypes = append([]string(nil), contentTypesJSON...)
		generated.ResponseLimits.MaxMessageBytes = responseLargeBytes
		generated.ResponseLimits.MaxAggregateBytes = responseLargeBytes
	}
	if entry.OperationID == "v2ExportLogs" {
		generated.ResponseLimits.MaxMessageBytes = responseLargeBytes
		generated.ResponseLimits.MaxAggregateBytes = responseLargeBytes
	}
	if entry.OperationID == "v2ListAccounts" || entry.OperationID == "v2ListTransactions" || entry.OperationID == "v2GetVolumesWithBalances" {
		generated.MaxRequestBytes = requestJSONBytes
		generated.RequestContentTypes = append([]string(nil), contentTypesJSON...)
	}
	if entry.IdempotencyKey {
		generated.RequestHeaders = append([]string(nil), headersIdempotent...)
	}
	if entry.OperationID == "v2ImportLogs" {
		generated.MaxRequestBytes = requestBulkBytes
		generated.RequestContentTypes = append([]string(nil), contentTypesBinary...)
	}

	risk := sdk.RiskRead
	if entry.Mutation {
		risk = sdk.RiskMutation
	}
	output := outputObject
	if entry.Paginated || strings.HasSuffix(entry.Path, " list") {
		output = outputCollection
	}
	if entry.OperationID == "v2ExportLogs" {
		output = outputBinary
	}

	policy := sdk.OperationPolicy{
		ID:      entry.OperationID,
		Service: sdk.ServiceLedger,
		Scopes:  append([]string(nil), entry.Scopes...),
		HTTP: &sdk.HTTPOperationPolicy{
			Method:          entry.HTTPMethod,
			GeneratedClient: generated,
		},
	}
	operations := []sdk.OperationPolicy{policy}
	if entry.OperationID == "v2ImportLogs" {
		operations = append(operations, sdk.OperationPolicy{
			ID:      "v2ListLogs",
			Service: sdk.ServiceLedger,
			Scopes:  []string{"ledger:read"},
			HTTP: &sdk.HTTPOperationPolicy{
				Method: http.MethodGet,
				GeneratedClient: &sdk.HTTPGeneratedClientPolicy{
					PathTemplate:    "/v2/{ledger}/logs",
					MaxRequestBytes: requestNoBodyBytes,
					RequestHeaders:  append([]string(nil), headersRead...),
					ResponseLimits: sdk.ResponseLimits{
						MaxMessageBytes:   responseSmallBytes,
						MaxMessages:       1,
						MaxAggregateBytes: responseSmallBytes,
					},
				},
			},
		})
	}
	if relativeTransactionIDCommand(entry.Path) {
		operations = append(operations, sdk.OperationPolicy{
			ID:      "v2ListTransactions",
			Service: sdk.ServiceLedger,
			Scopes:  []string{"ledger:read"},
			HTTP: &sdk.HTTPOperationPolicy{
				Method: http.MethodGet,
				GeneratedClient: &sdk.HTTPGeneratedClientPolicy{
					PathTemplate:        "/v2/{ledger}/transactions",
					MaxRequestBytes:     requestJSONBytes,
					RequestContentTypes: append([]string(nil), contentTypesJSON...),
					RequestHeaders:      append([]string(nil), headersRead...),
					ResponseLimits: sdk.ResponseLimits{
						MaxMessageBytes:   responseSmallBytes,
						MaxMessages:       1,
						MaxAggregateBytes: responseSmallBytes,
					},
				},
			},
		})
	}
	if generated.PathTemplate == "" {
		policy.HTTP.Path = entry.HTTPPath
	}

	mediaType := mediaTypeJSON
	rawSchema := objectOutputSchema()
	if output == outputCollection {
		rawSchema = collectionOutputSchema()
	}
	if output == outputBinary {
		mediaType = mediaTypeOctetStream
		rawSchema = binaryOutputSchema(mediaType)
	}

	maxRequests := uint32(len(operations))
	if entry.Paginated {
		maxRequests = sdk.DefaultAllPagesMaxPages
	}
	if entry.OperationID == "v2ImportLogs" {
		maxRequests = sdk.PortableMaxHostRequests
	}
	return sdk.Command{
		ID:                 "ledger.v2." + strings.Join(path[1:], "."),
		ExecutionKind:      sdk.ExecutionKindService,
		AuthMode:           sdk.AuthModeCapability,
		Path:               path,
		PathAliases:        aliases,
		Target:             sdk.TargetRequirement{Kind: sdk.TargetStack},
		Summary:            "Execute " + entry.Path + " against Ledger v2.",
		Long:               "Calls the audited " + entry.OperationID + " operation through the host-owned generated HTTP transport.",
		Example:            "fctl " + entry.Path,
		Arguments:          grammar.arguments,
		Flags:              flags,
		Auth:               []sdk.AuthRequirement{{Capability: authStackCapability}},
		Operations:         operations,
		Compatibility:      []sdk.ServiceCompatibility{{Service: sdk.ServiceLedger, Majors: []uint32{productMajor}}},
		Risk:               risk,
		InputSchema:        buildInputSchema(grammar.arguments, flags),
		RawOutputSchema:    rawSchema,
		PublicOutputSchema: rawSchema,
		InputArtifacts:     grammar.artifacts,
		Pagination:         sdk.PaginationSpec{Supported: entry.Paginated},
		OutputMediaType:    mediaType,
		ExecutionPolicy:    &sdk.CommandExecutionPolicy{MaxHostRequests: maxRequests},
	}
}

func relativeTransactionIDCommand(path string) bool {
	switch path {
	case "ledger transactions show", "ledger transactions set-metadata", "ledger transactions delete-metadata", "ledger transactions revert":
		return true
	default:
		return false
	}
}

type commandGrammar struct {
	arguments []sdk.Argument
	flags     []sdk.Flag
	artifacts []sdk.InputArtifactSpec
}

func grammarFor(path string) (commandGrammar, bool) {
	g, ok := commandGrammars[path]
	return g, ok
}

func arg(name string, required bool) sdk.Argument {
	return stringArgument(name, name, required)
}

func flag(name string) sdk.Flag { return stringFlag(name, name, false) }

func repeatedFlag(name string) sdk.Flag { return stringArrayFlag(name, name) }

func boolFlag(name string) sdk.Flag {
	return sdk.Flag{Name: name, Usage: name, Type: sdk.FlagBool, Completion: sdk.CompletionSpec{Kind: sdk.CompletionNone}}
}

func withDefault(value sdk.Flag, defaultValue string) sdk.Flag {
	value.HasDefault = true
	value.DefaultValue = defaultValue
	return value
}

func ledgerFlags(extra ...sdk.Flag) []sdk.Flag {
	return append([]sdk.Flag{ledgerFlag()}, extra...)
}

var commandGrammars = map[string]commandGrammar{
	"ledger accounts delete-metadata": {arguments: []sdk.Argument{arg("address", true), arg("key", true)}, flags: ledgerFlags()},
	"ledger accounts list":            {flags: ledgerFlags(repeatedFlag("metadata"), pageSizeFlag(), flag("cursor"))},
	"ledger accounts set-metadata":    {arguments: []sdk.Argument{arg("address", true), metadataArgument("metadata")}, flags: ledgerFlags()},
	"ledger accounts show":            {arguments: []sdk.Argument{arg("address", true)}, flags: ledgerFlags()},
	"ledger create":                   {arguments: []sdk.Argument{arg("name", true)}, flags: []sdk.Flag{flag("bucket"), repeatedFlag("features"), repeatedFlag("metadata")}},
	"ledger delete-metadata":          {arguments: []sdk.Argument{arg("ledger-name", true), arg("key", true)}},
	"ledger export":                   {flags: ledgerFlags()},
	"ledger import":                   {arguments: []sdk.Argument{arg("ledger-name", true), arg("file-path", true)}, flags: []sdk.Flag{boolFlag("resume-from-last-log")}, artifacts: []sdk.InputArtifactSpec{{ArgumentName: "file-path", MediaTypes: contentTypesBinary, MaxBytes: inputArtifactBytes, AllowFile: true}}},
	"ledger list":                     {},
	"ledger schemas get":              {arguments: []sdk.Argument{arg("version", true)}, flags: ledgerFlags()},
	"ledger schemas insert":           {arguments: []sdk.Argument{arg("version", true), arg("source", true)}, flags: ledgerFlags(), artifacts: []sdk.InputArtifactSpec{{ArgumentName: "source", MediaTypes: contentTypesBinary, MaxBytes: requestJSONBytes, AllowFile: true}}},
	"ledger schemas list":             {flags: ledgerFlags(flag("cursor"), withDefault(pageSizeFlag(), "15"))},
	// The SDK deliberately rejects an optional positional before required
	// positionals. Preserve the send semantics with an explicit --source flag.
	"ledger send":                         {arguments: []sdk.Argument{arg("destination", true), arg("amount", true), arg("asset", true)}, flags: ledgerFlags(flag("source"), repeatedFlag("metadata"), flag("reference"))},
	"ledger set-metadata":                 {arguments: []sdk.Argument{arg("ledger-name", true), metadataArgument("metadata")}},
	"ledger stats":                        {flags: ledgerFlags()},
	"ledger transactions delete-metadata": {arguments: []sdk.Argument{arg("transaction-id", true), arg("key", true)}, flags: ledgerFlags()},
	"ledger transactions list":            {flags: ledgerFlags(flag("account"), flag("dst"), flag("end"), repeatedFlag("metadata"), withDefault(pageSizeFlag(), "5"), flag("reference"), flag("src"), flag("start"), flag("cursor"))},
	"ledger transactions num":             {arguments: []sdk.Argument{arg("file", true)}, flags: ledgerFlags(repeatedFlag("account-var"), repeatedFlag("amount-var"), repeatedFlag("metadata"), repeatedFlag("portion-var"), flag("reference"), flag("timestamp")), artifacts: []sdk.InputArtifactSpec{{ArgumentName: "file", MediaTypes: []string{"text/plain"}, MaxBytes: requestJSONBytes, AllowFile: true, AllowStdin: true}}},
	"ledger transactions revert":          {arguments: []sdk.Argument{arg("transaction-id", true)}, flags: ledgerFlags(boolFlag("at-effective-date"), boolFlag("force"))},
	"ledger transactions set-metadata":    {arguments: []sdk.Argument{arg("transaction-id", true), metadataArgument("metadata")}, flags: ledgerFlags()},
	"ledger transactions show":            {arguments: []sdk.Argument{arg("transaction-id", true)}, flags: ledgerFlags()},
	"ledger volumes list":                 {flags: ledgerFlags(flag("address"), flag("cursor"), flag("end-time"), withDefault(int32Flag("group-by", "group-by"), "0"), boolFlag("insertion-date"), repeatedFlag("metadata"), withDefault(pageSizeFlag(), "10"), flag("start-time"))},
}
