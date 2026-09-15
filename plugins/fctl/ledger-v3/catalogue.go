package ledgerv3

import (
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// productMajor is the only Ledger major this plugin serves. ledger-v2 owns the
// HTTP majors; nothing is shared between the two catalogues.
const productMajor uint32 = 3

// signingPayloadType is the logical fctl identifier that RFC 0009 binds to the
// protobuf message ledger.ApplyBatch.
const signingPayloadType = "formance.ledger.v3.ApplyBatch"

const (
	signingAlgorithm          = "Ed25519"
	signingMaxKeyIDBytes      = uint32(1024)
	signingSignatureBytes     = uint32(64)
	protobufWireTypeVarint    = uint32(0)
	protobufWireTypeDelimited = uint32(2)
)

// spec is the single source for one command. Descriptor fields the host
// validates against each other — input schema versus grammar, pagination versus
// operation count, signing versus compatibility — are all derived from it, so
// 48 descriptors cannot drift field by field.
type spec struct {
	path      []string
	aliases   [][]string
	summary   string
	long      string
	example   string
	risk      sdk.Risk
	arguments []sdk.Argument
	flags     []sdk.Flag
	// operations lists every product RPC the command may issue. A paginated
	// command declares exactly one.
	operations []operation
	// scopeOverride replaces the single-operation scope for a compound Apply
	// whose batch spans several request variants.
	scopeOverride []string
	// artifacts declare host-supplied documents (a Numscript source, a
	// configuration file) that arrive through a flag rather than inline.
	artifacts   []sdk.InputArtifactSpec
	paginated   bool
	collection  bool
	ledgerInfo  bool
	maxRequests uint32
}

func (s spec) id() string {
	return "ledger.v3." + strings.Join(s.path, ".")
}

// command materialises the host descriptor. Signing is attached whenever one
// declared operation is /ledger.BucketService/Apply, the only method RFC 0009
// grants sign.ledger.apply-batch over. Compound configuration commands may
// also declare the reads needed to compute their atomic batch.
func (s spec) command() sdk.Command {
	policies := make([]sdk.OperationPolicy, 0, len(s.operations))
	for _, item := range s.operations {
		policy := item.policy()
		if len(s.scopeOverride) != 0 && item.method == bucketFullMethod("Apply") {
			policy.Scopes = append([]string(nil), s.scopeOverride...)
		}
		policies = append(policies, policy)
	}

	render := renderHintFor(s.id())
	var columns []sdk.TableColumn
	if render.Table != nil {
		columns = render.Table.Columns
	}
	outputSchema := outputSchemaFor(s.collection, columns)
	if s.ledgerInfo {
		outputSchema = ledgerInfoOutputSchema(s.collection)
	}

	// A paginated command must be able to serve the host's all-pages
	// continuation, which walks up to sdk.DefaultAllPagesMaxPages pages and so
	// needs one host request per page. Deriving the budget keeps the descriptor
	// and the host's own ceiling from drifting apart.
	maxRequests := s.maxRequests
	if maxRequests == 0 {
		maxRequests = 1
		if s.paginated {
			maxRequests = sdk.DefaultAllPagesMaxPages
		}
	}

	command := sdk.Command{
		ID:                 s.id(),
		ExecutionKind:      sdk.ExecutionKindService,
		AuthMode:           sdk.AuthModeCapability,
		Path:               s.path,
		PathAliases:        s.aliases,
		Target:             sdk.TargetRequirement{Kind: sdk.TargetStack},
		Summary:            s.summary,
		Long:               s.long,
		Example:            s.example,
		Arguments:          normalizeArguments(s.arguments),
		Flags:              normalizeFlags(s.flags),
		Auth:               []sdk.AuthRequirement{{Capability: stackAuthCapability}},
		Operations:         policies,
		Compatibility:      []sdk.ServiceCompatibility{{Service: sdk.ServiceLedger, Majors: []uint32{productMajor}}},
		Risk:               s.risk,
		InputSchema:        buildInputSchema(s.arguments, s.flags),
		RawOutputSchema:    outputSchema,
		PublicOutputSchema: outputSchema,
		InputArtifacts:     s.artifacts,
		Pagination:         sdk.PaginationSpec{Supported: s.paginated},
		OutputMediaType:    "application/json",
		Render:             render,
		ExecutionPolicy:    &sdk.CommandExecutionPolicy{MaxHostRequests: maxRequests},
	}

	for _, operation := range s.operations {
		if operation.method == bucketFullMethod("Apply") {
			maxPayloadBytes, maxSignedMessageBytes := operation.signingCeilings()
			command.RequestSigning = &sdk.RequestSigningSpec{
				Capability:   sdk.CapabilitySignLedgerApplyBatch,
				ProductMajor: productMajor,
				PayloadType:  signingPayloadType,
				OperationID:  operation.id,
				Algorithm:    signingAlgorithm,
				Protobuf: &sdk.OpaqueProtobufSigningRecipe{
					UnsignedField:          1,
					SignedField:            2,
					EnvelopeKeyIDField:     1,
					EnvelopeSignatureField: 2,
					EnvelopePayloadField:   3,
					PassthroughFields: []sdk.ProtobufField{
						{Number: 3, WireType: protobufWireTypeDelimited},
						{Number: 4, WireType: protobufWireTypeVarint},
					},
					MaxPayloadBytes:       maxPayloadBytes,
					MaxSignedMessageBytes: maxSignedMessageBytes,
					MaxKeyIDBytes:         signingMaxKeyIDBytes,
					SignatureLength:       signingSignatureBytes,
				},
			}
			break
		}
	}

	return command
}

var (
	objectSchema     = []byte(`{"$schema":"` + schemaDialect + `","type":"object"}`)
	collectionSchema = []byte(`{"$schema":"` + schemaDialect + `","type":"array"}`)
)

// stackAuthCapability is the only capability this plugin requires. The host
// owns endpoint, credential and target resolution; the plugin never sees them.
const stackAuthCapability = "auth.stack"

// catalogue is the frozen 48-command product surface. Order is the catalogue's
// declaration order and is stable across builds.
func catalogue() []spec {
	specs := make([]spec, 0, 48)
	specs = append(specs, accountTypesSpecs()...)
	specs = append(specs, accountsSpecs()...)
	specs = append(specs, auditSpecs()...)
	specs = append(specs, indexesSpecs()...)
	specs = append(specs, ledgersSpecs()...)
	specs = append(specs, logsSpecs()...)
	specs = append(specs, numscriptsSpecs()...)
	specs = append(specs, queriesSpecs()...)
	specs = append(specs, transactionsSpecs()...)
	return specs
}

// normalizeArguments and normalizeFlags fill in the explicit "no completion"
// kind. The host rejects an unset completion kind, and defaulting here keeps
// every spec free of the boilerplate.
func normalizeArguments(arguments []sdk.Argument) []sdk.Argument {
	if len(arguments) == 0 {
		return nil
	}
	out := make([]sdk.Argument, len(arguments))
	copy(out, arguments)
	for index := range out {
		if out[index].Completion.Kind == "" {
			out[index].Completion.Kind = sdk.CompletionNone
		}
	}
	return out
}

func normalizeFlags(flags []sdk.Flag) []sdk.Flag {
	if len(flags) == 0 {
		return nil
	}
	out := make([]sdk.Flag, len(flags))
	copy(out, flags)
	for index := range out {
		if out[index].Completion.Kind == "" {
			out[index].Completion.Kind = sdk.CompletionNone
		}
	}
	return out
}
