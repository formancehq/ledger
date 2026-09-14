package ledgerv2

import (
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// Request and response ceilings. Every value stays inside the RFC 0011 common
// profile the host validates, and each command declares the tier its audited
// operation actually needs rather than the profile maximum everywhere.
const (
	requestNoBodyBytes int64 = 4 << 10
	requestJSONBytes   int64 = 64 << 10
	requestBulkBytes         = sdk.GeneratedClientHTTPMaxRequestBytes

	responseSmallBytes int64 = 64 << 10
	responseLargeBytes       = sdk.GeneratedClientHTTPMaxResponseBytes
)

// Header names a bound operation may carry. `accept` is present on every
// generated call; `idempotency-key` only on the operations whose
// `openapi/v2.yaml` definition declares the header.
var (
	headersRead        = []string{"accept"}
	headersIdempotent  = []string{"accept", "idempotency-key"}
	contentTypesJSON   = []string{"application/json"}
	contentTypesBinary = []string{"application/octet-stream"}
)

const (
	// mediaTypeJSON is the structured result media type the host renders.
	mediaTypeJSON = "application/json"
	// mediaTypeOctetStream is the opaque log payload `ledger export` returns.
	mediaTypeOctetStream = "application/octet-stream"

	// authStackCapability is the one host-resolved capability this plugin
	// consumes. The plugin never sees a token, endpoint or client secret.
	authStackCapability = "auth.stack"

	// productMajor is the only Ledger major this plugin supports.
	productMajor uint32 = 2
)

// outputKind selects the result contract one command publishes.
type outputKind uint8

const (
	// outputObject is one JSON resource document. A no-content product
	// response is published as the empty object, never as a fabricated field.
	outputObject outputKind = iota
	// outputCollection is one JSON page, or the complete bounded collection
	// when the host asks for every page.
	outputCollection
	// outputBinary is an opaque payload the host renders without parsing.
	outputBinary
)

// operationBinding is one audited `openapi/v2.yaml` operation and the exact
// transport envelope the generated client is allowed to produce for it.
type operationBinding struct {
	operationID         string
	method              string
	route               string
	scopes              []string
	requestContentTypes []string
	requestHeaders      []string
	maxRequestBytes     int64
	maxResponseBytes    int64
}

// commandSpec is one published command. The fields mirror inventory.json, so a
// descriptor drift is a test failure rather than a silent surface change.
type commandSpec struct {
	path      []string
	aliases   []string
	summary   string
	long      string
	example   string
	arguments []sdk.Argument
	flags     []sdk.Flag

	primary   operationBinding
	secondary []operationBinding

	paginated bool
	mutation  bool
	output    outputKind

	// maxHostRequests bounds the command's Host.Request budget. A paginated
	// command needs one request per page the host may ask for; every other
	// command needs exactly the number of operations it binds.
	maxHostRequests uint32

	inputArtifacts []sdk.InputArtifactSpec
}

func stringArgument(name, usage string, required bool) sdk.Argument {
	return sdk.Argument{
		Name:       name,
		Usage:      usage,
		Type:       sdk.ArgumentString,
		Required:   required,
		Completion: sdk.CompletionSpec{Kind: sdk.CompletionNone},
	}
}

func metadataArgument(usage string) sdk.Argument {
	return sdk.Argument{
		Name:       "metadata",
		Usage:      usage,
		Type:       sdk.ArgumentStringArray,
		Required:   true,
		Repeated:   true,
		Completion: sdk.CompletionSpec{Kind: sdk.CompletionNone},
	}
}

func stringFlag(name, usage string, required bool) sdk.Flag {
	return sdk.Flag{
		Name:       name,
		Usage:      usage,
		Type:       sdk.FlagString,
		Required:   required,
		Completion: sdk.CompletionSpec{Kind: sdk.CompletionNone},
	}
}

func stringArrayFlag(name, usage string) sdk.Flag {
	return sdk.Flag{
		Name:       name,
		Usage:      usage,
		Type:       sdk.FlagStringArray,
		Completion: sdk.CompletionSpec{Kind: sdk.CompletionNone},
	}
}

func int32Flag(name, usage string) sdk.Flag {
	return sdk.Flag{
		Name:       name,
		Usage:      usage,
		Type:       sdk.FlagInt32,
		Completion: sdk.CompletionSpec{Kind: sdk.CompletionNone},
	}
}

// ledgerFlag selects the ledger for commands whose historical grammar carries
// no ledger argument. It is required on purpose: the host contract bars an
// active-ledger fallback, so an omitted ledger fails before any product call
// instead of resolving to a remembered default.
func ledgerFlag() sdk.Flag {
	return stringFlag("ledger", "Name of the ledger (required; there is no active-ledger fallback)", true)
}

func pageSizeFlag() sdk.Flag {
	return int32Flag("page-size", "Maximum number of items in one page")
}

// idempotencyKeyFlag exposes the `Idempotency-Key` header only for the
// operations whose specification declares it. The key is never synthesised:
// a value derived from the request body would silently deduplicate a
// deliberate repeat send, and the host performs no retry that would need one.
// See auth-scopes-risks.md D4.
func idempotencyKeyFlag() sdk.Flag {
	return stringFlag("idempotency-key", "Value for the Idempotency-Key request header", false)
}
