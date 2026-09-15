package sdk

import "context"

const (
	MaxInputArtifactHandleBytes = 4 << 10
	MaxInputArtifactChunkBytes  = 4 << 20
)

// Plugin is a transport-independent product command implementation.
type Plugin interface {
	Metadata() Metadata
	Commands() []Command
	Execute(context.Context, ExecuteRequest, Host) error
}

// Completer is an optional, bounded value-completion facet.
type Completer interface {
	Complete(context.Context, CompleteRequest, Host) (CompleteResponse, error)
}

// DocumentedPlugin optionally contributes digest-covered or inert linked
// documentation to Describe.
type DocumentedPlugin interface {
	DocumentationResources() []DocumentationResource
}

// Host exposes the only runtime operations available to a plugin core.
type Host interface {
	Request(context.Context, Request) (Responses, error)
	Wait(context.Context, WaitRequest) error
	Emit(Event) error
	Log(Level, string)
}

// InputHost is the bounded input-artifact extension implemented by hosts that
// advertise HostCapabilityInputArtifactV1. The opaque handle is valid only for
// the current execution. Each call returns at most one 4 MiB chunk; the final
// response consumes the handle and a later read fails closed.
type InputHost interface {
	ReadInput(context.Context, string) (InputArtifactChunk, error)
}

type InputArtifactChunk struct {
	Bytes []byte
	Final bool
}

// ReadInput keeps plugins source-compatible with Host implementations that do
// not advertise the input extension while providing one typed call site.
func ReadInput(ctx context.Context, host Host, opaqueHandle string) (InputArtifactChunk, error) {
	reader, ok := host.(InputHost)
	if !ok {
		return InputArtifactChunk{}, Failure{Code: string(FailureOperationNotPermitted), Message: "operation not permitted"}
	}
	return reader.ReadInput(ctx, opaqueHandle)
}

// WaitRequest asks the host to resume the same invocation after a bounded
// relative delay. Completion carries no product status or success signal.
type WaitRequest struct {
	DelayMilliseconds uint64
}

// Responses is a product response stream. A completed stream returns io.EOF.
type Responses interface {
	Recv() (Response, error)
}

// ResponseStreamMetadata carries bounded terminal information from a product
// response stream. It deliberately exposes only an opaque continuation value.
type ResponseStreamMetadata struct {
	Continuation string
}

// ResponseStreamMetadataProvider is optionally implemented by response streams
// that expose terminal metadata. Responses remains source-compatible with
// external stream implementations that do not implement this interface.
type ResponseStreamMetadataProvider interface {
	ResponseStreamMetadata() ResponseStreamMetadata
}

// ResponseStreamMetadataOf returns terminal metadata when supported, or an
// empty value for response streams that do not expose it.
func ResponseStreamMetadataOf(responses Responses) ResponseStreamMetadata {
	provider, ok := responses.(ResponseStreamMetadataProvider)
	if !ok || provider == nil {
		return ResponseStreamMetadata{}
	}
	return provider.ResponseStreamMetadata()
}

// AuthProvider is a privileged, transport-independent credential lifecycle facet.
type AuthProvider interface {
	Metadata() AuthProviderMetadata
	Provides() []string
	Resolve(context.Context, AuthRequest, AuthHost) (ServiceBinding, error)
}

// TargetProvider is a transport-independent target discovery implementation.
type TargetProvider interface {
	Metadata() TargetProviderMetadata
	Discover(context.Context, TargetDiscoveryRequest, TargetHost) (TargetDiscoveryResult, error)
}

// TargetHost exposes only bounded product requests needed for target discovery.
type TargetHost interface {
	Request(context.Context, Request) (Responses, error)
	Log(Level, string)
}

type AuthProviderMetadata struct {
	Name    string
	Version string
}

// AuthHost is the privileged credential broker exposed only to an activated auth facet.
// Handles are opaque references; raw credential material never crosses this boundary.
type AuthHost interface {
	Load(context.Context, CredentialSlot) (CredentialHandle, LeaseMetadata, StoreState, error)
	AuthorizeOIDC(context.Context, CredentialSlot, OIDCIntent) (CredentialHandle, LeaseMetadata, error)
	Refresh(context.Context, CredentialSlot, CredentialHandle, RefreshIntent) (CredentialHandle, LeaseMetadata, error)
	Exchange(context.Context, CredentialSlot, CredentialHandle, ExchangeIntent) (CredentialHandle, LeaseMetadata, error)
	Invalidate(context.Context, CredentialSelector) error
	Bind(context.Context, CredentialHandle, BindingSpec) (ServiceBinding, error)
}

// ClientCredentialsAuthHost is the directional client-credentials extension.
// The host retains endpoints and every credential byte.
type ClientCredentialsAuthHost interface {
	AuthorizeClientCredentials(context.Context, AuthorizeClientCredentialsRequest) (AuthorizeClientCredentialsResponse, error)
	BindCredential(context.Context, BindCredentialRequest) (BindCredentialResponse, error)
}

type AuthRequest struct {
	Capability string
	Service    Service
	Target     TargetCoordinates
	Operations []string
	// CredentialSlot is the host-validated client-credentials authority for a
	// final product flow. It is absent for OIDC-only and target-discovery flows.
	CredentialSlot *CredentialSlot
}

type TargetCoordinates struct {
	OrganizationID string
	StackID        string
}

type CredentialHandle string
type ServiceBinding string
type CredentialBinding string

type CredentialSlot struct {
	Stage                       string  `json:"stage,omitempty"`
	Profile                     string  `json:"profile,omitempty"`
	Service                     Service `json:"service,omitempty"`
	Capability                  string  `json:"capability,omitempty"`
	Provider                    string  `json:"provider,omitempty"`
	ProviderVersion             string  `json:"provider_version,omitempty"`
	ArtifactDigest              string  `json:"artifact_digest,omitempty"`
	FacetProtocolVersion        string  `json:"facet_protocol_version,omitempty"`
	ProfileCapabilityGeneration uint64  `json:"profile_capability_generation,omitempty"`
	OrganizationID              string  `json:"organization_id,omitempty"`
	StackID                     string  `json:"stack_id,omitempty"`
	Audience                    string  `json:"audience,omitempty"`
	// Scopes is a required key: RFC 0014 distinguishes the present empty exact
	// set of a protected bearer-only slot from absent scope metadata, so an
	// empty set must encode as "scopes":[] rather than disappear.
	Scopes []string `json:"scopes"`
}

type LeaseMetadata struct {
	ExpiresAtUnix int64
	Refreshable   bool
}

type StoreState string

const (
	StoreStateMissing StoreState = "missing"
	StoreStateValid   StoreState = "valid"
	StoreStateExpired StoreState = "expired"
	StoreStateInvalid StoreState = "invalid"
)

type OIDCIntent struct {
	Issuer string
	Scopes []string
}

type RefreshIntent struct {
	Scopes []string
}

type ExchangeIntent struct {
	Resource string
	Scopes   []string
}

type CredentialSelector struct {
	Slot        CredentialSlot
	Descendants bool
}

type BindingSpec struct {
	Service    Service
	Operations []string
}

type ClientCredentialsIntent struct {
	Service Service  `json:"service"`
	Scopes  []string `json:"scopes"`
}

type AuthorizeClientCredentialsRequest struct {
	Slot   CredentialSlot          `json:"slot"`
	Intent ClientCredentialsIntent `json:"intent"`
}

type AuthorizeClientCredentialsResponse struct {
	CredentialHandle CredentialHandle `json:"credential_handle"`
	Lease            LeaseMetadata    `json:"lease"`
}

type BindCredentialRequest struct {
	Slot             CredentialSlot   `json:"slot"`
	CredentialHandle CredentialHandle `json:"credential_handle"`
	Operations       []string         `json:"operations"`
}

type BindCredentialResponse struct {
	Binding CredentialBinding `json:"binding"`
	Lease   LeaseMetadata     `json:"lease"`
}

type AuthGrantRecord struct {
	Profile              string
	Capability           string
	Plugin               string
	PluginVersion        string
	ArtifactDigest       string
	FacetProtocolVersion string
}

type SignerSlot struct {
	Profile              string
	OrganizationID       string
	StackID              string
	Service              Service
	ProductMajor         uint32
	Capability           string
	Provider             string
	ProviderVersion      string
	Runtime              Runtime
	ArtifactDigest       string
	FacetProtocolVersion string
	Generation           uint64
	KeyID                string
}

type SignerHandle string
type SignerBinding string

type SignerMetadata struct {
	KeyID     string
	Algorithm string
}

type SignerProviderMetadata struct {
	Name    string
	Version string
}

type SignerRequest struct {
	Capability   string
	Service      Service
	ProductMajor uint32
	Target       TargetCoordinates
	KeyID        string
	Operations   []string
	FullMethod   string
	Algorithm    string
	Protobuf     *OpaqueProtobufSigningRecipe
	PayloadType  string
}

type SignerBindingSpec struct {
	Capability   string
	Service      Service
	ProductMajor uint32
	PayloadType  string
	KeyID        string
	Operations   []string
	FullMethod   string
	Algorithm    string
	Protobuf     *OpaqueProtobufSigningRecipe
}

type SignerProvider interface {
	Metadata() SignerProviderMetadata
	Provides() []string
	Resolve(context.Context, SignerRequest, SignerHost) (SignerBinding, error)
}

type SignerHost interface {
	Load(context.Context, SignerSlot) (SignerHandle, SignerMetadata, StoreState, error)
	Bind(context.Context, SignerHandle, SignerBindingSpec) (SignerBinding, error)
}
