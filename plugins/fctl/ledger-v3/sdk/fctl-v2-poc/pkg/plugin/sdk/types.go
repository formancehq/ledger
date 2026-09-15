package sdk

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Metadata identifies a plugin independently of its runtime adapter.
type Metadata struct {
	Name    string
	Version string
	Facets  []Facet
}

// FacetKind identifies one independently versioned plugin protocol surface.
type FacetKind string

const (
	FacetCommandProvider FacetKind = "command_provider"
	FacetAuthProvider    FacetKind = "auth_provider"
	FacetTargetProvider  FacetKind = "target_provider"
	FacetSignerProvider  FacetKind = "signer_provider"

	CurrentCommandProviderFacetProtocolVersion = "4"
	CurrentAuthProviderFacetProtocolVersion    = "1"
	CurrentTargetProviderFacetProtocolVersion  = "1"
	CurrentSignerProviderFacetProtocolVersion  = "1"

	HostCapabilityGeneratedClientV1 = "host.request.generated-client.v1"
	HostCapabilityInputArtifactV1   = "host.input-artifact.v1"
	HostCapabilityWaitV1            = "host.wait.v1"

	PortableMaxHostRequests                  uint32 = 256
	PortableMaxWaitDelayMilliseconds         uint64 = 30_000
	PortableMaxWaits                         uint32 = 120
	PortableMaxAggregateDelayMilliseconds    uint64 = 1_800_000
	PortableMaxExecutionDeadlineMilliseconds uint64 = 3_600_000
)

// Facet advertises one protocol surface and the capabilities it supplies.
type Facet struct {
	Kind                     FacetKind
	ProtocolVersion          string
	Capabilities             []string
	RequiredHostCapabilities []string
}

// Runtime identifies the host adapter that owns an opaque value.
type Runtime string

const (
	RuntimeNative  Runtime = "native"
	RuntimeBrowser Runtime = "browser"
)

// ExecutionKind selects either the host-owned service preflight or a bounded
// artifact-local execution with no service context.
type ExecutionKind string

const (
	ExecutionKindService ExecutionKind = "service"
	ExecutionKindLocal   ExecutionKind = "local"
)

// AuthMode selects whether the host resolves one declared authentication
// capability or constructs an explicitly credential-free product transport.
type AuthMode string

const (
	AuthModeNone       AuthMode = "none"
	AuthModeCapability AuthMode = "capability"
)

// Risk is informational operation metadata.
type Risk string

const (
	RiskRead     Risk = "read"
	RiskMutation Risk = "mutation"
)

type SensitiveDelivery string

const (
	SensitiveDisplayOnce SensitiveDelivery = "display_once"
	SensitiveProfileAuth SensitiveDelivery = "profile_auth"
)

type SensitiveOutput struct {
	JSONPointer       string              `json:"json_pointer"`
	AllowedDeliveries []SensitiveDelivery `json:"allowed_deliveries"`
}

type DocumentationKind string

const (
	DocumentationGuide        DocumentationKind = "guide"
	DocumentationTutorial     DocumentationKind = "tutorial"
	DocumentationAPIReference DocumentationKind = "api_reference"
	DocumentationConcept      DocumentationKind = "concept"
	DocumentationChangelog    DocumentationKind = "changelog"
)

type DocumentationResource struct {
	ID              string            `json:"id"`
	Kind            DocumentationKind `json:"kind"`
	Title           string            `json:"title"`
	Description     string            `json:"description"`
	URL             string            `json:"url,omitempty"`
	BundledPath     string            `json:"bundled_path,omitempty"`
	MediaType       string            `json:"media_type"`
	Locale          string            `json:"locale"`
	SupportedMajors []uint32          `json:"supported_majors"`
	FixtureBytes    int64             `json:"fixture_bytes,omitempty"`
}

type CompletionKind string

const (
	CompletionNone    CompletionKind = "none"
	CompletionStatic  CompletionKind = "static"
	CompletionDynamic CompletionKind = "dynamic"
)

type CompletionCandidate struct {
	Value       string `json:"value"`
	Description string `json:"description"`
}

type CompletionSpec struct {
	Kind             CompletionKind        `json:"kind"`
	Candidates       []CompletionCandidate `json:"candidates"`
	NoFileCompletion bool                  `json:"no_file_completion"`
}

type ArgumentType string

const (
	ArgumentString      ArgumentType = "string"
	ArgumentStringArray ArgumentType = "string_array"
	ArgumentInt32       ArgumentType = "int32"
	ArgumentBool        ArgumentType = "bool"
)

type Argument struct {
	Name       string         `json:"name"`
	Usage      string         `json:"usage"`
	Type       ArgumentType   `json:"type"`
	Required   bool           `json:"required"`
	Repeated   bool           `json:"repeated"`
	Completion CompletionSpec `json:"completion"`
}

type ServiceCompatibility struct {
	Service Service  `json:"service"`
	Majors  []uint32 `json:"majors"`
}

type ServiceVersion struct {
	Service Service `json:"service"`
	Version string  `json:"version"`
	Major   uint32  `json:"major"`
}

type RequestSigningSpec struct {
	Capability   string                       `json:"capability"`
	ProductMajor uint32                       `json:"product_major"`
	PayloadType  string                       `json:"payload_type"`
	OperationID  string                       `json:"operation_id"`
	Algorithm    string                       `json:"algorithm"`
	Protobuf     *OpaqueProtobufSigningRecipe `json:"protobuf"`
}

// ProtobufField describes one field the bounded opaque request rewriter may
// preserve without interpreting. WireType uses protobuf's numeric wire type.
type ProtobufField struct {
	Number   uint32 `json:"number"`
	WireType uint32 `json:"wire_type"`
}

// OpaqueProtobufSigningRecipe is a closed protobuf rewrite recipe. The host
// extracts one unsigned length-delimited payload, signs its exact bytes, and
// substitutes one host-built signed envelope while preserving only the
// explicitly listed sibling fields.
type OpaqueProtobufSigningRecipe struct {
	UnsignedField          uint32          `json:"unsigned_field"`
	SignedField            uint32          `json:"signed_field"`
	EnvelopeKeyIDField     uint32          `json:"envelope_key_id_field"`
	EnvelopeSignatureField uint32          `json:"envelope_signature_field"`
	EnvelopePayloadField   uint32          `json:"envelope_payload_field"`
	PassthroughFields      []ProtobufField `json:"passthrough_fields"`
	MaxPayloadBytes        uint64          `json:"max_payload_bytes"`
	MaxSignedMessageBytes  uint64          `json:"max_signed_message_bytes"`
	MaxKeyIDBytes          uint32          `json:"max_key_id_bytes"`
	SignatureLength        uint32          `json:"signature_length"`
}

const CapabilitySignLedgerApplyBatch = "sign.ledger.apply-batch"

type InputArtifactSpec struct {
	ArgumentName string   `json:"argument_name"`
	FlagName     string   `json:"flag_name"`
	MediaTypes   []string `json:"media_types"`
	MaxBytes     int64    `json:"max_bytes"`
	AllowFile    bool     `json:"allow_file"`
	AllowStdin   bool     `json:"allow_stdin"`
	Sensitive    bool     `json:"sensitive"`
	// Optional permits the referenced field to be absent. The zero value keeps
	// the original required-source contract for older descriptors.
	Optional bool `json:"optional"`
	// Repeated admits every occurrence of a referenced string-array flag. The
	// zero value keeps the original single-source contract.
	Repeated bool `json:"repeated"`
}

// InputArtifactMediaTypeFlag is the host-owned selector used only when an
// artifact accepts more than one declared media type. It is never forwarded as
// a product flag.
func InputArtifactMediaTypeFlag(spec InputArtifactSpec) string {
	name := spec.FlagName
	if name == "" {
		name = spec.ArgumentName
	}
	return name + "-media-type"
}

type PaginationSpec struct {
	Supported bool `json:"supported"`
}

// ContinuationMode tells a command plugin whether the host requested one page
// or the complete bounded collection. The plugin owns cursor traversal; the
// host supplies and enforces the control.
type ContinuationMode uint8

const (
	ContinuationUnspecified ContinuationMode = iota
	ContinuationSinglePage
	ContinuationAllPages
)

const (
	DefaultAllPagesMaxPages uint32 = 100
	DefaultAllPagesMaxItems uint32 = 10000
	// DefaultAllPagesMaxBytes leaves ample room for the result envelope under
	// the portable protocol-major-one 6,966,419-byte frame ceiling.
	DefaultAllPagesMaxBytes uint64 = 4 << 20
)

// ContinuationControl is host-owned execution input. Zero value is retained as
// the source-compatible single-page default for callers compiled before the
// typed control was introduced.
type ContinuationControl struct {
	Mode     ContinuationMode `json:"mode"`
	MaxPages uint32           `json:"maxPages"`
	MaxItems uint32           `json:"maxItems"`
	MaxBytes uint64           `json:"maxBytes"`
}

func SinglePageContinuationControl() ContinuationControl {
	return ContinuationControl{Mode: ContinuationSinglePage}
}

func AllPagesContinuationControl() ContinuationControl {
	return ContinuationControl{
		Mode:     ContinuationAllPages,
		MaxPages: DefaultAllPagesMaxPages,
		MaxItems: DefaultAllPagesMaxItems,
		MaxBytes: DefaultAllPagesMaxBytes,
	}
}

// ValidateFacets rejects unsupported or ambiguous facet declarations.
func ValidateFacets(facets []Facet) error {
	seen := make(map[FacetKind]struct{}, len(facets))
	for index, facet := range facets {
		if facet.Kind != FacetCommandProvider && facet.Kind != FacetAuthProvider && facet.Kind != FacetTargetProvider && facet.Kind != FacetSignerProvider {
			return fmt.Errorf("facet %d has unsupported kind %q", index, facet.Kind)
		}
		if facet.ProtocolVersion != currentFacetProtocolVersion(facet.Kind) {
			return fmt.Errorf("facet %d has unsupported protocol version %q", index, facet.ProtocolVersion)
		}
		if _, exists := seen[facet.Kind]; exists {
			return fmt.Errorf("facet %q is declared more than once", facet.Kind)
		}
		seen[facet.Kind] = struct{}{}
		if facet.Kind != FacetCommandProvider && len(facet.RequiredHostCapabilities) != 0 {
			return fmt.Errorf("facet %d kind %q cannot require host capabilities", index, facet.Kind)
		}
		if facet.Kind == FacetCommandProvider && !validRequiredHostCapabilities(facet.RequiredHostCapabilities) {
			return fmt.Errorf("command facet %d has a non-canonical required host capability set", index)
		}
		if facet.Kind == FacetAuthProvider && len(facet.Capabilities) == 0 {
			return fmt.Errorf("auth facet %d requires at least one capability", index)
		}
		if facet.Kind == FacetTargetProvider {
			if err := validateTargetCapabilities(facet.Capabilities); err != nil {
				return fmt.Errorf("target facet %d %w", index, err)
			}
		}
		if facet.Kind == FacetSignerProvider && (len(facet.Capabilities) != 1 || facet.Capabilities[0] != CapabilitySignLedgerApplyBatch) {
			return fmt.Errorf("signer facet %d requires exactly capability %q", index, CapabilitySignLedgerApplyBatch)
		}
	}
	return nil
}

// ValidateGeneratedClientHostRequirement fails closed when descriptor fields
// require the generated-client host surface. A declared singleton remains
// valid when descriptorUsesGeneratedClientSurface is false because adapter use
// is not observable from catalogue fields alone.
func ValidateGeneratedClientHostRequirement(facets []Facet, descriptorUsesGeneratedClientSurface bool) error {
	if err := ValidateFacets(facets); err != nil {
		return err
	}
	if !descriptorUsesGeneratedClientSurface {
		return nil
	}
	for _, facet := range facets {
		if facet.Kind == FacetCommandProvider {
			if slices.Contains(facet.RequiredHostCapabilities, HostCapabilityGeneratedClientV1) {
				return nil
			}
			return fmt.Errorf("command facet requires host capability %q for generated-client descriptor fields", HostCapabilityGeneratedClientV1)
		}
	}
	return fmt.Errorf("generated-client descriptor fields require a command facet")
}

// ValidateCommandFacetHostRequirements validates the one current command
// facet against the exact capability set derived from its admitted commands.
// The set is intentionally one-way: a facet cannot request a capability that
// no descriptor surface requires, nor omit one that it does require.
func ValidateCommandFacetHostRequirements(facets []Facet, commands []Command) error {
	if err := ValidateFacets(facets); err != nil {
		return err
	}
	want := RequiredHostCapabilitiesForCommands(commands)
	for _, facet := range facets {
		if facet.Kind == FacetCommandProvider {
			if !equalStrings(facet.RequiredHostCapabilities, want) {
				return fmt.Errorf("command facet requires host capabilities %#v, got %#v", want, facet.RequiredHostCapabilities)
			}
			return nil
		}
	}
	if len(commands) != 0 {
		return errors.New("commands require a command facet")
	}
	return nil
}

func currentFacetProtocolVersion(kind FacetKind) string {
	switch kind {
	case FacetCommandProvider:
		return CurrentCommandProviderFacetProtocolVersion
	case FacetAuthProvider:
		return CurrentAuthProviderFacetProtocolVersion
	case FacetTargetProvider:
		return CurrentTargetProviderFacetProtocolVersion
	case FacetSignerProvider:
		return CurrentSignerProviderFacetProtocolVersion
	default:
		return ""
	}
}

// Command is the stable command identity and its host requirements.
type Command struct {
	ID                 string                  `json:"id"`
	ExecutionKind      ExecutionKind           `json:"execution_kind,omitempty"`
	AuthMode           AuthMode                `json:"auth_mode"`
	Path               []string                `json:"path"`
	Target             TargetRequirement       `json:"target"`
	Requires           []CapabilityRequirement `json:"requires,omitempty"`
	PathAliases        [][]string              `json:"path_aliases,omitempty"`
	Summary            string                  `json:"summary,omitempty"`
	Long               string                  `json:"long,omitempty"`
	Example            string                  `json:"example,omitempty"`
	Arguments          []Argument              `json:"arguments,omitempty"`
	Flags              []Flag                  `json:"flags,omitempty"`
	Auth               []AuthRequirement       `json:"auth,omitempty"`
	Operations         []OperationPolicy       `json:"operations,omitempty"`
	Compatibility      []ServiceCompatibility  `json:"compatibility,omitempty"`
	Risk               Risk                    `json:"risk,omitempty"`
	SensitiveOutputs   []SensitiveOutput       `json:"sensitive_outputs,omitempty"`
	DocumentationIDs   []string                `json:"documentation_ids,omitempty"`
	RequestSigning     *RequestSigningSpec     `json:"request_signing,omitempty"`
	InputSchema        []byte                  `json:"input_schema,omitempty"`
	RawOutputSchema    []byte                  `json:"raw_output_schema,omitempty"`
	PublicOutputSchema []byte                  `json:"public_output_schema,omitempty"`
	InputArtifacts     []InputArtifactSpec     `json:"input_artifacts,omitempty"`
	Pagination         PaginationSpec          `json:"pagination,omitempty"`
	OutputMediaType    string                  `json:"output_media_type,omitempty"`
	Render             RenderHints             `json:"render,omitempty"`
	ExecutionPolicy    *CommandExecutionPolicy `json:"execution_policy,omitempty"`
}

// CommandExecutionPolicy is the digest-locked budget for one command
// invocation. A nil Wait policy forbids Host.Wait for that command.
type CommandExecutionPolicy struct {
	MaxHostRequests uint32      `json:"max_host_requests"`
	Wait            *WaitPolicy `json:"wait,omitempty"`
}

// WaitPolicy bounds the relative waits a command may request from its host.
type WaitPolicy struct {
	MaxDelayMilliseconds          uint64 `json:"max_delay_milliseconds"`
	MaxWaits                      uint32 `json:"max_waits"`
	MaxAggregateDelayMilliseconds uint64 `json:"max_aggregate_delay_milliseconds"`
}

// FlagType is a transport-independent command argument type.
type FlagType string

const (
	FlagString      FlagType = "string"
	FlagStringArray FlagType = "string_array"
	FlagInt32       FlagType = "int32"
	FlagBool        FlagType = "bool"
)

// Flag describes a host-parsed command flag.
type Flag struct {
	Name         string         `json:"name"`
	Aliases      []string       `json:"aliases,omitempty"`
	Usage        string         `json:"usage,omitempty"`
	Type         FlagType       `json:"type"`
	HasDefault   bool           `json:"has_default,omitempty"`
	DefaultValue string         `json:"default_value,omitempty"`
	Required     bool           `json:"required,omitempty"`
	Completion   CompletionSpec `json:"completion,omitempty"`
}

// AuthRequirement declares authentication independently of product operations.
type AuthRequirement struct {
	Capability string `json:"capability"`
	Optional   bool   `json:"optional,omitempty"`
}

// OperationPolicy declares one logical product operation allowed to a command.
type OperationPolicy struct {
	ID      string               `json:"id"`
	Service Service              `json:"service"`
	Scopes  []string             `json:"scopes"`
	HTTP    *HTTPOperationPolicy `json:"http,omitempty"`
	GRPC    *GRPCOperationPolicy `json:"grpc,omitempty"`
}

// ResponseLimits bounds one host-managed product response stream.
type ResponseLimits struct {
	MaxMessageBytes   int64  `json:"max_message_bytes"`
	MaxMessages       uint32 `json:"max_messages"`
	MaxAggregateBytes int64  `json:"max_aggregate_bytes"`
}

// HTTPGeneratedClientPolicy contains the bounded RFC 0011 HTTP adapter policy.
// Its containing pointer is presence-bearing: a non-nil empty value still
// selects generated-client admission and then fails semantic validation.
type HTTPGeneratedClientPolicy struct {
	PathTemplate        string         `json:"path_template,omitzero"`
	RequestContentTypes []string       `json:"request_content_types,omitzero"`
	RequestHeaders      []string       `json:"request_headers,omitzero"`
	MaxRequestBytes     int64          `json:"max_request_bytes,omitzero"`
	ResponseLimits      ResponseLimits `json:"response_limits,omitzero"`
}

// HTTPOperationPolicy constrains a logical operation to a relative HTTP route.
type HTTPOperationPolicy struct {
	Method          string                     `json:"method"`
	Path            string                     `json:"path,omitempty"`
	GeneratedClient *HTTPGeneratedClientPolicy `json:"generated_client,omitempty"`
}

// GRPCGeneratedClientPolicy contains the bounded RFC 0011 gRPC adapter policy.
type GRPCGeneratedClientPolicy struct {
	MaxRequestMessageBytes int64          `json:"max_request_message_bytes,omitzero"`
	ResponseLimits         ResponseLimits `json:"response_limits,omitzero"`
}

// GRPCOperationPolicy constrains a logical operation to one gRPC method.
type GRPCOperationPolicy struct {
	FullMethod      string                     `json:"full_method"`
	ServerStreaming bool                       `json:"server_streaming,omitempty"`
	GeneratedClient *GRPCGeneratedClientPolicy `json:"generated_client,omitempty"`
}

// RenderHints let the host render canonical structured results consistently.
type RenderHints struct {
	Table *TableRenderHint `json:"table,omitempty"`
}

type TableRenderHint struct {
	Columns []TableColumn `json:"columns"`
}

type TableColumn struct {
	Header string `json:"header"`
	Field  string `json:"field"`
}

// CapabilityRequirement declares a host capability and its allowed operations.
type CapabilityRequirement struct {
	ProviderCapability string   `json:"provider_capability"`
	Operations         []string `json:"operations,omitempty"`
	Optional           bool     `json:"optional,omitempty"`
}

// ValidateCommands rejects ambiguous or unsafe command descriptors before they
// can influence host command routing or product authorization policy.
func ValidateCommands(commands []Command) error {
	ids := make(map[string]struct{}, len(commands))
	paths := make(map[string]struct{}, len(commands))
	for commandIndex, command := range commands {
		if command.ID == "" {
			return fmt.Errorf("command %d has no id", commandIndex)
		}
		if _, exists := ids[command.ID]; exists {
			return fmt.Errorf("command id %q is declared more than once", command.ID)
		}
		ids[command.ID] = struct{}{}
		if len(command.Path) == 0 {
			return fmt.Errorf("command %q has no path", command.ID)
		}
		if _, err := normalizedTargetKind(command.Target); err != nil {
			return fmt.Errorf("command %q: %w", command.ID, err)
		}
		for _, part := range command.Path {
			if !safeCommandToken(part) {
				return fmt.Errorf("command %q has invalid path token %q", command.ID, part)
			}
		}
		pathKey := strings.Join(command.Path, "\x00")
		if _, exists := paths[pathKey]; exists {
			return fmt.Errorf("command path %q is declared more than once", strings.Join(command.Path, " "))
		}
		paths[pathKey] = struct{}{}
		if len(command.PathAliases) != 0 && len(command.PathAliases) != len(command.Path) {
			return fmt.Errorf("command %q aliases do not match its path", command.ID)
		}
		for index, aliases := range command.PathAliases {
			seen := map[string]struct{}{command.Path[index]: {}}
			for _, alias := range aliases {
				if !safeCommandToken(alias) {
					return fmt.Errorf("command %q has invalid path alias %q", command.ID, alias)
				}
				if _, exists := seen[alias]; exists {
					return fmt.Errorf("command %q repeats path alias %q", command.ID, alias)
				}
				seen[alias] = struct{}{}
			}
		}
		flagNames := make(map[string]struct{}, len(command.Flags))
		for _, flag := range command.Flags {
			if !safeCommandToken(flag.Name) {
				return fmt.Errorf("command %q has invalid flag %q", command.ID, flag.Name)
			}
			if err := validateFlagDefault(flag); err != nil {
				return fmt.Errorf("command %q flag %q: %w", command.ID, flag.Name, err)
			}
			for _, name := range append([]string{flag.Name}, flag.Aliases...) {
				if !safeCommandToken(name) {
					return fmt.Errorf("command %q has invalid flag alias %q", command.ID, name)
				}
				if _, exists := flagNames[name]; exists {
					return fmt.Errorf("command %q repeats flag name or alias %q", command.ID, name)
				}
				flagNames[name] = struct{}{}
			}
		}
		for _, requirement := range command.Auth {
			if err := validateAuthRequirement(requirement); err != nil {
				return fmt.Errorf("command %q: %w", command.ID, err)
			}
		}
		if err := validateCommandOperationPolicies(command.AuthMode, command.Operations); err != nil {
			return fmt.Errorf("command %q: %w", command.ID, err)
		}
		if command.Render.Table != nil {
			for _, column := range command.Render.Table.Columns {
				if column.Header == "" || column.Field == "" {
					return fmt.Errorf("command %q has incomplete table column", command.ID)
				}
			}
		}
		if command.ExecutionKind != "" {
			if err := validateV2Command(command); err != nil {
				return fmt.Errorf("command %q: %w", command.ID, err)
			}
		}
	}
	return nil
}

func validateAuthRequirement(requirement AuthRequirement) error {
	capability, ok := strings.CutPrefix(requirement.Capability, "auth.")
	if !ok || !safeCommandToken(capability) {
		return fmt.Errorf("has invalid auth capability %q", requirement.Capability)
	}
	return nil
}

func validateCommandOperationPolicies(authMode AuthMode, operations []OperationPolicy) error {
	seen := make(map[string]struct{}, len(operations))
	for _, operation := range operations {
		if err := validateCommandOperationPolicy(authMode, operation); err != nil {
			return err
		}
		if _, exists := seen[operation.ID]; exists {
			return fmt.Errorf("repeats operation %q", operation.ID)
		}
		seen[operation.ID] = struct{}{}
	}
	return nil
}

// CommandsUseGeneratedClientSurface reports whether commands contain any
// descriptor field introduced for RFC 0011 generated-client adapters. Adapter
// construction itself remains outside catalogue observability.
func CommandsUseGeneratedClientSurface(commands []Command) bool {
	for _, command := range commands {
		for _, operation := range command.Operations {
			if operationUsesGeneratedClientSurface(operation) {
				return true
			}
		}
	}
	return false
}

// CommandsUseWaitPolicy reports whether any command admits a wait policy.
func CommandsUseWaitPolicy(commands []Command) bool {
	for _, command := range commands {
		if command.ExecutionPolicy != nil && command.ExecutionPolicy.Wait != nil {
			return true
		}
	}
	return false
}

// RequiredHostCapabilitiesForCommands derives the canonical host capability
// set for a command catalogue. Its empty result is always non-nil.
func RequiredHostCapabilitiesForCommands(commands []Command) []string {
	capabilities := make([]string, 0, 3)
	if CommandsUseGeneratedClientSurface(commands) {
		capabilities = append(capabilities, HostCapabilityGeneratedClientV1)
	}
	for _, command := range commands {
		if len(command.InputArtifacts) != 0 {
			capabilities = append(capabilities, HostCapabilityInputArtifactV1)
			break
		}
	}
	if CommandsUseWaitPolicy(commands) {
		capabilities = append(capabilities, HostCapabilityWaitV1)
	}
	sort.Strings(capabilities)
	return capabilities
}

// ValidateRequiredHostCapabilities checks a declared capability set against
// the exact set derived from command descriptor surfaces.
func ValidateRequiredHostCapabilities(commands []Command, declared []string) error {
	if !validRequiredHostCapabilities(declared) {
		return fmt.Errorf("required host capabilities are not canonical")
	}
	want := RequiredHostCapabilitiesForCommands(commands)
	if !equalStrings(declared, want) {
		return fmt.Errorf("required host capabilities %#v do not match descriptor-derived set %#v", declared, want)
	}
	return nil
}

func validRequiredHostCapabilities(values []string) bool {
	for index, value := range values {
		if value != HostCapabilityGeneratedClientV1 && value != HostCapabilityInputArtifactV1 && value != HostCapabilityWaitV1 {
			return false
		}
		if index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateCommandOperationPolicy(authMode AuthMode, operation OperationPolicy) error {
	if err := validateOperationIdentity(operation); err != nil {
		return err
	}
	if authMode == AuthModeNone && len(operation.Scopes) != 0 {
		return fmt.Errorf("operation %q declares scopes for auth mode none", operation.ID)
	}
	if !operationUsesGeneratedClientSurface(operation) {
		return validateOperationPolicy(operation)
	}
	if operation.HTTP != nil {
		if err := validateGeneratedHTTPPolicy(*operation.HTTP); err != nil {
			return fmt.Errorf("operation %q has invalid HTTP policy: %w", operation.ID, err)
		}
		return nil
	}
	if err := validateGeneratedGRPCPolicy(*operation.GRPC); err != nil {
		return fmt.Errorf("operation %q has invalid gRPC policy: %w", operation.ID, err)
	}
	return nil
}

func operationUsesGeneratedClientSurface(operation OperationPolicy) bool {
	return operation.HTTP != nil && operation.HTTP.GeneratedClient != nil ||
		operation.GRPC != nil && operation.GRPC.GeneratedClient != nil
}

func validateOperationPolicy(operation OperationPolicy) error {
	if err := validateOperationIdentity(operation); err != nil {
		return err
	}
	if operation.HTTP != nil {
		if operation.HTTP.GeneratedClient != nil {
			return fmt.Errorf("operation %q target-provider HTTP policy uses generated-client envelope", operation.ID)
		}
		if operation.HTTP.Method == "" || operation.HTTP.Method != strings.ToUpper(operation.HTTP.Method) {
			return fmt.Errorf("operation %q has invalid HTTP policy", operation.ID)
		}
		parsed, err := url.ParseRequestURI(operation.HTTP.Path)
		if err != nil || parsed.IsAbs() || parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasPrefix(operation.HTTP.Path, "/") {
			return fmt.Errorf("operation %q has unsafe HTTP path %q", operation.ID, operation.HTTP.Path)
		}
		return nil
	}
	if operation.GRPC.GeneratedClient != nil {
		return fmt.Errorf("operation %q target-provider gRPC policy uses generated-client envelope", operation.ID)
	}
	if operation.GRPC.FullMethod == "" || !strings.HasPrefix(operation.GRPC.FullMethod, "/") {
		return fmt.Errorf("operation %q has invalid gRPC full method %q", operation.ID, operation.GRPC.FullMethod)
	}
	return nil
}

func validateOperationIdentity(operation OperationPolicy) error {
	if operation.ID == "" || !validExactScopes(operation.Scopes) {
		return fmt.Errorf("has incomplete operation")
	}
	if !supportedNetworkService(operation.Service) {
		return fmt.Errorf("operation %q has unsupported service %q", operation.ID, operation.Service)
	}
	if (operation.HTTP == nil) == (operation.GRPC == nil) {
		return fmt.Errorf("operation %q must declare exactly one transport", operation.ID)
	}
	return nil
}

func validExactScopes(scopes []string) bool {
	return ExactScopeSet(scopes)
}

// ExactScopeSet reports whether scopes is an admissible RFC 0014 exact scope
// set: a present, sorted, unique list of RFC 6749 scope-token values, or the
// present empty set of a protected bearer-only operation. Absent metadata is
// never exact. This is the authoritative grammar; hosts reject a set this
// function refuses before any broker, store, or provider admission.
func ExactScopeSet(scopes []string) bool {
	if scopes == nil {
		return false
	}
	for index, scope := range scopes {
		if !validOAuthScopeToken(scope) || index > 0 && scopes[index-1] >= scope {
			return false
		}
	}
	return true
}

func validOAuthScopeToken(scope string) bool {
	if scope == "" {
		return false
	}
	for index := 0; index < len(scope); index++ {
		value := scope[index]
		// RFC 6749 NQCHAR minus the comma: RFC 0014 forbids delimiter-packed
		// entries, so 0x2c rejects instead of hiding several scopes in one token.
		if value == 0x2c || value != 0x21 && (value < 0x23 || value > 0x5b) && (value < 0x5d || value > 0x7e) {
			return false
		}
	}
	return true
}

func validateFlagDefault(flag Flag) error {
	switch flag.Type {
	case FlagString, FlagStringArray:
		return nil
	case FlagInt32:
		if flag.DefaultValue == "" {
			return nil
		}
		value, err := strconv.ParseInt(flag.DefaultValue, 10, 32)
		if err != nil || value < -1<<31 || value > 1<<31-1 {
			return fmt.Errorf("invalid int32 default %q", flag.DefaultValue)
		}
		return nil
	case FlagBool:
		if flag.DefaultValue == "" {
			return nil
		}
		if _, err := strconv.ParseBool(flag.DefaultValue); err != nil {
			return fmt.Errorf("invalid bool default %q", flag.DefaultValue)
		}
		return nil
	default:
		return fmt.Errorf("unsupported type %q", flag.Type)
	}
}

func safeCommandToken(value string) bool {
	if value == "" || strings.HasPrefix(value, "-") {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

// ExecuteRequest selects a command and carries only plugin-relative arguments.
type ExecuteRequest struct {
	CommandID       string
	Arguments       []string
	Flags           []FlagOccurrence
	Target          TargetSelection
	ServiceVersions []ServiceVersion
	Continuation    ContinuationControl
}

type FlagOccurrence struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type CompletionTarget struct {
	ArgumentName string `json:"argument_name"`
	FlagName     string `json:"flag_name"`
}

type CompleteRequest struct {
	CommandID        string
	Arguments        []string
	Flags            []FlagOccurrence
	ToComplete       string
	CompletionTarget CompletionTarget
	Target           TargetSelection
	ServiceVersions  []ServiceVersion
}

type CompleteResponse struct {
	Candidates       []CompletionCandidate
	NoFileCompletion bool
}

// Service identifies a logical product service resolved by the host.
type Service string

const (
	// ServiceMembership identifies the Membership API without selecting a target.
	ServiceMembership Service = "membership"
	// ServiceConnectivity identifies the Connectivity API without exposing its endpoint.
	ServiceConnectivity Service = "connectivity"
	// ServiceLedger identifies the Ledger API without exposing its endpoint.
	ServiceLedger Service = "ledger"
	// ServiceAuth identifies the Auth API.
	ServiceAuth Service = "auth"
	// ServicePayments identifies the Payments API.
	ServicePayments Service = "payments"
	// ServiceWallets identifies the Wallets API.
	ServiceWallets Service = "wallets"
	// ServiceReconciliation identifies the Reconciliation API.
	ServiceReconciliation Service = "reconciliation"
	// ServiceFlows identifies the Flows API.
	ServiceFlows Service = "flows"
	// ServiceTransactionPlane identifies the Transaction Plane API.
	ServiceTransactionPlane Service = "transaction-plane"
	// ServiceNumscript identifies the Numscript language runtime.
	ServiceNumscript Service = "numscript"
	// ServiceStudioApps identifies Studio applications.
	ServiceStudioApps Service = "studio-apps"
	// ServiceBankingBridge reserves the Banking Bridge identity.
	ServiceBankingBridge Service = "banking-bridge"
)

// Request asks the host to perform one capability-authorized product operation.
type Request struct {
	Service    Service
	Capability string
	Operation  string
	HTTP       *HTTPRequest
	GRPC       *GRPCRequest
}

// HTTPRequest describes a host-managed HTTP call using a relative path.
type HTTPRequest struct {
	Method      string
	Path        string
	Query       map[string][]string
	Headers     map[string][]string
	Body        []byte
	ContentType string
}

// GRPCRequest describes one host-managed gRPC call using serialized protobuf bytes.
type GRPCRequest struct {
	FullMethod string
	Message    []byte
}

// Response contains one serialized product response payload.
type Response struct {
	Body        []byte
	Status      int32
	ContentType string
}

// Failure is a safe, structured error that may cross a plugin boundary.
type Failure struct {
	Code      string
	Message   string
	Details   []byte
	Retryable bool
}

func (failure Failure) Error() string { return failure.Message }

// EventKind identifies a plugin output event.
type EventKind string

const (
	EventProgress   EventKind = "progress"
	EventDiagnostic EventKind = "diagnostic"
	EventResult     EventKind = "result"
)

type ResultShape string

const (
	ResultObject     ResultShape = "object"
	ResultCollection ResultShape = "collection"
	ResultEmpty      ResultShape = "empty"
)

type PageInfo struct {
	NextCursor string
	HasMore    bool
}

type ResultEnvelope struct {
	OperationID string
	Shape       ResultShape
	MediaType   string
	Data        []byte
	Page        *PageInfo
}

// Event is emitted by a plugin for host-owned presentation.
type Event struct {
	Kind    EventKind
	Payload []byte
	Result  *ResultEnvelope
}

// Level is the severity of a safe plugin diagnostic.
type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// LogEntry is a diagnostic captured by MemoryHost.
type LogEntry struct {
	Level   Level
	Message string
}
