package componentbridgev1alpha1

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const ProtocolMajor uint32 = 1
const MaxCorrelationIDBytes = 128

var (
	ErrMalformed = errors.New("portable component payload is malformed")
	ErrProtocol  = errors.New("portable component protocol identity is invalid")
)

const redactedFailureMessage = "internal failure"

// Descriptor is the runtime-neutral install contract carried by describe().
// This file is the first production cutover slice: it owns Describe, the
// lifecycle fields FrameCodec must inspect, and the SDK payload values used by
// the command execution seam. The generated bindings remain authoritative for
// lifecycle wrappers and broker messages not yet consumed by those seams.
type Descriptor struct {
	Metadata               sdk.Metadata
	Commands               []sdk.Command
	AuthProviders          []AuthProvider
	TargetProviders        []sdk.TargetProviderMetadata
	SignerProviders        []SignerProvider
	DocumentationResources []sdk.DocumentationResource
}

type AuthProvider struct {
	Metadata        sdk.AuthProviderMetadata
	ProtocolVersion string
	Capabilities    []string
}

type SignerProvider struct {
	Metadata        sdk.SignerProviderMetadata
	ProtocolVersion string
	Capabilities    []string
}

// FailureFromSDK preserves only the closed public vocabulary. Unknown input is
// replaced atomically so diagnostic text and details cannot cross the codec.
func FailureFromSDK(value sdk.Failure) *Failure {
	if code := sdk.FailureCode(value.Code); !code.Valid() || code == sdk.FailureInternal {
		return &Failure{Code: string(sdk.FailureInternal), Message: redactedFailureMessage}
	}
	return &Failure{Code: value.Code, Message: value.Message, Details: cloneBytes(value.Details), Retryable: value.Retryable}
}

// FailureToSDK applies the same redaction rule to untrusted guest bytes.
func FailureToSDK(value *Failure) sdk.Failure {
	if value == nil {
		return sdk.Failure{Code: string(sdk.FailureInternal), Message: redactedFailureMessage}
	}
	if code := sdk.FailureCode(value.GetCode()); !code.Valid() || code == sdk.FailureInternal {
		return sdk.Failure{Code: string(sdk.FailureInternal), Message: redactedFailureMessage}
	}
	return sdk.Failure{Code: value.GetCode(), Message: value.GetMessage(), Details: cloneBytes(value.GetDetails()), Retryable: value.GetRetryable()}
}

// DecodeRequestCorrelation decodes only request kinds that carry a
// correlation identifier used by the lifecycle coordinator.
func DecodeRequestCorrelation(kind MessageKind, payload []byte) (string, error) {
	var correlation string
	switch kind {
	case MessageKind_MESSAGE_KIND_HOST_REQUEST:
		var request HostRequestPayload
		if err := unmarshalStrict(payload, &request, "host_request"); err != nil {
			return "", err
		}
		switch selected := request.GetRequest().(type) {
		case *HostRequestPayload_Product:
			if selected.Product == nil {
				return "", fmt.Errorf("%w: host product request is missing", ErrProtocol)
			}
		case *HostRequestPayload_AuthBroker:
			if selected.AuthBroker == nil {
				return "", fmt.Errorf("%w: host auth request is missing", ErrProtocol)
			}
		case *HostRequestPayload_SignerBroker:
			if selected.SignerBroker == nil {
				return "", fmt.Errorf("%w: host signer request is missing", ErrProtocol)
			}
		case *HostRequestPayload_InputArtifact:
			if selected.InputArtifact == nil || selected.InputArtifact.GetOpaqueHandle() == "" {
				return "", fmt.Errorf("%w: host input artifact request is missing", ErrProtocol)
			}
		default:
			return "", fmt.Errorf("%w: host request variant is missing", ErrProtocol)
		}
		correlation = request.GetCorrelationId()
	case MessageKind_MESSAGE_KIND_WAIT_REQUEST:
		var request WaitRequestPayload
		if err := unmarshalStrict(payload, &request, "wait_request"); err != nil {
			return "", err
		}
		correlation = request.GetCorrelationId()
	default:
		return "", fmt.Errorf("%w: message kind %d has no request correlation", ErrProtocol, kind)
	}
	if correlation == "" || len(correlation) > MaxCorrelationIDBytes || !utf8.ValidString(correlation) {
		return "", fmt.Errorf("%w: request correlation is not canonical", ErrProtocol)
	}
	return correlation, nil
}

// DecodeTerminalFailure returns an empty code for the explicit successful
// outcome and a declared code for failure. Missing, false, or widened outcomes
// fail closed.
func DecodeTerminalFailure(payload []byte) (sdk.FailureCode, error) {
	var termination TerminationPayload
	if err := unmarshalStrict(payload, &termination, "termination"); err != nil {
		return "", err
	}
	switch outcome := termination.GetOutcome().(type) {
	case *TerminationPayload_Success:
		if !outcome.Success {
			return "", fmt.Errorf("%w: false success outcome", ErrProtocol)
		}
		return "", nil
	case *TerminationPayload_Failure:
		if outcome.Failure == nil || !sdk.FailureCode(outcome.Failure.GetCode()).Valid() {
			return "", fmt.Errorf("%w: undeclared failure outcome", ErrProtocol)
		}
		return sdk.FailureCode(outcome.Failure.GetCode()), nil
	default:
		return "", fmt.Errorf("%w: terminal outcome is missing", ErrProtocol)
	}
}

func CommandStartFromSDK(value sdk.ExecuteRequest) (*CommandStart, error) {
	continuation, err := continuationFromSDK(value.Continuation)
	if err != nil {
		return nil, err
	}
	mapped := &CommandStart{CommandId: value.CommandID, Arguments: cloneStrings(value.Arguments), Target: &TargetCoordinates{OrganizationId: value.Target.OrganizationID, StackId: value.Target.StackID}}
	for _, flag := range value.Flags {
		mapped.Flags = append(mapped.Flags, &FlagOccurrence{Name: flag.Name, Value: flag.Value})
	}
	for _, version := range value.ServiceVersions {
		service, err := serviceFromSDK(version.Service)
		if err != nil {
			return nil, err
		}
		mapped.ServiceVersions = append(mapped.ServiceVersions, &ServiceVersion{Service: service, Version: version.Version, Major: version.Major})
	}
	mapped.Continuation = continuation
	return mapped, nil
}

func CommandStartToSDK(value *CommandStart) (sdk.ExecuteRequest, error) {
	if value == nil {
		return sdk.ExecuteRequest{}, fmt.Errorf("%w: command start is missing", ErrMalformed)
	}
	continuation, err := continuationToSDK(value.GetContinuation())
	if err != nil {
		return sdk.ExecuteRequest{}, err
	}
	mapped := sdk.ExecuteRequest{CommandID: value.GetCommandId(), Arguments: cloneStrings(value.GetArguments()), Continuation: continuation}
	if target := value.GetTarget(); target != nil {
		mapped.Target = sdk.TargetSelection{OrganizationID: target.GetOrganizationId(), StackID: target.GetStackId()}
	}
	for _, flag := range value.GetFlags() {
		if flag == nil {
			return sdk.ExecuteRequest{}, fmt.Errorf("%w: flag occurrence is missing", ErrMalformed)
		}
		mapped.Flags = append(mapped.Flags, sdk.FlagOccurrence{Name: flag.GetName(), Value: flag.GetValue()})
	}
	for _, version := range value.GetServiceVersions() {
		if version == nil {
			return sdk.ExecuteRequest{}, fmt.Errorf("%w: service version is missing", ErrMalformed)
		}
		service, err := serviceToSDK(version.GetService())
		if err != nil {
			return sdk.ExecuteRequest{}, err
		}
		mapped.ServiceVersions = append(mapped.ServiceVersions, sdk.ServiceVersion{Service: service, Version: version.GetVersion(), Major: version.GetMajor()})
	}
	return mapped, nil
}

func CompletionStartFromSDK(value sdk.CompleteRequest) (*CompletionStart, error) {
	mapped := &CompletionStart{
		CommandId: value.CommandID, Arguments: cloneStrings(value.Arguments), ToComplete: value.ToComplete,
		CompletionTarget: &CompletionTarget{ArgumentName: value.CompletionTarget.ArgumentName, FlagName: value.CompletionTarget.FlagName},
		Target:           &TargetCoordinates{OrganizationId: value.Target.OrganizationID, StackId: value.Target.StackID},
	}
	for _, flag := range value.Flags {
		mapped.Flags = append(mapped.Flags, &FlagOccurrence{Name: flag.Name, Value: flag.Value})
	}
	for _, version := range value.ServiceVersions {
		service, err := serviceFromSDK(version.Service)
		if err != nil {
			return nil, err
		}
		mapped.ServiceVersions = append(mapped.ServiceVersions, &ServiceVersion{Service: service, Version: version.Version, Major: version.Major})
	}
	return mapped, nil
}

func CompletionStartToSDK(value *CompletionStart) (sdk.CompleteRequest, error) {
	if value == nil {
		return sdk.CompleteRequest{}, fmt.Errorf("%w: completion start is missing", ErrMalformed)
	}
	mapped := sdk.CompleteRequest{CommandID: value.GetCommandId(), Arguments: cloneStrings(value.GetArguments()), ToComplete: value.GetToComplete()}
	if target := value.GetCompletionTarget(); target != nil {
		mapped.CompletionTarget = sdk.CompletionTarget{ArgumentName: target.GetArgumentName(), FlagName: target.GetFlagName()}
	}
	if target := value.GetTarget(); target != nil {
		mapped.Target = sdk.TargetSelection{OrganizationID: target.GetOrganizationId(), StackID: target.GetStackId()}
	}
	for _, flag := range value.GetFlags() {
		if flag == nil {
			return sdk.CompleteRequest{}, fmt.Errorf("%w: flag occurrence is missing", ErrMalformed)
		}
		mapped.Flags = append(mapped.Flags, sdk.FlagOccurrence{Name: flag.GetName(), Value: flag.GetValue()})
	}
	for _, version := range value.GetServiceVersions() {
		if version == nil {
			return sdk.CompleteRequest{}, fmt.Errorf("%w: service version is missing", ErrMalformed)
		}
		service, err := serviceToSDK(version.GetService())
		if err != nil {
			return sdk.CompleteRequest{}, err
		}
		mapped.ServiceVersions = append(mapped.ServiceVersions, sdk.ServiceVersion{Service: service, Version: version.GetVersion(), Major: version.GetMajor()})
	}
	return mapped, nil
}

func CompletionPayloadFromSDK(value sdk.CompleteResponse) (*CompletionPayload, error) {
	mapped := &CompletionPayload{NoFileCompletion: value.NoFileCompletion}
	for _, candidate := range value.Candidates {
		mapped.Candidates = append(mapped.Candidates, &CompletionCandidate{Value: candidate.Value, Description: candidate.Description})
	}
	return mapped, nil
}

func CompletionPayloadToSDK(value *CompletionPayload) (sdk.CompleteResponse, error) {
	if value == nil {
		return sdk.CompleteResponse{}, fmt.Errorf("%w: completion payload is missing", ErrMalformed)
	}
	mapped := sdk.CompleteResponse{NoFileCompletion: value.GetNoFileCompletion()}
	for _, candidate := range value.GetCandidates() {
		if candidate == nil {
			return sdk.CompleteResponse{}, fmt.Errorf("%w: completion candidate is missing", ErrMalformed)
		}
		mapped.Candidates = append(mapped.Candidates, sdk.CompletionCandidate{Value: candidate.GetValue(), Description: candidate.GetDescription()})
	}
	return mapped, nil
}

func TargetStartFromSDK(value sdk.TargetDiscoveryRequest) (*TargetStart, error) {
	mapped := &TargetStart{Capability: value.Capability}
	if value.Parent != nil {
		mapped.Parent = &TargetRef{Kind: value.Parent.Kind, Id: value.Parent.ID}
	}
	return mapped, nil
}

func TargetStartToSDK(value *TargetStart) (sdk.TargetDiscoveryRequest, error) {
	if value == nil {
		return sdk.TargetDiscoveryRequest{}, fmt.Errorf("%w: target start is missing", ErrMalformed)
	}
	mapped := sdk.TargetDiscoveryRequest{Capability: value.GetCapability()}
	if parent := value.GetParent(); parent != nil {
		mapped.Parent = &sdk.TargetRef{Kind: parent.GetKind(), ID: parent.GetId()}
	}
	return mapped, nil
}

func TargetDiscoveryResultFromSDK(value sdk.TargetDiscoveryResult) (*TargetDiscoveryResult, error) {
	mapped := &TargetDiscoveryResult{}
	for _, target := range value.Targets {
		mapped.Targets = append(mapped.Targets, &TargetDescriptor{Kind: target.Kind, Id: target.ID, ParentId: target.ParentID, Name: target.Name, State: target.State, ResourceUri: target.ResourceURI})
	}
	return mapped, nil
}

func TargetDiscoveryResultToSDK(value *TargetDiscoveryResult) (sdk.TargetDiscoveryResult, error) {
	if value == nil {
		return sdk.TargetDiscoveryResult{}, fmt.Errorf("%w: target discovery result is missing", ErrMalformed)
	}
	mapped := sdk.TargetDiscoveryResult{}
	for _, target := range value.GetTargets() {
		if target == nil {
			return sdk.TargetDiscoveryResult{}, fmt.Errorf("%w: target descriptor is missing", ErrMalformed)
		}
		mapped.Targets = append(mapped.Targets, sdk.TargetDescriptor{Kind: target.GetKind(), ID: target.GetId(), ParentID: target.GetParentId(), Name: target.GetName(), State: target.GetState(), ResourceURI: target.GetResourceUri()})
	}
	return mapped, nil
}

func continuationFromSDK(value sdk.ContinuationControl) (*ContinuationControl, error) {
	switch value.Mode {
	case sdk.ContinuationUnspecified:
		if value.MaxPages != 0 || value.MaxItems != 0 || value.MaxBytes != 0 {
			return nil, fmt.Errorf("%w: unspecified continuation contains traversal ceilings", ErrProtocol)
		}
		return nil, nil
	case sdk.ContinuationSinglePage:
		if value.MaxPages != 0 || value.MaxItems != 0 || value.MaxBytes != 0 {
			return nil, fmt.Errorf("%w: single-page continuation contains traversal ceilings", ErrProtocol)
		}
		return &ContinuationControl{Mode: ContinuationMode_CONTINUATION_MODE_SINGLE_PAGE}, nil
	case sdk.ContinuationAllPages:
		if value != sdk.AllPagesContinuationControl() {
			return nil, fmt.Errorf("%w: all-pages continuation is not canonical", ErrProtocol)
		}
		return &ContinuationControl{Mode: ContinuationMode_CONTINUATION_MODE_ALL_PAGES, MaxPages: value.MaxPages, MaxItems: value.MaxItems, MaxBytes: value.MaxBytes}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported continuation mode %d", ErrProtocol, value.Mode)
	}
}

func continuationToSDK(value *ContinuationControl) (sdk.ContinuationControl, error) {
	if value == nil {
		return sdk.ContinuationControl{}, nil
	}
	mapped := sdk.ContinuationControl{MaxPages: value.GetMaxPages(), MaxItems: value.GetMaxItems(), MaxBytes: value.GetMaxBytes()}
	switch value.GetMode() {
	case ContinuationMode_CONTINUATION_MODE_UNSPECIFIED:
		if mapped.MaxPages != 0 || mapped.MaxItems != 0 || mapped.MaxBytes != 0 {
			return sdk.ContinuationControl{}, fmt.Errorf("%w: unspecified continuation contains traversal ceilings", ErrProtocol)
		}
		return sdk.ContinuationControl{}, nil
	case ContinuationMode_CONTINUATION_MODE_SINGLE_PAGE:
		if mapped.MaxPages != 0 || mapped.MaxItems != 0 || mapped.MaxBytes != 0 {
			return sdk.ContinuationControl{}, fmt.Errorf("%w: single-page continuation contains traversal ceilings", ErrProtocol)
		}
		return sdk.SinglePageContinuationControl(), nil
	case ContinuationMode_CONTINUATION_MODE_ALL_PAGES:
		mapped.Mode = sdk.ContinuationAllPages
		if mapped != sdk.AllPagesContinuationControl() {
			return sdk.ContinuationControl{}, fmt.Errorf("%w: all-pages continuation is not canonical", ErrProtocol)
		}
		return mapped, nil
	default:
		return sdk.ContinuationControl{}, fmt.Errorf("%w: unsupported continuation mode %d", ErrProtocol, value.GetMode())
	}
}

func ProductRequestFromSDK(value sdk.Request) (*ProductRequest, error) {
	service, err := serviceFromSDK(value.Service)
	if err != nil {
		return nil, err
	}
	if (value.HTTP == nil) == (value.GRPC == nil) {
		return nil, fmt.Errorf("%w: product request must select one transport", ErrProtocol)
	}
	mapped := &ProductRequest{Service: service, Capability: value.Capability, OperationId: value.Operation}
	if value.HTTP != nil {
		http := &HttpRequest{Method: value.HTTP.Method, Path: value.HTTP.Path, Body: cloneBytes(value.HTTP.Body), ContentType: value.HTTP.ContentType, Query: make(map[string]*StringList, len(value.HTTP.Query)), Headers: make(map[string]*StringList, len(value.HTTP.Headers))}
		for key, values := range value.HTTP.Query {
			http.Query[key] = &StringList{Values: cloneStrings(values)}
		}
		for key, values := range value.HTTP.Headers {
			http.Headers[key] = &StringList{Values: cloneStrings(values)}
		}
		mapped.Transport = &ProductRequest_Http{Http: http}
	} else {
		mapped.Transport = &ProductRequest_Grpc{Grpc: &GrpcRequest{FullMethod: value.GRPC.FullMethod, Message: cloneBytes(value.GRPC.Message)}}
	}
	return mapped, nil
}

func ProductRequestToSDK(value *ProductRequest) (sdk.Request, error) {
	if value == nil {
		return sdk.Request{}, fmt.Errorf("%w: product request is missing", ErrMalformed)
	}
	service, err := serviceToSDK(value.GetService())
	if err != nil {
		return sdk.Request{}, err
	}
	mapped := sdk.Request{Service: service, Capability: value.GetCapability(), Operation: value.GetOperationId()}
	switch transport := value.GetTransport().(type) {
	case *ProductRequest_Http:
		if transport.Http == nil {
			return sdk.Request{}, fmt.Errorf("%w: HTTP request is missing", ErrMalformed)
		}
		http := &sdk.HTTPRequest{Method: transport.Http.GetMethod(), Path: transport.Http.GetPath(), Body: cloneBytes(transport.Http.GetBody()), ContentType: transport.Http.GetContentType(), Query: make(map[string][]string, len(transport.Http.GetQuery())), Headers: make(map[string][]string, len(transport.Http.GetHeaders()))}
		for key, values := range transport.Http.GetQuery() {
			if values == nil {
				return sdk.Request{}, fmt.Errorf("%w: HTTP query value is missing", ErrMalformed)
			}
			http.Query[key] = cloneStrings(values.GetValues())
		}
		for key, values := range transport.Http.GetHeaders() {
			if values == nil {
				return sdk.Request{}, fmt.Errorf("%w: HTTP header value is missing", ErrMalformed)
			}
			http.Headers[key] = cloneStrings(values.GetValues())
		}
		mapped.HTTP = http
	case *ProductRequest_Grpc:
		if transport.Grpc == nil {
			return sdk.Request{}, fmt.Errorf("%w: gRPC request is missing", ErrMalformed)
		}
		mapped.GRPC = &sdk.GRPCRequest{FullMethod: transport.Grpc.GetFullMethod(), Message: cloneBytes(transport.Grpc.GetMessage())}
	default:
		return sdk.Request{}, fmt.Errorf("%w: product request transport is missing", ErrProtocol)
	}
	return mapped, nil
}

func ProductResponseFromSDK(value sdk.Response) *ProductResponse {
	return &ProductResponse{Body: cloneBytes(value.Body), Status: value.Status, ContentType: value.ContentType}
}

func ProductResponseToSDK(value *ProductResponse) sdk.Response {
	if value == nil {
		return sdk.Response{}
	}
	return sdk.Response{Body: cloneBytes(value.GetBody()), Status: value.GetStatus(), ContentType: value.GetContentType()}
}

func CommandEventFromSDK(value sdk.Event) (*CommandEvent, error) {
	kind, err := eventKindFromSDK(value.Kind)
	if err != nil {
		return nil, err
	}
	mapped := &CommandEvent{Kind: kind, Payload: cloneBytes(value.Payload)}
	if value.Result != nil {
		shape, err := resultShapeFromSDK(value.Result.Shape)
		if err != nil {
			return nil, err
		}
		mapped.Result = &ResultEnvelope{OperationId: value.Result.OperationID, Shape: shape, MediaType: value.Result.MediaType, Data: cloneBytes(value.Result.Data)}
		if value.Result.Page != nil {
			mapped.Result.Page = &PageInfo{NextCursor: value.Result.Page.NextCursor, HasMore: value.Result.Page.HasMore}
		}
	}
	return mapped, nil
}

func CommandEventToSDK(value *CommandEvent) (sdk.Event, error) {
	if value == nil {
		return sdk.Event{}, fmt.Errorf("%w: command event is missing", ErrMalformed)
	}
	kind, err := eventKindToSDK(value.GetKind())
	if err != nil {
		return sdk.Event{}, err
	}
	mapped := sdk.Event{Kind: kind, Payload: cloneBytes(value.GetPayload())}
	if result := value.GetResult(); result != nil {
		shape, err := resultShapeToSDK(result.GetShape())
		if err != nil {
			return sdk.Event{}, err
		}
		mapped.Result = &sdk.ResultEnvelope{OperationID: result.GetOperationId(), Shape: shape, MediaType: result.GetMediaType(), Data: cloneBytes(result.GetData())}
		if page := result.GetPage(); page != nil {
			mapped.Result.Page = &sdk.PageInfo{NextCursor: page.GetNextCursor(), HasMore: page.GetHasMore()}
		}
	}
	return mapped, nil
}

func LogPayloadFromSDK(value sdk.LogEntry) (*LogPayload, error) {
	level, err := logLevelFromSDK(value.Level)
	if err != nil {
		return nil, err
	}
	return &LogPayload{Level: level, Message: value.Message}, nil
}

func LogPayloadToSDK(value *LogPayload) (sdk.LogEntry, error) {
	if value == nil {
		return sdk.LogEntry{}, fmt.Errorf("%w: log payload is missing", ErrMalformed)
	}
	level, err := logLevelToSDK(value.GetLevel())
	if err != nil {
		return sdk.LogEntry{}, err
	}
	return sdk.LogEntry{Level: level, Message: value.GetMessage()}, nil
}

func unmarshalStrict(payload []byte, message proto.Message, path string) error {
	if len(payload) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrMalformed, path)
	}
	if err := validateStrictWire(payload, message.ProtoReflect().Descriptor(), path); err != nil {
		return err
	}
	if err := proto.Unmarshal(payload, message); err != nil {
		return fmt.Errorf("%w: %s", ErrMalformed, path)
	}
	if err := rejectUnknown(message.ProtoReflect(), path); err != nil {
		return err
	}
	return nil
}

// UnmarshalStrict decodes one governed component-bridge protobuf message
// without protobuf's tolerant last-one-wins or unknown-field behavior. Guest
// and host adapters use it at the public wire boundary so duplicate singular
// fields, duplicate oneofs, non-canonical scalars, and unknown nested fields
// remain observable protocol errors.
func UnmarshalStrict(encoded []byte, message proto.Message) error {
	if message == nil {
		return fmt.Errorf("%w: destination message is missing", ErrMalformed)
	}
	return unmarshalStrict(encoded, message, string(message.ProtoReflect().Descriptor().FullName()))
}

// validateStrictWire enforces the generated message shape before protobuf's
// intentionally tolerant last-one-wins decoder can erase duplicate evidence.
// It delegates value decoding to protowire and uses reflection only to identify
// known fields, cardinality, wire types, nested messages, and oneof groups.
func validateStrictWire(encoded []byte, descriptor protoreflect.MessageDescriptor, path string) error {
	seen := make(map[protowire.Number]struct{})
	seenOneofs := make(map[protoreflect.FullName]protowire.Number)
	for len(encoded) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(encoded)
		if tagBytes < 0 || number <= 0 {
			return malformedWire(path, tagBytes)
		}
		if tagBytes != protowire.SizeVarint(uint64(number)<<3|uint64(wireType)) {
			return fmt.Errorf("%w: non-canonical tag at %s", ErrMalformed, path)
		}
		encoded = encoded[tagBytes:]
		field := descriptor.Fields().ByNumber(protoreflect.FieldNumber(number))
		if field == nil {
			return fmt.Errorf("%w: unknown field %d at %s", ErrMalformed, number, path)
		}
		if want := expectedWireType(field); wireType != want {
			return fmt.Errorf("%w: field %s.%s has wire type %d, want %d", ErrMalformed, path, field.Name(), wireType, want)
		}
		if field.Cardinality() != protoreflect.Repeated {
			if _, duplicate := seen[number]; duplicate {
				return fmt.Errorf("%w: duplicate field %s.%s", ErrMalformed, path, field.Name())
			}
			seen[number] = struct{}{}
		}
		if oneof := field.ContainingOneof(); oneof != nil {
			if previous, duplicate := seenOneofs[oneof.FullName()]; duplicate {
				return fmt.Errorf("%w: duplicate oneof %s at %s (fields %d and %d)", ErrMalformed, oneof.Name(), path, previous, number)
			}
			seenOneofs[oneof.FullName()] = number
		}

		var valueBytes int
		switch wireType {
		case protowire.VarintType:
			var value uint64
			value, valueBytes = protowire.ConsumeVarint(encoded)
			if valueBytes >= 0 {
				if valueBytes != protowire.SizeVarint(value) {
					return fmt.Errorf("%w: non-canonical varint at %s.%s", ErrMalformed, path, field.Name())
				}
				if err := validateVarintScalar(value, field, path); err != nil {
					return err
				}
			}
		case protowire.Fixed32Type:
			_, valueBytes = protowire.ConsumeFixed32(encoded)
		case protowire.Fixed64Type:
			_, valueBytes = protowire.ConsumeFixed64(encoded)
		case protowire.BytesType:
			var value []byte
			value, valueBytes = protowire.ConsumeBytes(encoded)
			if valueBytes >= 0 {
				if prefixBytes := valueBytes - len(value); prefixBytes != protowire.SizeVarint(uint64(len(value))) {
					return fmt.Errorf("%w: non-canonical length at %s.%s", ErrMalformed, path, field.Name())
				}
				switch {
				case field.IsPacked():
					if err := validatePackedScalars(value, field, path); err != nil {
						return err
					}
				case field.Kind() == protoreflect.MessageKind:
					if err := validateStrictWire(value, field.Message(), path+"."+string(field.Name())); err != nil {
						return err
					}
				case field.Kind() == protoreflect.StringKind && !utf8.Valid(value):
					return fmt.Errorf("%w: invalid UTF-8 at %s.%s", ErrMalformed, path, field.Name())
				}
			}
		default:
			return fmt.Errorf("%w: unsupported wire type %d at %s.%s", ErrMalformed, wireType, path, field.Name())
		}
		if valueBytes < 0 {
			return malformedWire(path+"."+string(field.Name()), valueBytes)
		}
		encoded = encoded[valueBytes:]
	}
	return nil
}

func validatePackedScalars(encoded []byte, field protoreflect.FieldDescriptor, path string) error {
	wireType := unpackedWireType(field)
	for len(encoded) > 0 {
		var valueBytes int
		switch wireType {
		case protowire.VarintType:
			value, size := protowire.ConsumeVarint(encoded)
			valueBytes = size
			if size >= 0 {
				if size != protowire.SizeVarint(value) {
					return fmt.Errorf("%w: non-canonical packed varint at %s.%s", ErrMalformed, path, field.Name())
				}
				if err := validateVarintScalar(value, field, path); err != nil {
					return err
				}
			}
		case protowire.Fixed32Type:
			_, valueBytes = protowire.ConsumeFixed32(encoded)
		case protowire.Fixed64Type:
			_, valueBytes = protowire.ConsumeFixed64(encoded)
		default:
			return fmt.Errorf("%w: unsupported packed field %s.%s", ErrMalformed, path, field.Name())
		}
		if valueBytes < 0 {
			return malformedWire(path+"."+string(field.Name()), valueBytes)
		}
		encoded = encoded[valueBytes:]
	}
	return nil
}

func validateVarintScalar(value uint64, field protoreflect.FieldDescriptor, path string) error {
	invalid := false
	switch field.Kind() {
	case protoreflect.BoolKind:
		invalid = value > 1
	case protoreflect.EnumKind:
		number, ok := int32Varint(value)
		invalid = !ok || field.Enum().Values().ByNumber(protoreflect.EnumNumber(number)) == nil
	case protoreflect.Int32Kind:
		_, ok := int32Varint(value)
		invalid = !ok
	case protoreflect.Sint32Kind, protoreflect.Uint32Kind:
		invalid = value > uint64(^uint32(0))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		// Every uint64 bit pattern represents one value for these domains.
	default:
		return fmt.Errorf("%w: unexpected varint field %s.%s", ErrMalformed, path, field.Name())
	}
	if invalid {
		return fmt.Errorf("%w: scalar value out of range at %s.%s", ErrMalformed, path, field.Name())
	}
	return nil
}

func int32Varint(value uint64) (int32, bool) {
	signed := int64(value)
	if signed < math.MinInt32 || signed > math.MaxInt32 {
		return 0, false
	}
	return int32(signed), true
}

func expectedWireType(field protoreflect.FieldDescriptor) protowire.Type {
	if field.IsPacked() {
		return protowire.BytesType
	}
	return unpackedWireType(field)
}

func unpackedWireType(field protoreflect.FieldDescriptor) protowire.Type {
	switch field.Kind() {
	case protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		return protowire.VarintType
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		return protowire.Fixed32Type
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return protowire.Fixed64Type
	default:
		return protowire.BytesType
	}
}

func malformedWire(path string, code int) error {
	if code < 0 {
		return fmt.Errorf("%w: %s: %v", ErrMalformed, path, protowire.ParseError(code))
	}
	return fmt.Errorf("%w: %s", ErrMalformed, path)
}

// DecodeDescriptorEnvelope negotiates the outer governed identity before it
// decodes the descriptor payload.
func DecodeDescriptorEnvelope(encoded []byte) (Descriptor, error) {
	if len(encoded) == 0 {
		return Descriptor{}, fmt.Errorf("%w: envelope is empty", ErrMalformed)
	}
	var envelope PluginEnvelope
	if err := validateStrictWire(encoded, envelope.ProtoReflect().Descriptor(), "plugin_envelope"); err != nil {
		return Descriptor{}, err
	}
	if err := proto.Unmarshal(encoded, &envelope); err != nil {
		return Descriptor{}, fmt.Errorf("%w: envelope", ErrMalformed)
	}
	if err := rejectUnknown(envelope.ProtoReflect(), "plugin_envelope"); err != nil {
		return Descriptor{}, err
	}
	if envelope.GetProtocolMajor() != ProtocolMajor ||
		envelope.GetFacetKind() != FacetKind_FACET_KIND_UNSPECIFIED ||
		envelope.GetMessageKind() != MessageKind_MESSAGE_KIND_DESCRIPTOR_SET ||
		envelope.GetExecutionId() != "" {
		return Descriptor{}, ErrProtocol
	}

	var payload PluginDescriptorSet
	if err := validateStrictWire(envelope.GetPayload(), payload.ProtoReflect().Descriptor(), "plugin_descriptor_set"); err != nil {
		return Descriptor{}, err
	}
	if err := proto.Unmarshal(envelope.GetPayload(), &payload); err != nil {
		return Descriptor{}, fmt.Errorf("%w: descriptor", ErrMalformed)
	}
	if err := rejectUnknown(payload.ProtoReflect(), "plugin_descriptor_set"); err != nil {
		return Descriptor{}, err
	}
	if payload.GetMetadata() == nil {
		return Descriptor{}, fmt.Errorf("%w: metadata is missing", ErrMalformed)
	}
	metadata := sdk.Metadata{Name: payload.GetMetadata().GetName(), Version: payload.GetMetadata().GetVersion()}
	for _, facet := range payload.GetMetadata().GetFacets() {
		if facet == nil {
			return Descriptor{}, fmt.Errorf("%w: facet is missing", ErrMalformed)
		}
		kind, err := facetKindToSDK(facet.GetKind())
		if err != nil {
			return Descriptor{}, err
		}
		metadata.Facets = append(metadata.Facets, sdk.Facet{
			Kind:                     kind,
			ProtocolVersion:          facet.GetProtocolVersion(),
			Capabilities:             append([]string(nil), facet.GetCapabilities()...),
			RequiredHostCapabilities: append([]string(nil), facet.GetRequiredHostCapabilities()...),
		})
	}
	commands := make([]sdk.Command, len(payload.GetCommands()))
	for index, command := range payload.GetCommands() {
		mapped, err := commandToSDK(command)
		if err != nil {
			return Descriptor{}, fmt.Errorf("command %d: %w", index, err)
		}
		commands[index] = mapped
	}
	documentation := make([]sdk.DocumentationResource, len(payload.GetDocumentationResources()))
	for index, resource := range payload.GetDocumentationResources() {
		mapped, err := documentationToSDK(resource)
		if err != nil {
			return Descriptor{}, fmt.Errorf("documentation resource %d: %w", index, err)
		}
		documentation[index] = mapped
	}
	authProviders := make([]AuthProvider, len(payload.GetAuthProviders()))
	for index, provider := range payload.GetAuthProviders() {
		if provider == nil {
			return Descriptor{}, fmt.Errorf("%w: auth provider %d is missing", ErrMalformed, index)
		}
		authProviders[index] = AuthProvider{Metadata: sdk.AuthProviderMetadata{Name: provider.GetName(), Version: provider.GetVersion()}, ProtocolVersion: provider.GetProtocolVersion(), Capabilities: cloneStrings(provider.GetCapabilities())}
	}
	targetProviders := make([]sdk.TargetProviderMetadata, len(payload.GetTargetProviders()))
	for index, provider := range payload.GetTargetProviders() {
		if provider == nil {
			return Descriptor{}, fmt.Errorf("%w: target provider %d is missing", ErrMalformed, index)
		}
		mapped := sdk.TargetProviderMetadata{Name: provider.GetName(), Version: provider.GetVersion(), Capabilities: cloneStrings(provider.GetCapabilities())}
		for operationIndex, operation := range provider.GetOperations() {
			if operation == nil {
				return Descriptor{}, fmt.Errorf("%w: target provider %d operation %d is missing", ErrMalformed, index, operationIndex)
			}
			mappedOperation, err := operationToSDK(operation.GetOperation())
			if err != nil {
				return Descriptor{}, fmt.Errorf("target provider %d operation %d: %w", index, operationIndex, err)
			}
			auth := make([]sdk.AuthRequirement, len(operation.GetAuth()))
			for authIndex, requirement := range operation.GetAuth() {
				if requirement == nil {
					return Descriptor{}, fmt.Errorf("%w: target provider %d auth requirement %d is missing", ErrMalformed, index, authIndex)
				}
				auth[authIndex] = sdk.AuthRequirement{Capability: requirement.GetCapability(), Optional: requirement.GetOptional()}
			}
			mapped.Operations = append(mapped.Operations, sdk.TargetOperationPolicy{Operation: mappedOperation, Auth: auth})
		}
		targetProviders[index] = mapped
	}
	signerProviders := make([]SignerProvider, len(payload.GetSignerProviders()))
	for index, provider := range payload.GetSignerProviders() {
		if provider == nil {
			return Descriptor{}, fmt.Errorf("%w: signer provider %d is missing", ErrMalformed, index)
		}
		signerProviders[index] = SignerProvider{Metadata: sdk.SignerProviderMetadata{Name: provider.GetName(), Version: provider.GetVersion()}, ProtocolVersion: provider.GetProtocolVersion(), Capabilities: cloneStrings(provider.GetCapabilities())}
	}
	return Descriptor{Metadata: metadata, Commands: commands, AuthProviders: authProviders, TargetProviders: targetProviders, SignerProviders: signerProviders, DocumentationResources: documentation}, nil
}

// EncodeDescriptorEnvelope maps the public SDK descriptor to the governed
// payload and outer-envelope bindings.
func EncodeDescriptorEnvelope(descriptor Descriptor) ([]byte, error) {
	metadata := &PluginMetadata{Name: descriptor.Metadata.Name, Version: descriptor.Metadata.Version}
	for _, facet := range descriptor.Metadata.Facets {
		kind, err := facetKindFromSDK(facet.Kind)
		if err != nil {
			return nil, err
		}
		metadata.Facets = append(metadata.Facets, &FacetDescriptor{Kind: kind, ProtocolVersion: facet.ProtocolVersion, Capabilities: cloneStrings(facet.Capabilities), RequiredHostCapabilities: cloneStrings(facet.RequiredHostCapabilities)})
	}
	payload := &PluginDescriptorSet{Metadata: metadata}
	for index := range descriptor.Commands {
		mapped, err := commandFromSDK(descriptor.Commands[index])
		if err != nil {
			return nil, fmt.Errorf("command %d: %w", index, err)
		}
		payload.Commands = append(payload.Commands, mapped)
	}
	for _, provider := range descriptor.AuthProviders {
		payload.AuthProviders = append(payload.AuthProviders, &AuthProviderDescriptor{Name: provider.Metadata.Name, Version: provider.Metadata.Version, ProtocolVersion: provider.ProtocolVersion, Capabilities: cloneStrings(provider.Capabilities)})
	}
	for providerIndex, provider := range descriptor.TargetProviders {
		mapped := &TargetProviderDescriptor{Name: provider.Name, Version: provider.Version, Capabilities: cloneStrings(provider.Capabilities)}
		for operationIndex, operation := range provider.Operations {
			mappedOperation, err := operationFromSDK(operation.Operation)
			if err != nil {
				return nil, fmt.Errorf("target provider %d operation %d: %w", providerIndex, operationIndex, err)
			}
			wireOperation := &TargetOperationPolicy{Operation: mappedOperation}
			for _, requirement := range operation.Auth {
				wireOperation.Auth = append(wireOperation.Auth, &AuthRequirement{Capability: requirement.Capability, Optional: requirement.Optional})
			}
			mapped.Operations = append(mapped.Operations, wireOperation)
		}
		payload.TargetProviders = append(payload.TargetProviders, mapped)
	}
	for _, provider := range descriptor.SignerProviders {
		payload.SignerProviders = append(payload.SignerProviders, &SignerProviderDescriptor{Name: provider.Metadata.Name, Version: provider.Metadata.Version, ProtocolVersion: provider.ProtocolVersion, Capabilities: cloneStrings(provider.Capabilities)})
	}
	for index := range descriptor.DocumentationResources {
		mapped, err := documentationFromSDK(descriptor.DocumentationResources[index])
		if err != nil {
			return nil, fmt.Errorf("documentation resource %d: %w", index, err)
		}
		payload.DocumentationResources = append(payload.DocumentationResources, mapped)
	}
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: descriptor", ErrMalformed)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&PluginEnvelope{ProtocolMajor: ProtocolMajor, MessageKind: MessageKind_MESSAGE_KIND_DESCRIPTOR_SET, Payload: payloadBytes})
	if err != nil {
		return nil, fmt.Errorf("%w: envelope", ErrMalformed)
	}
	return encoded, nil
}

func commandToSDK(command *CommandDescriptor) (sdk.Command, error) {
	if command == nil {
		return sdk.Command{}, fmt.Errorf("%w: command is missing", ErrMalformed)
	}
	target, err := targetToSDK(command.GetTarget())
	if err != nil {
		return sdk.Command{}, err
	}
	executionKind, err := executionKindToSDK(command.GetExecutionKind())
	if err != nil {
		return sdk.Command{}, err
	}
	authMode, err := authModeToSDK(command.GetAuthMode())
	if err != nil {
		return sdk.Command{}, err
	}
	risk, err := riskToSDK(command.GetRisk())
	if err != nil {
		return sdk.Command{}, err
	}
	mapped := sdk.Command{
		ID: command.GetId(), ExecutionKind: executionKind, AuthMode: authMode, Path: cloneStrings(command.GetPath()), Target: target,
		Summary: command.GetSummary(), Long: command.GetLongDescription(), Example: command.GetExample(), Risk: risk,
		DocumentationIDs: cloneStrings(command.GetDocumentationIds()), InputSchema: cloneBytes(command.GetInputSchema()), RawOutputSchema: cloneBytes(command.GetRawOutputSchema()), PublicOutputSchema: cloneBytes(command.GetPublicOutputSchema()), OutputMediaType: command.GetOutputMediaType(),
	}
	if policy := command.GetExecutionPolicy(); policy != nil {
		mapped.ExecutionPolicy = &sdk.CommandExecutionPolicy{MaxHostRequests: policy.GetMaxHostRequests()}
		if wait := policy.GetWait(); wait != nil {
			mapped.ExecutionPolicy.Wait = &sdk.WaitPolicy{MaxDelayMilliseconds: wait.GetMaxDelayMilliseconds(), MaxWaits: wait.GetMaxWaits(), MaxAggregateDelayMilliseconds: wait.GetMaxAggregateDelayMilliseconds()}
		}
	}
	if pagination := command.GetPagination(); pagination != nil {
		mapped.Pagination.Supported = pagination.GetSupported()
	}
	if signing := command.GetRequestSigning(); signing != nil {
		mapped.RequestSigning = &sdk.RequestSigningSpec{Capability: signing.GetCapability(), ProductMajor: signing.GetProductMajor(), PayloadType: signing.GetPayloadType(), OperationID: signing.GetOperationId(), Algorithm: signing.GetAlgorithm(), Protobuf: protobufSigningRecipeToSDK(signing.GetProtobuf())}
	}
	for _, aliases := range command.GetPathAliases() {
		if aliases == nil {
			return sdk.Command{}, fmt.Errorf("%w: path alias is missing", ErrMalformed)
		}
		mapped.PathAliases = append(mapped.PathAliases, cloneStrings(aliases.GetValues()))
	}
	for _, requirement := range command.GetRequirements() {
		if requirement == nil {
			return sdk.Command{}, fmt.Errorf("%w: capability requirement is missing", ErrMalformed)
		}
		mapped.Requires = append(mapped.Requires, sdk.CapabilityRequirement{ProviderCapability: requirement.GetProviderCapability(), Operations: cloneStrings(requirement.GetOperations()), Optional: requirement.GetOptional()})
	}
	for _, argument := range command.GetArguments() {
		if argument == nil {
			return sdk.Command{}, fmt.Errorf("%w: argument is missing", ErrMalformed)
		}
		kind, err := argumentTypeToSDK(argument.GetType())
		if err != nil {
			return sdk.Command{}, err
		}
		completion, err := completionToSDK(argument.GetCompletion())
		if err != nil {
			return sdk.Command{}, err
		}
		mapped.Arguments = append(mapped.Arguments, sdk.Argument{Name: argument.GetName(), Usage: argument.GetUsage(), Type: kind, Required: argument.GetRequired(), Repeated: argument.GetRepeated(), Completion: completion})
	}
	for _, flag := range command.GetFlags() {
		if flag == nil {
			return sdk.Command{}, fmt.Errorf("%w: flag is missing", ErrMalformed)
		}
		kind, err := flagTypeToSDK(flag.GetType())
		if err != nil {
			return sdk.Command{}, err
		}
		completion, err := completionToSDK(flag.GetCompletion())
		if err != nil {
			return sdk.Command{}, err
		}
		mapped.Flags = append(mapped.Flags, sdk.Flag{Name: flag.GetName(), Aliases: cloneStrings(flag.GetAliases()), Usage: flag.GetUsage(), Type: kind, HasDefault: flag.GetHasDefault(), DefaultValue: flag.GetDefaultValue(), Required: flag.GetRequired(), Completion: completion})
	}
	for _, requirement := range command.GetAuth() {
		if requirement == nil {
			return sdk.Command{}, fmt.Errorf("%w: auth requirement is missing", ErrMalformed)
		}
		mapped.Auth = append(mapped.Auth, sdk.AuthRequirement{Capability: requirement.GetCapability(), Optional: requirement.GetOptional()})
	}
	for _, operation := range command.GetOperations() {
		mappedOperation, err := operationToSDK(operation)
		if err != nil {
			return sdk.Command{}, err
		}
		mapped.Operations = append(mapped.Operations, mappedOperation)
	}
	for _, compatibility := range command.GetCompatibility() {
		if compatibility == nil {
			return sdk.Command{}, fmt.Errorf("%w: compatibility is missing", ErrMalformed)
		}
		service, err := serviceToSDK(compatibility.GetService())
		if err != nil {
			return sdk.Command{}, err
		}
		mapped.Compatibility = append(mapped.Compatibility, sdk.ServiceCompatibility{Service: service, Majors: cloneUint32s(compatibility.GetMajors())})
	}
	for _, output := range command.GetSensitiveOutputs() {
		if output == nil {
			return sdk.Command{}, fmt.Errorf("%w: sensitive output is missing", ErrMalformed)
		}
		mappedOutput := sdk.SensitiveOutput{JSONPointer: output.GetJsonPointer()}
		for _, delivery := range output.GetAllowedDeliveries() {
			value, err := sensitiveDeliveryToSDK(delivery)
			if err != nil {
				return sdk.Command{}, err
			}
			mappedOutput.AllowedDeliveries = append(mappedOutput.AllowedDeliveries, value)
		}
		mapped.SensitiveOutputs = append(mapped.SensitiveOutputs, mappedOutput)
	}
	for _, artifact := range command.GetInputArtifacts() {
		if artifact == nil {
			return sdk.Command{}, fmt.Errorf("%w: input artifact is missing", ErrMalformed)
		}
		mapped.InputArtifacts = append(mapped.InputArtifacts, sdk.InputArtifactSpec{ArgumentName: artifact.GetArgumentName(), FlagName: artifact.GetFlagName(), MediaTypes: cloneStrings(artifact.GetMediaTypes()), MaxBytes: artifact.GetMaxBytes(), AllowFile: artifact.GetAllowFile(), AllowStdin: artifact.GetAllowStdin(), Sensitive: artifact.GetSensitive(), Optional: artifact.GetOptional(), Repeated: artifact.GetRepeated()})
	}
	if table := command.GetRender().GetTable(); table != nil {
		mapped.Render.Table = &sdk.TableRenderHint{}
		for _, column := range table.GetColumns() {
			if column == nil {
				return sdk.Command{}, fmt.Errorf("%w: table column is missing", ErrMalformed)
			}
			mapped.Render.Table.Columns = append(mapped.Render.Table.Columns, sdk.TableColumn{Header: column.GetHeader(), Field: column.GetField()})
		}
	}
	return mapped, nil
}

func commandFromSDK(command sdk.Command) (*CommandDescriptor, error) {
	target, err := targetFromSDK(command.Target)
	if err != nil {
		return nil, err
	}
	executionKind, err := executionKindFromSDK(command.ExecutionKind)
	if err != nil {
		return nil, err
	}
	authMode, err := authModeFromSDK(command.AuthMode)
	if err != nil {
		return nil, err
	}
	risk, err := riskFromSDK(command.Risk)
	if err != nil {
		return nil, err
	}
	mapped := &CommandDescriptor{Id: command.ID, Path: cloneStrings(command.Path), Target: target, ExecutionKind: executionKind, AuthMode: authMode, Summary: command.Summary, LongDescription: command.Long, Example: command.Example, Risk: risk, DocumentationIds: cloneStrings(command.DocumentationIDs), InputSchema: cloneBytes(command.InputSchema), RawOutputSchema: cloneBytes(command.RawOutputSchema), PublicOutputSchema: cloneBytes(command.PublicOutputSchema), Pagination: &PaginationSpec{Supported: command.Pagination.Supported}, OutputMediaType: command.OutputMediaType}
	if command.ExecutionPolicy != nil {
		mapped.ExecutionPolicy = &CommandExecutionPolicy{MaxHostRequests: command.ExecutionPolicy.MaxHostRequests}
		if command.ExecutionPolicy.Wait != nil {
			mapped.ExecutionPolicy.Wait = &WaitPolicy{MaxDelayMilliseconds: command.ExecutionPolicy.Wait.MaxDelayMilliseconds, MaxWaits: command.ExecutionPolicy.Wait.MaxWaits, MaxAggregateDelayMilliseconds: command.ExecutionPolicy.Wait.MaxAggregateDelayMilliseconds}
		}
	}
	if command.RequestSigning != nil {
		mapped.RequestSigning = &RequestSigningSpec{Capability: command.RequestSigning.Capability, ProductMajor: command.RequestSigning.ProductMajor, PayloadType: command.RequestSigning.PayloadType, OperationId: command.RequestSigning.OperationID, Algorithm: command.RequestSigning.Algorithm, Protobuf: protobufSigningRecipeFromSDK(command.RequestSigning.Protobuf)}
	}
	for _, aliases := range command.PathAliases {
		mapped.PathAliases = append(mapped.PathAliases, &StringList{Values: cloneStrings(aliases)})
	}
	for _, requirement := range command.Requires {
		mapped.Requirements = append(mapped.Requirements, &CapabilityRequirement{ProviderCapability: requirement.ProviderCapability, Operations: cloneStrings(requirement.Operations), Optional: requirement.Optional})
	}
	for _, argument := range command.Arguments {
		kind, err := argumentTypeFromSDK(argument.Type)
		if err != nil {
			return nil, err
		}
		completion, err := completionFromSDK(argument.Completion)
		if err != nil {
			return nil, err
		}
		mapped.Arguments = append(mapped.Arguments, &ArgumentDescriptor{Name: argument.Name, Usage: argument.Usage, Type: kind, Required: argument.Required, Repeated: argument.Repeated, Completion: completion})
	}
	for _, flag := range command.Flags {
		kind, err := flagTypeFromSDK(flag.Type)
		if err != nil {
			return nil, err
		}
		completion, err := completionFromSDK(flag.Completion)
		if err != nil {
			return nil, err
		}
		mapped.Flags = append(mapped.Flags, &FlagDescriptor{Name: flag.Name, Aliases: cloneStrings(flag.Aliases), Usage: flag.Usage, Type: kind, HasDefault: flag.HasDefault, DefaultValue: flag.DefaultValue, Required: flag.Required, Completion: completion})
	}
	for _, requirement := range command.Auth {
		mapped.Auth = append(mapped.Auth, &AuthRequirement{Capability: requirement.Capability, Optional: requirement.Optional})
	}
	for _, operation := range command.Operations {
		value, err := operationFromSDK(operation)
		if err != nil {
			return nil, err
		}
		mapped.Operations = append(mapped.Operations, value)
	}
	for _, compatibility := range command.Compatibility {
		service, err := serviceFromSDK(compatibility.Service)
		if err != nil {
			return nil, err
		}
		mapped.Compatibility = append(mapped.Compatibility, &ServiceCompatibility{Service: service, Majors: cloneUint32s(compatibility.Majors)})
	}
	for _, output := range command.SensitiveOutputs {
		value := &SensitiveOutput{JsonPointer: output.JSONPointer}
		for _, delivery := range output.AllowedDeliveries {
			mappedDelivery, err := sensitiveDeliveryFromSDK(delivery)
			if err != nil {
				return nil, err
			}
			value.AllowedDeliveries = append(value.AllowedDeliveries, mappedDelivery)
		}
		mapped.SensitiveOutputs = append(mapped.SensitiveOutputs, value)
	}
	for _, artifact := range command.InputArtifacts {
		mapped.InputArtifacts = append(mapped.InputArtifacts, &InputArtifactSpec{ArgumentName: artifact.ArgumentName, FlagName: artifact.FlagName, MediaTypes: cloneStrings(artifact.MediaTypes), MaxBytes: artifact.MaxBytes, AllowFile: artifact.AllowFile, AllowStdin: artifact.AllowStdin, Sensitive: artifact.Sensitive, Optional: artifact.Optional, Repeated: artifact.Repeated})
	}
	if command.Render.Table != nil {
		mapped.Render = &RenderHints{Table: &TableRenderHint{}}
		for _, column := range command.Render.Table.Columns {
			mapped.Render.Table.Columns = append(mapped.Render.Table.Columns, &TableColumn{Header: column.Header, Field: column.Field})
		}
	}
	return mapped, nil
}

func rejectUnknown(message protoreflect.Message, path string) error {
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("%w: unknown field at %s", ErrMalformed, path)
	}
	var nestedErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return true
		}
		fieldPath := path + "." + string(field.Name())
		switch {
		case field.IsList():
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if nestedErr = rejectUnknown(list.Get(index).Message(), fmt.Sprintf("%s[%d]", fieldPath, index)); nestedErr != nil {
					return false
				}
			}
		case field.IsMap():
			if field.MapValue().Kind() != protoreflect.MessageKind && field.MapValue().Kind() != protoreflect.GroupKind {
				return true
			}
			value.Map().Range(func(key protoreflect.MapKey, mapped protoreflect.Value) bool {
				nestedErr = rejectUnknown(mapped.Message(), fmt.Sprintf("%s[%v]", fieldPath, key.Interface()))
				return nestedErr == nil
			})
		default:
			nestedErr = rejectUnknown(value.Message(), fieldPath)
		}
		return nestedErr == nil
	})
	return nestedErr
}

func facetKindToSDK(kind FacetKind) (sdk.FacetKind, error) {
	switch kind {
	case FacetKind_FACET_KIND_COMMAND_PROVIDER:
		return sdk.FacetCommandProvider, nil
	case FacetKind_FACET_KIND_AUTH_PROVIDER:
		return sdk.FacetAuthProvider, nil
	case FacetKind_FACET_KIND_TARGET_PROVIDER:
		return sdk.FacetTargetProvider, nil
	case FacetKind_FACET_KIND_SIGNER_PROVIDER:
		return sdk.FacetSignerProvider, nil
	default:
		return "", fmt.Errorf("%w: unsupported facet kind %d", ErrProtocol, kind)
	}
}

func facetKindFromSDK(kind sdk.FacetKind) (FacetKind, error) {
	switch kind {
	case sdk.FacetCommandProvider:
		return FacetKind_FACET_KIND_COMMAND_PROVIDER, nil
	case sdk.FacetAuthProvider:
		return FacetKind_FACET_KIND_AUTH_PROVIDER, nil
	case sdk.FacetTargetProvider:
		return FacetKind_FACET_KIND_TARGET_PROVIDER, nil
	case sdk.FacetSignerProvider:
		return FacetKind_FACET_KIND_SIGNER_PROVIDER, nil
	default:
		return FacetKind_FACET_KIND_UNSPECIFIED, fmt.Errorf("%w: unsupported facet kind %q", ErrProtocol, kind)
	}
}

func completionToSDK(value *CompletionSpec) (sdk.CompletionSpec, error) {
	if value == nil {
		return sdk.CompletionSpec{}, nil
	}
	var kind sdk.CompletionKind
	switch value.GetKind() {
	case CompletionKind_COMPLETION_KIND_UNSPECIFIED:
		kind = ""
	case CompletionKind_COMPLETION_KIND_NONE:
		kind = sdk.CompletionNone
	case CompletionKind_COMPLETION_KIND_STATIC:
		kind = sdk.CompletionStatic
	case CompletionKind_COMPLETION_KIND_DYNAMIC:
		kind = sdk.CompletionDynamic
	default:
		return sdk.CompletionSpec{}, fmt.Errorf("%w: unsupported completion kind %d", ErrProtocol, value.GetKind())
	}
	mapped := sdk.CompletionSpec{Kind: kind, NoFileCompletion: value.GetNoFileCompletion()}
	for _, candidate := range value.GetCandidates() {
		if candidate == nil {
			return sdk.CompletionSpec{}, fmt.Errorf("%w: completion candidate is missing", ErrMalformed)
		}
		mapped.Candidates = append(mapped.Candidates, sdk.CompletionCandidate{Value: candidate.GetValue(), Description: candidate.GetDescription()})
	}
	return mapped, nil
}

func completionFromSDK(value sdk.CompletionSpec) (*CompletionSpec, error) {
	var kind CompletionKind
	switch value.Kind {
	case "":
		kind = CompletionKind_COMPLETION_KIND_UNSPECIFIED
	case sdk.CompletionNone:
		kind = CompletionKind_COMPLETION_KIND_NONE
	case sdk.CompletionStatic:
		kind = CompletionKind_COMPLETION_KIND_STATIC
	case sdk.CompletionDynamic:
		kind = CompletionKind_COMPLETION_KIND_DYNAMIC
	default:
		return nil, fmt.Errorf("%w: unsupported completion kind %q", ErrProtocol, value.Kind)
	}
	mapped := &CompletionSpec{Kind: kind, NoFileCompletion: value.NoFileCompletion}
	for _, candidate := range value.Candidates {
		mapped.Candidates = append(mapped.Candidates, &CompletionCandidate{Value: candidate.Value, Description: candidate.Description})
	}
	return mapped, nil
}

func operationToSDK(value *OperationPolicy) (sdk.OperationPolicy, error) {
	if value == nil {
		return sdk.OperationPolicy{}, fmt.Errorf("%w: operation is missing", ErrMalformed)
	}
	service, err := serviceToSDK(value.GetService())
	if err != nil {
		return sdk.OperationPolicy{}, err
	}
	mapped := sdk.OperationPolicy{ID: value.GetId(), Service: service}
	if value.GetScopes() != nil {
		mapped.Scopes = append([]string{}, value.GetScopes().GetValues()...)
	}
	switch transport := value.GetTransport().(type) {
	case *OperationPolicy_Http:
		if transport.Http == nil {
			return sdk.OperationPolicy{}, fmt.Errorf("%w: HTTP operation is missing", ErrMalformed)
		}
		mapped.HTTP = &sdk.HTTPOperationPolicy{Method: transport.Http.GetMethod(), Path: transport.Http.GetPath()}
		if generated := transport.Http.GetGeneratedClient(); generated != nil {
			mapped.HTTP.GeneratedClient = &sdk.HTTPGeneratedClientPolicy{PathTemplate: generated.GetPathTemplate(), RequestContentTypes: cloneStrings(generated.GetRequestContentTypes()), RequestHeaders: cloneStrings(generated.GetRequestHeaders()), MaxRequestBytes: generated.GetMaxRequestBytes(), ResponseLimits: responseLimitsToSDK(generated.GetResponseLimits())}
		}
	case *OperationPolicy_Grpc:
		if transport.Grpc == nil {
			return sdk.OperationPolicy{}, fmt.Errorf("%w: gRPC operation is missing", ErrMalformed)
		}
		mapped.GRPC = &sdk.GRPCOperationPolicy{FullMethod: transport.Grpc.GetFullMethod(), ServerStreaming: transport.Grpc.GetServerStreaming()}
		if generated := transport.Grpc.GetGeneratedClient(); generated != nil {
			mapped.GRPC.GeneratedClient = &sdk.GRPCGeneratedClientPolicy{MaxRequestMessageBytes: generated.GetMaxRequestMessageBytes(), ResponseLimits: responseLimitsToSDK(generated.GetResponseLimits())}
		}
	case nil:
	default:
		return sdk.OperationPolicy{}, fmt.Errorf("%w: unsupported operation transport", ErrProtocol)
	}
	return mapped, nil
}

func operationFromSDK(value sdk.OperationPolicy) (*OperationPolicy, error) {
	service, err := serviceFromSDK(value.Service)
	if err != nil {
		return nil, err
	}
	mapped := &OperationPolicy{Id: value.ID, Service: service}
	if value.Scopes != nil {
		mapped.Scopes = &ScopeSet{Values: cloneStrings(value.Scopes)}
	}
	if value.HTTP != nil && value.GRPC != nil {
		return nil, fmt.Errorf("%w: multiple operation transports", ErrProtocol)
	}
	if value.HTTP != nil {
		http := &HttpOperationPolicy{Method: value.HTTP.Method, Path: value.HTTP.Path}
		if generated := value.HTTP.GeneratedClient; generated != nil {
			http.GeneratedClient = &GeneratedHttpPolicy{PathTemplate: generated.PathTemplate, RequestContentTypes: cloneStrings(generated.RequestContentTypes), RequestHeaders: cloneStrings(generated.RequestHeaders), MaxRequestBytes: generated.MaxRequestBytes, ResponseLimits: responseLimitsFromSDK(generated.ResponseLimits)}
		}
		mapped.Transport = &OperationPolicy_Http{Http: http}
	}
	if value.GRPC != nil {
		grpc := &GrpcOperationPolicy{FullMethod: value.GRPC.FullMethod, ServerStreaming: value.GRPC.ServerStreaming}
		if generated := value.GRPC.GeneratedClient; generated != nil {
			grpc.GeneratedClient = &GeneratedGrpcPolicy{MaxRequestMessageBytes: generated.MaxRequestMessageBytes, ResponseLimits: responseLimitsFromSDK(generated.ResponseLimits)}
		}
		mapped.Transport = &OperationPolicy_Grpc{Grpc: grpc}
	}
	return mapped, nil
}

func responseLimitsToSDK(value *ResponseLimits) sdk.ResponseLimits {
	if value == nil {
		return sdk.ResponseLimits{}
	}
	return sdk.ResponseLimits{MaxMessageBytes: value.GetMaxMessageBytes(), MaxMessages: value.GetMaxMessages(), MaxAggregateBytes: value.GetMaxAggregateBytes()}
}

func responseLimitsFromSDK(value sdk.ResponseLimits) *ResponseLimits {
	return &ResponseLimits{MaxMessageBytes: value.MaxMessageBytes, MaxMessages: value.MaxMessages, MaxAggregateBytes: value.MaxAggregateBytes}
}

func protobufSigningRecipeToSDK(recipe *OpaqueProtobufSigningRecipe) *sdk.OpaqueProtobufSigningRecipe {
	if recipe == nil {
		return nil
	}
	fields := make([]sdk.ProtobufField, len(recipe.GetPassthroughFields()))
	for index, field := range recipe.GetPassthroughFields() {
		if field != nil {
			fields[index] = sdk.ProtobufField{Number: field.GetNumber(), WireType: field.GetWireType()}
		}
	}
	return &sdk.OpaqueProtobufSigningRecipe{
		UnsignedField: recipe.GetUnsignedField(), SignedField: recipe.GetSignedField(), EnvelopeKeyIDField: recipe.GetEnvelopeKeyIdField(),
		EnvelopeSignatureField: recipe.GetEnvelopeSignatureField(), EnvelopePayloadField: recipe.GetEnvelopePayloadField(), PassthroughFields: fields,
		MaxPayloadBytes: recipe.GetMaxPayloadBytes(), MaxSignedMessageBytes: recipe.GetMaxSignedMessageBytes(), MaxKeyIDBytes: recipe.GetMaxKeyIdBytes(), SignatureLength: recipe.GetSignatureLength(),
	}
}

func protobufSigningRecipeFromSDK(recipe *sdk.OpaqueProtobufSigningRecipe) *OpaqueProtobufSigningRecipe {
	if recipe == nil {
		return nil
	}
	fields := make([]*ProtobufField, len(recipe.PassthroughFields))
	for index, field := range recipe.PassthroughFields {
		fields[index] = &ProtobufField{Number: field.Number, WireType: field.WireType}
	}
	return &OpaqueProtobufSigningRecipe{
		UnsignedField: recipe.UnsignedField, SignedField: recipe.SignedField, EnvelopeKeyIdField: recipe.EnvelopeKeyIDField,
		EnvelopeSignatureField: recipe.EnvelopeSignatureField, EnvelopePayloadField: recipe.EnvelopePayloadField, PassthroughFields: fields,
		MaxPayloadBytes: recipe.MaxPayloadBytes, MaxSignedMessageBytes: recipe.MaxSignedMessageBytes, MaxKeyIdBytes: recipe.MaxKeyIDBytes, SignatureLength: recipe.SignatureLength,
	}
}

func documentationToSDK(value *DocumentationResource) (sdk.DocumentationResource, error) {
	if value == nil {
		return sdk.DocumentationResource{}, fmt.Errorf("%w: documentation resource is missing", ErrMalformed)
	}
	var kind sdk.DocumentationKind
	switch value.GetKind() {
	case DocumentationKind_DOCUMENTATION_KIND_GUIDE:
		kind = sdk.DocumentationGuide
	case DocumentationKind_DOCUMENTATION_KIND_TUTORIAL:
		kind = sdk.DocumentationTutorial
	case DocumentationKind_DOCUMENTATION_KIND_API_REFERENCE:
		kind = sdk.DocumentationAPIReference
	case DocumentationKind_DOCUMENTATION_KIND_CONCEPT:
		kind = sdk.DocumentationConcept
	case DocumentationKind_DOCUMENTATION_KIND_CHANGELOG:
		kind = sdk.DocumentationChangelog
	default:
		return sdk.DocumentationResource{}, fmt.Errorf("%w: unsupported documentation kind %d", ErrProtocol, value.GetKind())
	}
	return sdk.DocumentationResource{ID: value.GetId(), Kind: kind, Title: value.GetTitle(), Description: value.GetDescription(), URL: value.GetUrl(), BundledPath: value.GetBundledPath(), MediaType: value.GetMediaType(), Locale: value.GetLocale(), SupportedMajors: cloneUint32s(value.GetSupportedMajors())}, nil
}

func documentationFromSDK(value sdk.DocumentationResource) (*DocumentationResource, error) {
	var kind DocumentationKind
	switch value.Kind {
	case sdk.DocumentationGuide:
		kind = DocumentationKind_DOCUMENTATION_KIND_GUIDE
	case sdk.DocumentationTutorial:
		kind = DocumentationKind_DOCUMENTATION_KIND_TUTORIAL
	case sdk.DocumentationAPIReference:
		kind = DocumentationKind_DOCUMENTATION_KIND_API_REFERENCE
	case sdk.DocumentationConcept:
		kind = DocumentationKind_DOCUMENTATION_KIND_CONCEPT
	case sdk.DocumentationChangelog:
		kind = DocumentationKind_DOCUMENTATION_KIND_CHANGELOG
	default:
		return nil, fmt.Errorf("%w: unsupported documentation kind %q", ErrProtocol, value.Kind)
	}
	return &DocumentationResource{Id: value.ID, Kind: kind, Title: value.Title, Description: value.Description, Url: value.URL, BundledPath: value.BundledPath, MediaType: value.MediaType, Locale: value.Locale, SupportedMajors: cloneUint32s(value.SupportedMajors)}, nil
}

func targetToSDK(value *TargetRequirement) (sdk.TargetRequirement, error) {
	if value == nil {
		return sdk.TargetRequirement{}, fmt.Errorf("%w: target requirement is missing", ErrMalformed)
	}
	switch value.GetKind() {
	case TargetKind_TARGET_KIND_NONE:
		return sdk.TargetRequirement{Kind: sdk.TargetNone}, nil
	case TargetKind_TARGET_KIND_ORGANIZATION:
		return sdk.TargetRequirement{Kind: sdk.TargetOrganization}, nil
	case TargetKind_TARGET_KIND_STACK:
		return sdk.TargetRequirement{Kind: sdk.TargetStack}, nil
	case TargetKind_TARGET_KIND_APPLICATION:
		return sdk.TargetRequirement{Kind: sdk.TargetApplication}, nil
	default:
		return sdk.TargetRequirement{}, fmt.Errorf("%w: unsupported target kind %d", ErrProtocol, value.GetKind())
	}
}

func targetFromSDK(value sdk.TargetRequirement) (*TargetRequirement, error) {
	var kind TargetKind
	switch value.Kind {
	case sdk.TargetNone:
		kind = TargetKind_TARGET_KIND_NONE
	case sdk.TargetOrganization:
		kind = TargetKind_TARGET_KIND_ORGANIZATION
	case sdk.TargetStack:
		kind = TargetKind_TARGET_KIND_STACK
	case sdk.TargetApplication:
		kind = TargetKind_TARGET_KIND_APPLICATION
	default:
		return nil, fmt.Errorf("%w: unsupported target kind %q", ErrProtocol, value.Kind)
	}
	return &TargetRequirement{Kind: kind}, nil
}

func executionKindToSDK(value ExecutionKind) (sdk.ExecutionKind, error) {
	switch value {
	case ExecutionKind_EXECUTION_KIND_UNSPECIFIED:
		return "", nil
	case ExecutionKind_EXECUTION_KIND_SERVICE:
		return sdk.ExecutionKindService, nil
	case ExecutionKind_EXECUTION_KIND_LOCAL:
		return sdk.ExecutionKindLocal, nil
	default:
		return "", fmt.Errorf("%w: unsupported execution kind %d", ErrProtocol, value)
	}
}

func executionKindFromSDK(value sdk.ExecutionKind) (ExecutionKind, error) {
	switch value {
	case "":
		return ExecutionKind_EXECUTION_KIND_UNSPECIFIED, nil
	case sdk.ExecutionKindService:
		return ExecutionKind_EXECUTION_KIND_SERVICE, nil
	case sdk.ExecutionKindLocal:
		return ExecutionKind_EXECUTION_KIND_LOCAL, nil
	default:
		return ExecutionKind_EXECUTION_KIND_UNSPECIFIED, fmt.Errorf("%w: unsupported execution kind %q", ErrProtocol, value)
	}
}

func authModeToSDK(value AuthMode) (sdk.AuthMode, error) {
	switch value {
	case AuthMode_AUTH_MODE_NONE:
		return sdk.AuthModeNone, nil
	case AuthMode_AUTH_MODE_CAPABILITY:
		return sdk.AuthModeCapability, nil
	default:
		return "", fmt.Errorf("%w: unsupported auth mode %d", ErrProtocol, value)
	}
}

func authModeFromSDK(value sdk.AuthMode) (AuthMode, error) {
	switch value {
	case sdk.AuthModeNone:
		return AuthMode_AUTH_MODE_NONE, nil
	case sdk.AuthModeCapability:
		return AuthMode_AUTH_MODE_CAPABILITY, nil
	default:
		return AuthMode_AUTH_MODE_UNSPECIFIED, fmt.Errorf("%w: unsupported auth mode %q", ErrProtocol, value)
	}
}

func riskToSDK(value Risk) (sdk.Risk, error) {
	switch value {
	case Risk_RISK_UNSPECIFIED:
		return "", nil
	case Risk_RISK_READ:
		return sdk.RiskRead, nil
	case Risk_RISK_MUTATION:
		return sdk.RiskMutation, nil
	default:
		return "", fmt.Errorf("%w: unsupported risk %d", ErrProtocol, value)
	}
}

func riskFromSDK(value sdk.Risk) (Risk, error) {
	switch value {
	case "":
		return Risk_RISK_UNSPECIFIED, nil
	case sdk.RiskRead:
		return Risk_RISK_READ, nil
	case sdk.RiskMutation:
		return Risk_RISK_MUTATION, nil
	default:
		return Risk_RISK_UNSPECIFIED, fmt.Errorf("%w: unsupported risk %q", ErrProtocol, value)
	}
}

func argumentTypeToSDK(value ArgumentType) (sdk.ArgumentType, error) {
	switch value {
	case ArgumentType_ARGUMENT_TYPE_STRING:
		return sdk.ArgumentString, nil
	case ArgumentType_ARGUMENT_TYPE_STRING_ARRAY:
		return sdk.ArgumentStringArray, nil
	case ArgumentType_ARGUMENT_TYPE_INT32:
		return sdk.ArgumentInt32, nil
	case ArgumentType_ARGUMENT_TYPE_BOOL:
		return sdk.ArgumentBool, nil
	default:
		return "", fmt.Errorf("%w: unsupported argument type %d", ErrProtocol, value)
	}
}

func argumentTypeFromSDK(value sdk.ArgumentType) (ArgumentType, error) {
	switch value {
	case sdk.ArgumentString:
		return ArgumentType_ARGUMENT_TYPE_STRING, nil
	case sdk.ArgumentStringArray:
		return ArgumentType_ARGUMENT_TYPE_STRING_ARRAY, nil
	case sdk.ArgumentInt32:
		return ArgumentType_ARGUMENT_TYPE_INT32, nil
	case sdk.ArgumentBool:
		return ArgumentType_ARGUMENT_TYPE_BOOL, nil
	default:
		return ArgumentType_ARGUMENT_TYPE_UNSPECIFIED, fmt.Errorf("%w: unsupported argument type %q", ErrProtocol, value)
	}
}

func flagTypeToSDK(value FlagType) (sdk.FlagType, error) {
	switch value {
	case FlagType_FLAG_TYPE_STRING:
		return sdk.FlagString, nil
	case FlagType_FLAG_TYPE_STRING_ARRAY:
		return sdk.FlagStringArray, nil
	case FlagType_FLAG_TYPE_INT32:
		return sdk.FlagInt32, nil
	case FlagType_FLAG_TYPE_BOOL:
		return sdk.FlagBool, nil
	default:
		return "", fmt.Errorf("%w: unsupported flag type %d", ErrProtocol, value)
	}
}

func flagTypeFromSDK(value sdk.FlagType) (FlagType, error) {
	switch value {
	case sdk.FlagString:
		return FlagType_FLAG_TYPE_STRING, nil
	case sdk.FlagStringArray:
		return FlagType_FLAG_TYPE_STRING_ARRAY, nil
	case sdk.FlagInt32:
		return FlagType_FLAG_TYPE_INT32, nil
	case sdk.FlagBool:
		return FlagType_FLAG_TYPE_BOOL, nil
	default:
		return FlagType_FLAG_TYPE_UNSPECIFIED, fmt.Errorf("%w: unsupported flag type %q", ErrProtocol, value)
	}
}

func sensitiveDeliveryToSDK(value SensitiveDelivery) (sdk.SensitiveDelivery, error) {
	switch value {
	case SensitiveDelivery_SENSITIVE_DELIVERY_DISPLAY_ONCE:
		return sdk.SensitiveDisplayOnce, nil
	case SensitiveDelivery_SENSITIVE_DELIVERY_PROFILE_AUTH:
		return sdk.SensitiveProfileAuth, nil
	default:
		return "", fmt.Errorf("%w: unsupported sensitive delivery %d", ErrProtocol, value)
	}
}

func sensitiveDeliveryFromSDK(value sdk.SensitiveDelivery) (SensitiveDelivery, error) {
	switch value {
	case sdk.SensitiveDisplayOnce:
		return SensitiveDelivery_SENSITIVE_DELIVERY_DISPLAY_ONCE, nil
	case sdk.SensitiveProfileAuth:
		return SensitiveDelivery_SENSITIVE_DELIVERY_PROFILE_AUTH, nil
	default:
		return SensitiveDelivery_SENSITIVE_DELIVERY_UNSPECIFIED, fmt.Errorf("%w: unsupported sensitive delivery %q", ErrProtocol, value)
	}
}

func serviceToSDK(value Service) (sdk.Service, error) {
	services := [...]sdk.Service{"", sdk.ServiceMembership, sdk.ServiceConnectivity, sdk.ServiceLedger, sdk.ServiceAuth, sdk.ServicePayments, sdk.ServiceWallets, sdk.ServiceReconciliation, sdk.ServiceFlows, sdk.ServiceTransactionPlane, sdk.ServiceNumscript, sdk.ServiceStudioApps, sdk.ServiceBankingBridge}
	if value <= Service_SERVICE_UNSPECIFIED || int(value) >= len(services) {
		return "", fmt.Errorf("%w: unsupported service %d", ErrProtocol, value)
	}
	return services[value], nil
}

func serviceFromSDK(value sdk.Service) (Service, error) {
	services := [...]sdk.Service{"", sdk.ServiceMembership, sdk.ServiceConnectivity, sdk.ServiceLedger, sdk.ServiceAuth, sdk.ServicePayments, sdk.ServiceWallets, sdk.ServiceReconciliation, sdk.ServiceFlows, sdk.ServiceTransactionPlane, sdk.ServiceNumscript, sdk.ServiceStudioApps, sdk.ServiceBankingBridge}
	for index := 1; index < len(services); index++ {
		if value == services[index] {
			return Service(index), nil
		}
	}
	return Service_SERVICE_UNSPECIFIED, fmt.Errorf("%w: unsupported service %q", ErrProtocol, value)
}

func eventKindToSDK(value EventKind) (sdk.EventKind, error) {
	switch value {
	case EventKind_EVENT_KIND_RESULT:
		return sdk.EventResult, nil
	case EventKind_EVENT_KIND_PROGRESS:
		return sdk.EventProgress, nil
	case EventKind_EVENT_KIND_DIAGNOSTIC:
		return sdk.EventDiagnostic, nil
	default:
		return "", fmt.Errorf("%w: unsupported event kind %d", ErrProtocol, value)
	}
}

func eventKindFromSDK(value sdk.EventKind) (EventKind, error) {
	switch value {
	case sdk.EventResult:
		return EventKind_EVENT_KIND_RESULT, nil
	case sdk.EventProgress:
		return EventKind_EVENT_KIND_PROGRESS, nil
	case sdk.EventDiagnostic:
		return EventKind_EVENT_KIND_DIAGNOSTIC, nil
	default:
		return EventKind_EVENT_KIND_UNSPECIFIED, fmt.Errorf("%w: unsupported event kind %q", ErrProtocol, value)
	}
}

func resultShapeToSDK(value ResultShape) (sdk.ResultShape, error) {
	switch value {
	case ResultShape_RESULT_SHAPE_OBJECT:
		return sdk.ResultObject, nil
	case ResultShape_RESULT_SHAPE_COLLECTION:
		return sdk.ResultCollection, nil
	case ResultShape_RESULT_SHAPE_EMPTY:
		return sdk.ResultEmpty, nil
	default:
		return "", fmt.Errorf("%w: unsupported result shape %d", ErrProtocol, value)
	}
}

func resultShapeFromSDK(value sdk.ResultShape) (ResultShape, error) {
	switch value {
	case sdk.ResultObject:
		return ResultShape_RESULT_SHAPE_OBJECT, nil
	case sdk.ResultCollection:
		return ResultShape_RESULT_SHAPE_COLLECTION, nil
	case sdk.ResultEmpty:
		return ResultShape_RESULT_SHAPE_EMPTY, nil
	default:
		return ResultShape_RESULT_SHAPE_UNSPECIFIED, fmt.Errorf("%w: unsupported result shape %q", ErrProtocol, value)
	}
}

func logLevelToSDK(value LogLevel) (sdk.Level, error) {
	switch value {
	case LogLevel_LOG_LEVEL_DEBUG:
		return sdk.LevelDebug, nil
	case LogLevel_LOG_LEVEL_INFO:
		return sdk.LevelInfo, nil
	case LogLevel_LOG_LEVEL_WARN:
		return sdk.LevelWarn, nil
	case LogLevel_LOG_LEVEL_ERROR:
		return sdk.LevelError, nil
	default:
		return "", fmt.Errorf("%w: unsupported log level %d", ErrProtocol, value)
	}
}

func logLevelFromSDK(value sdk.Level) (LogLevel, error) {
	switch value {
	case sdk.LevelDebug:
		return LogLevel_LOG_LEVEL_DEBUG, nil
	case sdk.LevelInfo:
		return LogLevel_LOG_LEVEL_INFO, nil
	case sdk.LevelWarn:
		return LogLevel_LOG_LEVEL_WARN, nil
	case sdk.LevelError:
		return LogLevel_LOG_LEVEL_ERROR, nil
	default:
		return LogLevel_LOG_LEVEL_UNSPECIFIED, fmt.Errorf("%w: unsupported log level %q", ErrProtocol, value)
	}
}

func cloneStrings(value []string) []string {
	return append([]string(nil), value...)
}

func cloneUint32s(value []uint32) []uint32 {
	return append([]uint32(nil), value...)
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
