// Package component adapts the blocking public Go plugin SDK to the portable
// component lifecycle exported by the generated WIT bindings.
package component

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"unicode/utf8"

	pb "github.com/formancehq/fctl-v2-poc/pkg/plugin/protocol/componentbridgev1alpha1"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/portable"
	"google.golang.org/protobuf/proto"
)

const maxExecutionIDBytes = 128

var ErrInvalidConfiguration = errors.New("portable component is not configured")

// Component is one reconstructible instance serving its declared facets. Its
// methods have the same values as the lifecycle WIT exports, so generated
// binding implementations can delegate to it without teaching product code a
// state machine.
type Component struct {
	providers          Providers
	descriptor         []byte
	commands           map[string]map[string]bool
	authCapabilities   []string
	targetCapabilities []string
	targetOperations   map[string]bool
	facet              pb.FacetKind

	mu        sync.Mutex
	execution *execution
}

type execution struct {
	id         string
	pump       *portable.Pump
	operations map[string]bool
	pending    map[string]pendingOperation
	terminal   bool
}

type pendingOperation struct {
	kind      portable.FrameKind
	streaming bool
	auth      portable.AuthOperation
}

// Command remains source-compatible with existing command-only bindings.
type Command = Component

// Providers are implementations behind one atomic descriptor. Each privileged
// interface has its own Metadata method and is supplied independently.
type Providers struct {
	Command sdk.Plugin
	Auth    sdk.AuthProvider
	Target  sdk.TargetProvider
}

// NewCommand validates and snapshots the descriptor used by describe(). A
// bool in the operation map records whether that operation is server-streaming.
func NewCommand(plugin sdk.Plugin, descriptor pb.Descriptor) (*Command, error) {
	if plugin == nil || len(descriptor.AuthProviders) != 0 || len(descriptor.TargetProviders) != 0 {
		return nil, ErrInvalidConfiguration
	}
	return New(Providers{Command: plugin}, descriptor)
}

// New validates provider identities and snapshots the complete descriptor.
func New(providers Providers, descriptor pb.Descriptor) (*Component, error) {
	if providers.Command == nil && providers.Auth == nil && providers.Target == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := sdk.ValidateFacets(descriptor.Metadata.Facets); err != nil {
		return nil, ErrInvalidConfiguration
	}
	if len(descriptor.SignerProviders) != 0 {
		return nil, ErrInvalidConfiguration
	}
	c := &Component{providers: providers, facet: pb.FacetKind_FACET_KIND_COMMAND_PROVIDER}
	commandFacet := false
	for _, facet := range descriptor.Metadata.Facets {
		switch facet.Kind {
		case sdk.FacetCommandProvider:
			commandFacet = true
			if providers.Command == nil {
				return nil, ErrInvalidConfiguration
			}
		case sdk.FacetAuthProvider:
			c.authCapabilities = append([]string(nil), facet.Capabilities...)
		case sdk.FacetTargetProvider:
			c.targetCapabilities = append([]string(nil), facet.Capabilities...)
		default:
			return nil, ErrInvalidConfiguration
		}
	}
	if (providers.Command != nil) != commandFacet {
		return nil, ErrInvalidConfiguration
	}
	if providers.Command != nil {
		declared, err := pb.EncodeDescriptorEnvelope(pb.Descriptor{Metadata: providers.Command.Metadata(), Commands: providers.Command.Commands()})
		if err != nil {
			return nil, ErrInvalidConfiguration
		}
		expected, err := pb.EncodeDescriptorEnvelope(pb.Descriptor{Metadata: descriptor.Metadata, Commands: descriptor.Commands})
		if err != nil || !bytes.Equal(declared, expected) {
			return nil, ErrInvalidConfiguration
		}
	}
	if (providers.Command == nil && len(descriptor.Commands) != 0) || (providers.Auth == nil) != (len(descriptor.AuthProviders) == 0) || (providers.Target == nil) != (len(descriptor.TargetProviders) == 0) {
		return nil, ErrInvalidConfiguration
	}
	if providers.Auth != nil {
		if len(descriptor.AuthProviders) != 1 {
			return nil, ErrInvalidConfiguration
		}
		p := descriptor.AuthProviders[0]
		if p.Metadata != providers.Auth.Metadata() || p.Metadata.Name != descriptor.Metadata.Name || p.Metadata.Version != descriptor.Metadata.Version || p.ProtocolVersion != sdk.CurrentAuthProviderFacetProtocolVersion || !sameSet(p.Capabilities, c.authCapabilities) || !sameSet(p.Capabilities, providers.Auth.Provides()) {
			return nil, ErrInvalidConfiguration
		}
	} else if len(c.authCapabilities) != 0 {
		return nil, ErrInvalidConfiguration
	}
	if providers.Target != nil {
		if len(descriptor.TargetProviders) != 1 {
			return nil, ErrInvalidConfiguration
		}
		p := descriptor.TargetProviders[0]
		if !reflect.DeepEqual(p, providers.Target.Metadata()) || p.Name != descriptor.Metadata.Name || p.Version != descriptor.Metadata.Version || !sameSet(p.Capabilities, c.targetCapabilities) || sdk.ValidateTargetProviderMetadata(p) != nil {
			return nil, ErrInvalidConfiguration
		}
		c.targetOperations = make(map[string]bool, len(p.Operations))
		for _, operation := range p.Operations {
			c.targetOperations[operation.Operation.ID] = operation.Operation.GRPC != nil && operation.Operation.GRPC.ServerStreaming
		}
	} else if len(c.targetCapabilities) != 0 {
		return nil, ErrInvalidConfiguration
	}
	described, err := pb.EncodeDescriptorEnvelope(descriptor)
	if err != nil {
		return nil, fmt.Errorf("encode portable descriptor: %w", err)
	}
	commands := make(map[string]map[string]bool, len(descriptor.Commands))
	for _, command := range descriptor.Commands {
		if _, duplicate := commands[command.ID]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		if command.ID == "" {
			return nil, ErrInvalidConfiguration
		}
		operations := make(map[string]bool, len(command.Operations))
		for _, operation := range command.Operations {
			if _, duplicate := operations[operation.ID]; duplicate {
				return nil, ErrInvalidConfiguration
			}
			if operation.ID == "" {
				return nil, ErrInvalidConfiguration
			}
			operations[operation.ID] = operation.GRPC != nil && operation.GRPC.ServerStreaming
		}
		commands[command.ID] = operations
	}
	if len(described) > 1<<20 {
		return nil, ErrInvalidConfiguration
	}
	c.descriptor = described
	c.commands = commands
	return c, nil
}

func sameSet(a, b []string) bool {
	if len(a) == 0 || len(a) != len(b) {
		return false
	}
	a = slices.Clone(a)
	b = slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	for i := range a {
		if a[i] == "" || (i > 0 && a[i] == a[i-1]) || a[i] != b[i] {
			return false
		}
	}
	return true
}

// Describe returns an owned copy of the deterministic descriptor envelope.
func (c *Component) Describe() []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.descriptor...)
}

// Start enters the blocking Plugin.Execute implementation and returns when it
// first yields lifecycle frames or terminates.
func (c *Component) Start(executionID string, input []byte) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.execution != nil || !validExecutionID(executionID) {
		return c.protocolFailureLocked(executionID)
	}
	envelope, err := decodeFacetEnvelope(executionID, input)
	if err != nil {
		return c.protocolFailureLocked(executionID)
	}
	c.facet = envelope.GetFacetKind()
	var start pb.StartPayload
	if envelope.GetMessageKind() != pb.MessageKind_MESSAGE_KIND_START_EXECUTION || unmarshal(envelope.GetPayload(), &start) != nil {
		return c.protocolFailureLocked(executionID)
	}
	var pump *portable.Pump
	var frames []portable.Frame
	var operations map[string]bool
	switch c.facet {
	case pb.FacetKind_FACET_KIND_COMMAND_PROVIDER:
		request, e := pb.CommandStartToSDK(start.GetCommand())
		if e != nil || c.providers.Command == nil {
			return c.protocolFailureLocked(executionID)
		}
		var declared bool
		operations, declared = c.commands[request.CommandID]
		if !declared {
			return c.protocolFailureLocked(executionID)
		}
		pump, frames, err = portable.Start(context.Background(), c.providers.Command, request, &correlations{})
	case pb.FacetKind_FACET_KIND_AUTH_PROVIDER:
		request := start.GetAuth()
		if request == nil || c.providers.Auth == nil || !slices.Contains(c.authCapabilities, request.GetCapability()) || request.GetService() == "" || len(request.GetOperations()) == 0 {
			return c.protocolFailureLocked(executionID)
		}
		authRequest := sdk.AuthRequest{Capability: request.GetCapability(), Service: sdk.Service(request.GetService()), Target: sdk.TargetCoordinates{OrganizationID: request.GetTarget().GetOrganizationId(), StackID: request.GetTarget().GetStackId()}, Operations: append([]string(nil), request.GetOperations()...)}
		if request.GetCredentialSlot() != nil {
			slot := authSlotToSDK(request.GetCredentialSlot())
			authRequest.CredentialSlot = &slot
		}
		if sdk.ValidateAuthRequest(authRequest) != nil {
			return c.protocolFailureLocked(executionID)
		}
		pump, frames, err = portable.StartAuth(context.Background(), c.providers.Auth, authRequest, &correlations{})
	case pb.FacetKind_FACET_KIND_TARGET_PROVIDER:
		request, e := pb.TargetStartToSDK(start.GetTarget())
		if e != nil || c.providers.Target == nil || !slices.Contains(c.targetCapabilities, request.Capability) {
			return c.protocolFailureLocked(executionID)
		}
		operations = c.targetOperations
		pump, frames, err = portable.StartTarget(context.Background(), c.providers.Target, request, &correlations{})
	default:
		return c.protocolFailureLocked(executionID)
	}
	if err != nil {
		return c.protocolFailureLocked(executionID)
	}
	c.execution = &execution{id: executionID, pump: pump, operations: operations, pending: make(map[string]pendingOperation)}
	encoded, err := c.encodeFramesLocked(frames, operations)
	if err != nil {
		return c.encodingFailureLocked(executionID, err)
	}
	return encoded
}

// Resume delivers one host response to the suspended Plugin.Execute call.
func (c *Component) Resume(executionID string, input []byte) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.execution == nil || c.execution.terminal {
		return nil
	}
	if executionID != c.execution.id {
		return c.protocolFailureLocked(executionID)
	}
	response, err := c.decodeResponseLocked(executionID, input)
	if err != nil {
		return c.protocolFailureLocked(executionID)
	}
	frames, err := c.execution.pump.Resume(context.Background(), response)
	if err != nil {
		return c.protocolFailureLocked(executionID)
	}
	encoded, err := c.encodeFramesLocked(frames, c.execution.operations)
	if err != nil {
		return c.encodingFailureLocked(executionID, err)
	}
	return encoded
}

// Cancel terminates the active author execution and returns its terminal frame.
func (c *Component) Cancel(executionID string) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.execution == nil || c.execution.terminal {
		return nil
	}
	if executionID != c.execution.id {
		return c.protocolFailureLocked(executionID)
	}
	frames, err := c.execution.pump.Cancel(context.Background())
	if err != nil {
		return c.protocolFailureLocked(executionID)
	}
	encoded, err := c.encodeFramesLocked(frames, c.execution.operations)
	if err != nil {
		return c.encodingFailureLocked(executionID, err)
	}
	return encoded
}

// Close releases any suspended goroutine and resets this instance. Production
// hosts still create a fresh component instance for every execution.
func (c *Component) Close(executionID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.execution == nil || c.execution.id != executionID {
		return
	}
	c.execution.pump.Close()
	c.execution = nil
}

func (c *Component) encodeFramesLocked(frames []portable.Frame, operations map[string]bool) ([][]byte, error) {
	if len(frames) > 4 {
		return nil, sdk.Failure{Code: string(sdk.FailureBudgetExhausted)}
	}
	encoded := make([][]byte, 0, len(frames))
	total := 0
	for _, frame := range frames {
		var kind pb.MessageKind
		var payload proto.Message
		switch frame.Kind {
		case portable.FrameHostRequest:
			if c.facet == pb.FacetKind_FACET_KIND_AUTH_PROVIDER {
				return nil, pb.ErrProtocol
			}
			streaming, declared := operations[frame.Request.Operation]
			if !declared || frame.CorrelationID == "" {
				return nil, errors.New("undeclared portable host request")
			}
			request, err := pb.ProductRequestFromSDK(frame.Request)
			if err != nil {
				return nil, err
			}
			c.execution.pending[frame.CorrelationID] = pendingOperation{kind: portable.FrameHostRequest, streaming: streaming}
			kind = pb.MessageKind_MESSAGE_KIND_HOST_REQUEST
			payload = &pb.HostRequestPayload{CorrelationId: frame.CorrelationID, Request: &pb.HostRequestPayload_Product{Product: request}}
		case portable.FrameWaitRequest:
			if c.facet != pb.FacetKind_FACET_KIND_COMMAND_PROVIDER {
				return nil, pb.ErrProtocol
			}
			if frame.CorrelationID == "" {
				return nil, errors.New("portable wait correlation is missing")
			}
			c.execution.pending[frame.CorrelationID] = pendingOperation{kind: portable.FrameWaitRequest}
			kind = pb.MessageKind_MESSAGE_KIND_WAIT_REQUEST
			payload = &pb.WaitRequestPayload{CorrelationId: frame.CorrelationID, DelayMilliseconds: frame.Wait.DelayMilliseconds}
		case portable.FrameInputRead:
			if c.facet != pb.FacetKind_FACET_KIND_COMMAND_PROVIDER || frame.CorrelationID == "" || frame.InputHandle == "" {
				return nil, pb.ErrProtocol
			}
			c.execution.pending[frame.CorrelationID] = pendingOperation{kind: portable.FrameInputRead}
			kind = pb.MessageKind_MESSAGE_KIND_HOST_REQUEST
			payload = &pb.HostRequestPayload{CorrelationId: frame.CorrelationID, Request: &pb.HostRequestPayload_InputArtifact{InputArtifact: &pb.InputArtifactReadRequest{OpaqueHandle: frame.InputHandle}}}
		case portable.FrameEvent:
			if c.facet != pb.FacetKind_FACET_KIND_COMMAND_PROVIDER {
				return nil, pb.ErrProtocol
			}
			event, err := pb.CommandEventFromSDK(frame.Event)
			if err != nil {
				return nil, err
			}
			kind = pb.MessageKind_MESSAGE_KIND_EVENT
			payload = &pb.EventPayload{Event: &pb.EventPayload_Command{Command: event}}
		case portable.FrameTargetResult:
			if c.facet != pb.FacetKind_FACET_KIND_TARGET_PROVIDER {
				return nil, pb.ErrProtocol
			}
			result, err := pb.TargetDiscoveryResultFromSDK(frame.Target)
			if err != nil {
				return nil, err
			}
			kind = pb.MessageKind_MESSAGE_KIND_EVENT
			payload = &pb.EventPayload{Event: &pb.EventPayload_TargetDiscovery{TargetDiscovery: result}}
		case portable.FrameAuthRequest:
			if c.facet != pb.FacetKind_FACET_KIND_AUTH_PROVIDER {
				return nil, pb.ErrProtocol
			}
			request, err := encodeAuthRequest(frame.Auth)
			if err != nil {
				return nil, err
			}
			c.execution.pending[frame.CorrelationID] = pendingOperation{kind: portable.FrameAuthRequest, auth: frame.Auth.Operation}
			kind = pb.MessageKind_MESSAGE_KIND_HOST_REQUEST
			payload = &pb.HostRequestPayload{CorrelationId: frame.CorrelationID, Request: &pb.HostRequestPayload_AuthBroker{AuthBroker: request}}
		case portable.FrameLog:
			log, err := pb.LogPayloadFromSDK(sdk.LogEntry{Level: frame.Level, Message: frame.Message})
			if err != nil {
				return nil, err
			}
			kind = pb.MessageKind_MESSAGE_KIND_LOG
			payload = log
		case portable.FrameTerminal:
			c.execution.terminal = true
			kind = pb.MessageKind_MESSAGE_KIND_TERMINATION
			if frame.FailureCode == "" {
				payload = &pb.TerminationPayload{Outcome: &pb.TerminationPayload_Success{Success: true}}
			} else {
				payload = &pb.TerminationPayload{Outcome: &pb.TerminationPayload_Failure{Failure: pb.FailureFromSDK(sdk.Failure{Code: string(frame.FailureCode)})}}
			}
		default:
			return nil, errors.New("unsupported portable command frame")
		}
		wire, err := encodeFacetEnvelope(c.facet, c.execution.id, kind, payload)
		if err != nil {
			return nil, err
		}
		total += len(wire)
		if len(wire) > 6966419 || total > 18<<20 {
			return nil, sdk.Failure{Code: string(sdk.FailureBudgetExhausted)}
		}
		encoded = append(encoded, wire)
	}
	return encoded, nil
}

func (c *Component) decodeResponseLocked(executionID string, encoded []byte) (portable.Response, error) {
	envelope, err := decodeFacetEnvelope(executionID, encoded)
	if err == nil && envelope.GetFacetKind() != c.facet {
		err = pb.ErrProtocol
	}
	if err != nil {
		return portable.Response{}, err
	}
	if envelope.GetMessageKind() == pb.MessageKind_MESSAGE_KIND_WAIT_COMPLETION {
		var completion pb.WaitCompletionPayload
		if err := unmarshal(envelope.GetPayload(), &completion); err != nil {
			return portable.Response{}, err
		}
		pending, found := c.execution.pending[completion.GetCorrelationId()]
		if !found || pending.kind != portable.FrameWaitRequest {
			return portable.Response{}, errors.New("portable wait correlation is not pending")
		}
		delete(c.execution.pending, completion.GetCorrelationId())
		return portable.Response{CorrelationID: completion.GetCorrelationId(), Complete: true}, nil
	}
	if envelope.GetMessageKind() != pb.MessageKind_MESSAGE_KIND_HOST_RESPONSE {
		return portable.Response{}, errors.New("portable command resume kind is invalid")
	}
	var response pb.HostResponsePayload
	if err := unmarshal(envelope.GetPayload(), &response); err != nil {
		return portable.Response{}, err
	}
	pending, found := c.execution.pending[response.GetCorrelationId()]
	if !found || (pending.kind != portable.FrameHostRequest && pending.kind != portable.FrameAuthRequest && pending.kind != portable.FrameInputRead) {
		return portable.Response{}, errors.New("portable response correlation is not pending")
	}
	mapped := portable.Response{CorrelationID: response.GetCorrelationId()}
	switch value := response.GetResponse().(type) {
	case *pb.HostResponsePayload_AuthBroker:
		if pending.kind != portable.FrameAuthRequest {
			return portable.Response{}, pb.ErrProtocol
		}
		mapped.Auth, err = decodeAuthResponse(pending.auth, value.AuthBroker)
		if err != nil {
			return portable.Response{}, err
		}
		mapped.Complete = true
		delete(c.execution.pending, mapped.CorrelationID)
	case *pb.HostResponsePayload_Product:
		if value.Product == nil || pending.kind != portable.FrameHostRequest {
			return portable.Response{}, errors.New("portable product response is missing")
		}
		mapped.Response = pb.ProductResponseToSDK(value.Product)
		mapped.Complete = !pending.streaming
		if !pending.streaming {
			delete(c.execution.pending, mapped.CorrelationID)
		}
	case *pb.HostResponsePayload_InputArtifact:
		if value.InputArtifact == nil || pending.kind != portable.FrameInputRead {
			return portable.Response{}, pb.ErrProtocol
		}
		mapped.Input = sdk.InputArtifactChunk{Bytes: append([]byte(nil), value.InputArtifact.GetChunk()...), Final: value.InputArtifact.GetFinal()}
		mapped.Complete = true
		delete(c.execution.pending, mapped.CorrelationID)
	case *pb.HostResponsePayload_StreamTerminal:
		if !pending.streaming || value.StreamTerminal == nil {
			return portable.Response{}, errors.New("portable stream terminal is invalid")
		}
		mapped.Complete = true
		mapped.StreamTerminal = &sdk.ResponseStreamMetadata{Continuation: value.StreamTerminal.GetContinuation()}
		delete(c.execution.pending, mapped.CorrelationID)
	case *pb.HostResponsePayload_Failure:
		if value.Failure == nil {
			return portable.Response{}, errors.New("portable failure response is missing")
		}
		failure := pb.FailureToSDK(value.Failure)
		mapped.Err = failure
		mapped.Complete = true
		delete(c.execution.pending, mapped.CorrelationID)
	default:
		return portable.Response{}, errors.New("portable command response has the wrong variant")
	}
	return mapped, nil
}

func (c *Component) protocolFailureLocked(executionID string) [][]byte {
	return c.failureLocked(executionID, sdk.FailureProtocolError)
}

func (c *Component) failureLocked(executionID string, code sdk.FailureCode) [][]byte {
	if c != nil && c.execution != nil {
		c.execution.pump.Close()
		c.execution = nil
	}
	if !validExecutionID(executionID) {
		return nil
	}
	encoded, err := encodeFacetEnvelope(c.facet, executionID, pb.MessageKind_MESSAGE_KIND_TERMINATION, &pb.TerminationPayload{
		Outcome: &pb.TerminationPayload_Failure{Failure: pb.FailureFromSDK(sdk.Failure{Code: string(code)})},
	})
	if err != nil {
		return nil
	}
	return [][]byte{encoded}
}

func (c *Component) encodingFailureLocked(executionID string, err error) [][]byte {
	var failure sdk.Failure
	if errors.As(err, &failure) && failure.Code == string(sdk.FailureBudgetExhausted) {
		return c.failureLocked(executionID, sdk.FailureBudgetExhausted)
	}
	return c.protocolFailureLocked(executionID)
}

func decodeFacetEnvelope(executionID string, encoded []byte) (*pb.PluginEnvelope, error) {
	if !validExecutionID(executionID) {
		return nil, errors.New("portable execution identifier is invalid")
	}
	var envelope pb.PluginEnvelope
	if err := unmarshal(encoded, &envelope); err != nil {
		return nil, err
	}
	if envelope.GetProtocolMajor() != pb.ProtocolMajor || envelope.GetExecutionId() != executionID {
		return nil, errors.New("portable command envelope identity is invalid")
	}
	return &envelope, nil
}

func encodeFacetEnvelope(facet pb.FacetKind, executionID string, kind pb.MessageKind, payload proto.Message) ([]byte, error) {
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(&pb.PluginEnvelope{
		ProtocolMajor: pb.ProtocolMajor,
		FacetKind:     facet,
		MessageKind:   kind,
		ExecutionId:   executionID,
		Payload:       payloadBytes,
	})
}

func unmarshal(encoded []byte, message proto.Message) error {
	return pb.UnmarshalStrict(encoded, message)
}

func validExecutionID(value string) bool {
	return value != "" && len(value) <= maxExecutionIDBytes && utf8.ValidString(value)
}

type correlations struct{ next uint64 }

func (c *correlations) NextCorrelationID() (string, error) {
	c.next++
	return fmt.Sprintf("request-%d", c.next), nil
}
