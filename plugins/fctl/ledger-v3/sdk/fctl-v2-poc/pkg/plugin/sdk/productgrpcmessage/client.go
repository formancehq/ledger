// Package productgrpcmessage adapts generated protobuf messages to one
// descriptor-bound host operation without a grpc-go client or transport.
package productgrpcmessage

import (
	"context"
	"io"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Client holds one immutable operation binding. It may be reused concurrently;
// each invocation makes its own single Host.Request.
type Client struct {
	host       sdk.Host
	policy     sdk.OperationPolicy
	capability string
	method     protoreflect.MethodDescriptor
}

// NewClient validates the generated-client policy against the authoritative
// protobuf method descriptor. Client or bidirectional streaming is unsupported.
func NewClient(host sdk.Host, policy sdk.OperationPolicy, capability string, method protoreflect.MethodDescriptor) (*Client, error) {
	if host == nil || method == nil || method.IsStreamingClient() || capability == sdk.HostCapabilityGeneratedClientV1 || policy.GRPC == nil || policy.GRPC.GeneratedClient == nil || sdk.ValidateGeneratedClientOperationPolicy(policy) != nil {
		return nil, failure(sdk.FailureDescriptorInvalid, "invalid generated message binding")
	}
	service, ok := method.Parent().(protoreflect.ServiceDescriptor)
	if !ok || policy.GRPC.FullMethod != "/"+string(service.FullName())+"/"+string(method.Name()) || policy.GRPC.ServerStreaming != method.IsStreamingServer() {
		return nil, failure(sdk.FailureDescriptorInvalid, "invalid generated message binding")
	}
	grpcPolicy := *policy.GRPC
	generated := *grpcPolicy.GeneratedClient
	grpcPolicy.GeneratedClient = &generated
	policy.GRPC = &grpcPolicy
	policy.Scopes = append([]string(nil), policy.Scopes...)
	return &Client{host: host, policy: policy, capability: capability, method: method}, nil
}

// Invoke decodes one unary response only after the host stream ends cleanly.
// On failure response is unchanged; callers retain ownership of both messages.
func (c *Client) Invoke(ctx context.Context, request, response proto.Message) error {
	if c.policy.GRPC.ServerStreaming || !matches(response, c.method.Output()) {
		return failure(sdk.FailureInvalidArgument, "invalid generated message invocation")
	}
	responses, err := c.dispatch(ctx, request)
	if err != nil {
		return err
	}
	item, err := recv(ctx, responses, c.policy.GRPC.GeneratedClient.ResponseLimits, 0, 0)
	if err != nil {
		if err == io.EOF {
			return invalidResponse()
		}
		return err
	}
	if _, err := recv(ctx, responses, c.policy.GRPC.GeneratedClient.ResponseLimits, 1, int64(len(item.Body))); err != io.EOF {
		if err != nil {
			return err
		}
		return invalidResponse()
	}
	if sdk.ResponseStreamMetadataOf(responses) != (sdk.ResponseStreamMetadata{}) {
		return invalidResponse()
	}
	return decode(ctx, item.Body, response)
}

func (c *Client) dispatch(ctx context.Context, request proto.Message) (sdk.Responses, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !matches(request, c.method.Input()) {
		return nil, failure(sdk.FailureInvalidArgument, "invalid generated message request")
	}
	if int64(proto.Size(request)) > c.policy.GRPC.GeneratedClient.MaxRequestMessageBytes {
		return nil, failure(sdk.FailureInvalidArgument, "invalid generated message request")
	}
	encoded, err := proto.Marshal(request)
	if err != nil || int64(len(encoded)) > c.policy.GRPC.GeneratedClient.MaxRequestMessageBytes {
		return nil, failure(sdk.FailureInvalidArgument, "invalid generated message request")
	}
	responses, err := c.host.Request(ctx, sdk.Request{Service: c.policy.Service, Operation: c.policy.ID, Capability: c.capability, GRPC: &sdk.GRPCRequest{FullMethod: c.policy.GRPC.FullMethod, Message: encoded}})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, mapHostError(err)
	}
	if responses == nil {
		return nil, invalidResponse()
	}
	return responses, nil
}

func recv(ctx context.Context, responses sdk.Responses, limits sdk.ResponseLimits, count uint32, aggregate int64) (sdk.Response, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Response{}, err
	}
	item, err := responses.Recv()
	if ctx.Err() != nil {
		return sdk.Response{}, ctx.Err()
	}
	if err != nil {
		if err == io.EOF {
			return sdk.Response{}, io.EOF
		}
		return sdk.Response{}, mapHostError(err)
	}
	if item.Status != 0 || item.ContentType != "" {
		return sdk.Response{}, invalidResponse()
	}
	size := int64(len(item.Body))
	if size > limits.MaxMessageBytes || count >= limits.MaxMessages || aggregate > limits.MaxAggregateBytes-size {
		return sdk.Response{}, invalidResponse()
	}
	return item, nil
}

func matches(message proto.Message, descriptor protoreflect.MessageDescriptor) bool {
	return message != nil && message.ProtoReflect().IsValid() && message.ProtoReflect().Descriptor() == descriptor
}

func decode(ctx context.Context, wire []byte, target proto.Message) error {
	decoded := target.ProtoReflect().New().Interface()
	if proto.Unmarshal(wire, decoded) != nil {
		return invalidResponse()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	proto.Reset(target)
	proto.Merge(target, decoded)
	return nil
}

func failure(code sdk.FailureCode, message string) sdk.Failure {
	return sdk.Failure{Code: string(code), Message: message}
}

func invalidResponse() sdk.Failure {
	return failure(sdk.FailureProductResponseFailed, "invalid product gRPC response")
}
