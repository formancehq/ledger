package ledgerv3

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/productgrpcmessage"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	ledgerconfig "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3/internal/ledgerconfig"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// executeV3 is the only adapter between the transport-independent plugin and
// Ledger v3's generated gRPC client. Endpoint and credential resolution remain
// entirely host-owned through productgrpcmessage.
func executeV3(ctx context.Context, request sdk.ExecuteRequest, host sdk.Host) error {
	command, ok := commandByID(request.CommandID)
	if !ok {
		return invalidArgument("unknown command %q", request.CommandID)
	}
	if err := sdk.ValidateExecuteRequest(command, request); err != nil {
		return invalidArgument("invalid execution request: %s", err)
	}
	if err := sdk.ValidateTargetSelection(command.Target, request.Target); err != nil {
		return invalidArgument("invalid target: %s", err)
	}
	decoded, err := decode(command, request)
	if err != nil {
		return err
	}

	handlers := []func(context.Context, sdk.ExecuteRequest, input, sdk.Command, sdk.Host) (bool, error){
		executeV3Reads,
		executeV3AccountMutations,
		executeV3Transactions,
		executeV3AccountTypes,
		executeV3Indexes,
		executeV3Ledgers,
		executeV3Numscripts,
		executeV3Queries,
	}
	for _, handle := range handlers {
		handled, err := handle(ctx, request, decoded, command, host)
		if handled {
			return err
		}
	}
	return v3Failure("command %q has no Ledger v3 adapter", request.CommandID)
}

func commandByID(id string) (sdk.Command, bool) {
	for _, command := range (Plugin{}).Commands() {
		if command.ID == id {
			return command, true
		}
	}
	return sdk.Command{}, false
}

func operationByID(command sdk.Command, id string) (sdk.OperationPolicy, bool) {
	for _, operation := range command.Operations {
		if operation.ID == id {
			return operation, true
		}
	}
	return sdk.OperationPolicy{}, false
}

func newV3Client(host sdk.Host, command sdk.Command, operationID string) (*productgrpcmessage.Client, error) {
	operation, ok := operationByID(command, operationID)
	if !ok {
		return nil, v3Failure("descriptor does not declare operation %q", operationID)
	}
	method, err := bucketMethodDescriptor(operation)
	if err != nil {
		return nil, err
	}
	client, err := productgrpcmessage.NewClient(host, operation, stackAuthCapability, method)
	if err != nil {
		return nil, v3Failure("configure generated gRPC operation %q: %v", operationID, err)
	}
	return client, nil
}

func bucketMethodDescriptor(operation sdk.OperationPolicy) (protoreflect.MethodDescriptor, error) {
	if operation.GRPC == nil || !strings.HasPrefix(operation.GRPC.FullMethod, bucketServicePrefix) {
		return nil, v3Failure("operation %q does not bind a Ledger v3 BucketService method", operation.ID)
	}
	name := strings.TrimPrefix(operation.GRPC.FullMethod, bucketServicePrefix)
	service := servicepb.File_bucket_proto.Services().ByName(bucketServiceName)
	if service == nil {
		return nil, v3Failure("Ledger v3 BucketService descriptor is unavailable")
	}
	method := service.Methods().ByName(protoreflect.Name(name))
	if method == nil {
		return nil, v3Failure("operation %q binds unknown Ledger v3 method %q", operation.ID, name)
	}
	return method, nil
}

func unaryV3[Response proto.Message](ctx context.Context, host sdk.Host, command sdk.Command, operationID string, request proto.Message, response Response) (Response, error) {
	var zero Response
	client, err := newV3Client(host, command, operationID)
	if err != nil {
		return zero, err
	}
	if err := client.Invoke(ctx, request, response); err != nil {
		return zero, v3Failure("%s: %v", operationID, err)
	}
	return response, nil
}

type v3Stream[Message proto.Message] struct {
	stream  *productgrpcmessage.Stream
	newItem func() Message
}

func streamV3[Message proto.Message](ctx context.Context, host sdk.Host, command sdk.Command, operationID string, request proto.Message, newItem func() Message) (*v3Stream[Message], error) {
	client, err := newV3Client(host, command, operationID)
	if err != nil {
		return nil, err
	}
	stream, err := client.NewStream(ctx, request)
	if err != nil {
		return nil, v3Failure("%s: %v", operationID, err)
	}
	return &v3Stream[Message]{stream: stream, newItem: newItem}, nil
}

func applyV3(ctx context.Context, host sdk.Host, command sdk.Command, operationID, idempotencyKey string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
	response, err := unaryV3(ctx, host, command, operationID, servicepb.UnsignedApplyRequest(idempotencyKey, requests...), &servicepb.ApplyResponse{})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func emitProto(host sdk.Host, operationID string, message proto.Message) error {
	encoded, err := protojson.MarshalOptions{UseProtoNames: false}.Marshal(message)
	if err != nil {
		return v3Failure("encode %q response: %v", operationID, err)
	}
	return emitJSONBytes(host, operationID, sdk.ResultObject, encoded, nil)
}

func emitProtoList(host sdk.Host, operationID string, messages []proto.Message, page *sdk.PageInfo) error {
	items := make([]json.RawMessage, 0, len(messages))
	for _, message := range messages {
		encoded, err := protojson.MarshalOptions{UseProtoNames: false}.Marshal(message)
		if err != nil {
			return v3Failure("encode %q response: %v", operationID, err)
		}
		items = append(items, encoded)
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return v3Failure("encode %q collection: %v", operationID, err)
	}
	return emitJSONBytes(host, operationID, sdk.ResultCollection, encoded, page)
}

func emitJSON(host sdk.Host, operationID string, shape sdk.ResultShape, value any, page *sdk.PageInfo) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return v3Failure("encode %q result: %v", operationID, err)
	}
	return emitJSONBytes(host, operationID, shape, encoded, page)
}

func emitJSONBytes(host sdk.Host, operationID string, shape sdk.ResultShape, encoded []byte, page *sdk.PageInfo) error {
	return host.Emit(sdk.Event{Kind: sdk.EventResult, Result: &sdk.ResultEnvelope{
		OperationID: operationID,
		Shape:       shape,
		MediaType:   "application/json",
		Data:        encoded,
		Page:        page,
	}})
}

func emitEmpty(host sdk.Host, operationID string) error {
	return emitJSONBytes(host, operationID, sdk.ResultEmpty, []byte(`{}`), nil)
}

func v3Failure(format string, args ...any) error {
	return fmt.Errorf("ledger v3: %s", fmt.Sprintf(format, args...))
}

func parseMetadata(values []string) (map[string]*commonpb.MetadataValue, error) {
	metadata := make(map[string]string, len(values))
	for _, value := range values {
		key, raw, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(key) == "" {
			return nil, invalidArgument("metadata expects key=value")
		}
		if _, exists := metadata[key]; exists {
			return nil, invalidArgument("metadata key %q is repeated", key)
		}
		metadata[key] = raw
	}
	return commonpb.MetadataFromGoMap(metadata), nil
}

func parseQueryFilter(raw string, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	if raw == "" {
		return nil, nil
	}
	filter, err := ledgerconfig.DecodeFilter([]byte(raw), target)
	if err != nil {
		return nil, invalidArgument("invalid filter expression")
	}
	return filter, nil
}

func readArtifact(ctx context.Context, host sdk.Host, handle string, maxBytes int64) ([]byte, error) {
	if handle == "" {
		return nil, invalidArgument("input artifact handle is required")
	}
	result := make([]byte, 0)
	for reads := 0; reads < 4; reads++ {
		chunk, err := sdk.ReadInput(ctx, host, handle)
		if err != nil {
			return nil, v3Failure("read input artifact: %v", err)
		}
		if int64(len(result)) > maxBytes-int64(len(chunk.Bytes)) {
			return nil, invalidArgument("input artifact exceeds declared limit")
		}
		result = append(result, chunk.Bytes...)
		if chunk.Final {
			return result, nil
		}
	}
	return nil, invalidArgument("input artifact exceeds chunk limit")
}
