package connectionconfig

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type sinkNormalizer struct {
	outputField protoreflect.FieldDescriptor
	normalize   func(proto.Message) (proto.Message, error)
}

type sinkNormalizerRegistry map[protoreflect.FieldNumber]sinkNormalizer

// Each sink registers from init in an unconditional, driver-free source file.
// After package initialization this registry is read-only. Runtime factory
// availability must never influence the projection of a committed order.
var sinkNormalizers = sinkNormalizerRegistry{}

// registerSinkNormalizer binds typed input/output messages to matching oneof
// variants. Registration mistakes are programmer errors and fail on startup.
// The registry parameter lets tests qualify invalid registrations in isolation.
func registerSinkNormalizer[I proto.Message, O proto.Message](registry sinkNormalizerRegistry, name protoreflect.Name, normalize func(I) (O, error)) {
	inputField := (&commonpb.SinkConfigInput{}).ProtoReflect().Descriptor().Fields().ByName(name)
	outputField := (&commonpb.SinkConfig{}).ProtoReflect().Descriptor().Fields().ByName(name)
	var input I
	var output O
	if inputField == nil || outputField == nil ||
		inputField.ContainingOneof() == nil || inputField.ContainingOneof().Name() != "type" ||
		outputField.ContainingOneof() == nil || outputField.ContainingOneof().Name() != "type" ||
		inputField.Message() != input.ProtoReflect().Descriptor() ||
		outputField.Message() != output.ProtoReflect().Descriptor() || normalize == nil {
		panic(fmt.Sprintf("invalid sink normalizer registration for %s", name))
	}
	if _, exists := registry[inputField.Number()]; exists {
		panic(fmt.Sprintf("duplicate sink normalizer registration for %s", name))
	}
	registry[inputField.Number()] = sinkNormalizer{
		outputField: outputField,
		normalize: func(message proto.Message) (proto.Message, error) {
			result, err := normalize(message.(I))
			if err != nil {
				return nil, err
			}
			if !result.ProtoReflect().IsValid() {
				panic(fmt.Sprintf("sink normalizer %s returned no configuration", name))
			}

			return result, nil
		},
	}
}

// Sink returns the operational projection without changing the accepted input.
func Sink(input *commonpb.SinkConfigInput) (*commonpb.SinkConfig, error) {
	return sinkNormalizers.project(input)
}

func (registry sinkNormalizerRegistry) project(input *commonpb.SinkConfigInput) (*commonpb.SinkConfig, error) {
	if input == nil {
		return nil, nil
	}
	message := input.ProtoReflect()
	field := message.WhichOneof(message.Descriptor().Oneofs().ByName("type"))
	if field == nil {
		return nil, errors.New("sink type is required")
	}
	normalizer, ok := registry[field.Number()]
	if !ok {
		panic(fmt.Sprintf("missing sink normalizer for %s", field.Name()))
	}
	config, err := normalizer.normalize(message.Get(field).Message().Interface())
	if err != nil {
		return nil, err
	}
	out := &commonpb.SinkConfig{Name: input.GetName(), Format: input.GetFormat(), BatchSize: input.GetBatchSize(), BatchDelayMs: input.GetBatchDelayMs(), EventTypes: append([]commonpb.EventType(nil), input.GetEventTypes()...)}
	out.ProtoReflect().Set(normalizer.outputField, protoreflect.ValueOfMessage(config.ProtoReflect()))

	return out, nil
}
