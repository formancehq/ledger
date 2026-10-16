package grpc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func NewFailureDetails(code string, facts map[string]string) (*FailureDetails, error) {
	details := &FailureDetails{}
	parent := details.ProtoReflect()
	arm := parent.Descriptor().Fields().ByName(protoreflect.Name(strings.ToLower(code)))
	if arm == nil || arm.ContainingOneof() == nil {
		return nil, fmt.Errorf("unknown failure detail code %q", code)
	}
	value := parent.NewField(arm)
	message := value.Message()
	for key, raw := range facts {
		var field protoreflect.FieldDescriptor
		fields := message.Descriptor().Fields()
		for i := range fields.Len() {
			candidate := fields.Get(i)
			if candidate.JSONName() == key {
				field = candidate
				break
			}
		}
		if field == nil {
			return nil, fmt.Errorf("fact %q is not part of %s", key, code)
		}
		switch field.Kind() {
		case protoreflect.StringKind:
			if !utf8.ValidString(raw) {
				return nil, fmt.Errorf("fact %q is not valid UTF-8", key)
			}
			message.Set(field, protoreflect.ValueOfString(raw))
		case protoreflect.Uint64Kind:
			number, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || strconv.FormatUint(number, 10) != raw {
				return nil, fmt.Errorf("fact %q is not a canonical unsigned decimal", key)
			}
			message.Set(field, protoreflect.ValueOfUint64(number))
		default:
			return nil, fmt.Errorf("unsupported failure fact type %s", field.Kind())
		}
	}
	parent.Set(arm, value)
	return details, nil
}

// MustFailureDetails is the test/fixture convenience form of NewFailureDetails.
func MustFailureDetails(code string, facts map[string]string) *FailureDetails {
	details, err := NewFailureDetails(code, facts)
	if err != nil {
		panic(err)
	}
	return details
}

func ValidateRequiredFailureFacts(details *FailureDetails) error {
	if details == nil {
		return errors.New("failure details are missing")
	}
	parent := details.ProtoReflect()
	arm := parent.WhichOneof(parent.Descriptor().Oneofs().ByName("kind"))
	if arm == nil {
		return errors.New("failure detail variant is missing")
	}
	message := parent.Get(arm).Message()
	fields := message.Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if !message.Has(field) {
			return fmt.Errorf("failure detail %s is missing required fact %s", arm.Name(), field.JSONName())
		}
		if field.Kind() == protoreflect.StringKind && !utf8.ValidString(message.Get(field).String()) {
			return fmt.Errorf("failure detail %s has invalid UTF-8 in fact %s", arm.Name(), field.JSONName())
		}
	}
	return nil
}

func FailureDetailsReason(details *FailureDetails) ErrorReason {
	if details == nil {
		return ErrorReason_ERROR_REASON_UNSPECIFIED
	}
	message := details.ProtoReflect()
	arm := message.WhichOneof(message.Descriptor().Oneofs().ByName("kind"))
	if arm == nil {
		return ErrorReason_ERROR_REASON_UNSPECIFIED
	}
	options, ok := arm.Options().(*descriptorpb.FieldOptions)
	if !ok || !proto.HasExtension(options, E_FailureReason) {
		return ErrorReason_ERROR_REASON_UNSPECIFIED
	}
	return proto.GetExtension(options, E_FailureReason).(ErrorReason)
}

func FailureDetailsCode(details *FailureDetails) string {
	if details == nil {
		return ""
	}
	message := details.ProtoReflect()
	arm := message.WhichOneof(message.Descriptor().Oneofs().ByName("kind"))
	if arm == nil {
		return ""
	}
	return strings.ToUpper(string(arm.Name()))
}

func FailureDetailsFacts(details *FailureDetails) map[string]string {
	if details == nil {
		return nil
	}
	parent := details.ProtoReflect()
	arm := parent.WhichOneof(parent.Descriptor().Oneofs().ByName("kind"))
	if arm == nil {
		return nil
	}
	message := parent.Get(arm).Message()
	facts := make(map[string]string)
	fields := message.Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if !message.Has(field) {
			continue
		}
		value := message.Get(field)
		switch field.Kind() {
		case protoreflect.StringKind:
			facts[field.JSONName()] = value.String()
		case protoreflect.Uint64Kind:
			facts[field.JSONName()] = strconv.FormatUint(value.Uint(), 10)
		}
	}
	return facts
}

func (x *OrderSkippedLog) GetCode() string             { return FailureDetailsCode(x.GetDetails()) }
func (x *OrderSkippedLog) GetFacts() map[string]string { return FailureDetailsFacts(x.GetDetails()) }
func (x *OrderSkippedLog) GetReason() ErrorReason      { return FailureDetailsReason(x.GetDetails()) }
