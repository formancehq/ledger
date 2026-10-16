package grpc

import (
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func failureDetailsReason(details *FailureDetails) ErrorReason {
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

func failureDetailsCode(details *FailureDetails) string {
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

func failureDetailsFacts(details *FailureDetails) map[string]string {
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

func (x *AuditFailure) GetCode() string { return failureDetailsCode(x.GetDetails()) }
func (x *AuditFailure) GetFacts() map[string]string {
	return failureDetailsFacts(x.GetDetails())
}
func (x *AuditFailure) GetReason() ErrorReason {
	return failureDetailsReason(x.GetDetails())
}
