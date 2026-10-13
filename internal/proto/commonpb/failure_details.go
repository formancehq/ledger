package commonpb

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// NewFailureDetails converts validated public facts into their typed persisted
// representation. The concrete code selects exactly one protobuf variant.
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
		for i := range message.Descriptor().Fields().Len() {
			candidate := message.Descriptor().Fields().Get(i)
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
			if err != nil {
				return nil, fmt.Errorf("fact %q is not an unsigned integer: %w", key, err)
			}
			if strconv.FormatUint(number, 10) != raw {
				return nil, fmt.Errorf("fact %q is not canonical unsigned decimal", key)
			}
			message.Set(field, protoreflect.ValueOfUint64(number))
		default:
			return nil, fmt.Errorf("unsupported failure fact type %s", field.Kind())
		}
	}
	parent.Set(arm, value)

	return details, nil
}

// FailureDetailsAllowsReason reads the coarse reasons declared on the selected
// protobuf variant. The schema owns this relationship; producers do not keep
// separate code/reason registries.
func FailureDetailsAllowsReason(details *FailureDetails, reason ErrorReason) bool {
	if details == nil || reason == ErrorReason_ERROR_REASON_UNSPECIFIED {
		return false
	}
	message := details.ProtoReflect()
	arm := message.WhichOneof(message.Descriptor().Oneofs().ByName("kind"))
	if arm == nil {
		return false
	}
	options, ok := arm.Options().(*descriptorpb.FieldOptions)
	if !ok || !proto.HasExtension(options, E_FailureReasons) {
		return false
	}

	return slices.Contains(proto.GetExtension(options, E_FailureReasons).([]ErrorReason), reason)
}

// ValidateRequiredFailureFacts enforces presence of every scalar fact in the
// selected variant. It deliberately checks presence rather than value:
// an explicitly supplied zero or empty string is different from an absent fact.
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
	}

	return nil
}

// MustFailureDetails is intended for static fixtures whose schema is fixed by
// the test. Production paths handle NewFailureDetails errors explicitly.
func MustFailureDetails(code string, facts map[string]string) *FailureDetails {
	details, err := NewFailureDetails(code, facts)
	if err != nil {
		panic(err)
	}

	return details
}

// FailureDetailsCode returns the selected stable code. An empty result means
// the details lack a variant and are not a valid audited outcome.
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

// FailureDetailsFacts renders the selected variant for public ErrorInfo and
// JSON boundaries. Persistence always uses the typed message instead.
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

func (x *OrderSkippedLog) GetFacts() map[string]string    { return FailureDetailsFacts(x.GetDetails()) }
func (x *IdempotencyFailure) GetFacts() map[string]string { return FailureDetailsFacts(x.GetDetails()) }
