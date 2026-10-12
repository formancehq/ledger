package commonpb

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
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
			message.Set(field, protoreflect.ValueOfString(raw))
		case protoreflect.Uint64Kind:
			number, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("fact %q is not an unsigned integer: %w", key, err)
			}
			message.Set(field, protoreflect.ValueOfUint64(number))
		default:
			return nil, fmt.Errorf("unsupported failure fact type %s", field.Kind())
		}
	}
	parent.Set(arm, value)

	return details, nil
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
