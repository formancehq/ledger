package readprojection

import (
	"errors"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// validateCredentialWire protects opaque evidence from protobuf's last-value
// semantics. An earlier credential can disappear from the decoded message
// while remaining in its original bytes. Reject conflicting occurrences only
// when one contains a value the ordinary credential policy would redact.
// Unrelated duplicate fields and noncanonical encodings remain byte-exact.
func validateCredentialWire(data []byte, message proto.Message) error {
	descriptor := message.ProtoReflect().Descriptor()
	relevant := credentialWireAncestors(descriptor)
	_, err := walkCredentialWire(data, descriptor, relevant, credentialWirePath{}, 0)

	return err
}

// Derive the relevant schema paths, rather than maintaining another list of
// request/order/log variants beside the generated schema. Cycles in unrelated
// query and business messages are visited once and are not walked on the wire.
func credentialWireAncestors(root protoreflect.MessageDescriptor) map[protoreflect.FullName]bool {
	reverse := map[protoreflect.FullName][]protoreflect.FullName{}
	seen := map[protoreflect.FullName]bool{}
	var visit func(protoreflect.MessageDescriptor)
	visit = func(desc protoreflect.MessageDescriptor) {
		if seen[desc.FullName()] {
			return
		}
		seen[desc.FullName()] = true
		for i := range desc.Fields().Len() {
			if child := desc.Fields().Get(i).Message(); child != nil {
				reverse[child.FullName()] = append(reverse[child.FullName()], desc.FullName())
				visit(child)
			}
		}
	}
	visit(root)
	relevant := map[protoreflect.FullName]bool{}
	queue := []protoreflect.FullName{"common.SinkConfig", "common.MirrorSourceConfig"}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if relevant[name] {
			continue
		}
		relevant[name] = true
		queue = append(queue, reverse[name]...)
	}

	return relevant
}

type credentialWirePath struct {
	root   protoreflect.FullName
	fields []protowire.Number
}

type credentialWireOccurrence struct {
	count     int
	sensitive bool
}

func walkCredentialWire(data []byte, desc protoreflect.MessageDescriptor, relevant map[protoreflect.FullName]bool, path credentialWirePath, depth int) (bool, error) {
	// Credential configurations have shallow schemas. Bound malformed or
	// recursively encoded input independently of the decoder's recursion limit.
	if depth > 100 {
		return false, errors.New("credential container exceeds wire nesting limit")
	}
	if desc.FullName() == "common.SinkConfig" || desc.FullName() == "common.MirrorSourceConfig" {
		path = credentialWirePath{root: desc.FullName()}
	}
	fields := map[protoreflect.FieldNumber]credentialWireOccurrence{}
	oneofs := map[protoreflect.FullName]credentialWireOccurrence{}
	anySensitive := false
	for len(data) > 0 {
		number, kind, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 {
			return false, errors.New("invalid credential container wire tag")
		}
		valueSize := protowire.ConsumeFieldValue(number, kind, data[tagSize:])
		if valueSize < 0 {
			return false, errors.New("invalid credential container wire value")
		}
		raw := data[:tagSize+valueSize]
		value := data[tagSize : tagSize+valueSize]
		data = data[tagSize+valueSize:]
		field := desc.Fields().ByNumber(number)
		if field == nil {
			continue // Unknown fields are not credential fields in this contract.
		}
		sensitive := false
		if child := field.Message(); child != nil && (path.root != "" || relevant[child.FullName()]) {
			if kind != protowire.BytesType {
				return false, errors.New("invalid credential message wire type")
			}
			body, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return false, errors.New("invalid credential message wire bytes")
			}
			childPath := credentialWirePath{root: path.root, fields: append(append([]protowire.Number(nil), path.fields...), number)}
			var err error
			sensitive, err = walkCredentialWire(body, child, relevant, childPath, depth+1)
			if err != nil {
				return false, err
			}
		} else if path.root != "" && field.Message() == nil {
			var err error
			sensitive, err = credentialWireLeafChanges(raw, path)
			if err != nil {
				return false, err
			}
		}
		anySensitive = anySensitive || sensitive
		if !field.IsList() && !field.IsMap() {
			previous := fields[number]
			if previous.count > 0 && (previous.sensitive || sensitive) {
				return false, errors.New("shadowed credential in repeated singular protobuf field")
			}
			fields[number] = credentialWireOccurrence{count: previous.count + 1, sensitive: previous.sensitive || sensitive}
		}
		if oneof := field.ContainingOneof(); oneof != nil {
			previous := oneofs[oneof.FullName()]
			if previous.count > 0 && (previous.sensitive || sensitive) {
				return false, errors.New("shadowed credential in repeated protobuf oneof")
			}
			oneofs[oneof.FullName()] = credentialWireOccurrence{count: previous.count + 1, sensitive: previous.sensitive || sensitive}
		}
	}

	return anySensitive, nil
}

// Rebuild only the wrappers leading to this occurrence. Running the existing
// configuration policy on that leaf reveals shadowed values without duplicating
// the URL, DSN, OAuth, or sink credential rules in a second implementation.
func credentialWireLeafChanges(raw []byte, path credentialWirePath) (bool, error) {
	for _, v := range slices.Backward(path.fields) {
		raw = protowire.AppendBytes(protowire.AppendTag(nil, v, protowire.BytesType), raw)
	}
	switch path.root {
	case "common.SinkConfig":
		config := &commonpb.SinkConfig{}
		if err := config.UnmarshalVT(raw); err != nil {
			return false, errors.New("invalid sink credential wire field")
		}

		return redactSink(config), nil
	case "common.MirrorSourceConfig":
		config := &commonpb.MirrorSourceConfig{}
		if err := config.UnmarshalVT(raw); err != nil {
			return false, errors.New("invalid mirror credential wire field")
		}

		return redactMirror(config), nil
	default:
		return false, errors.New("unknown credential wire container")
	}
}
