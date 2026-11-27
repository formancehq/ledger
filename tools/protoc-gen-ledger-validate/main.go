// protoc-gen-ledger-validate generates declared input presence and value checks.
package main

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/pluginpb"
)

const (
	requiredExtensionName   = "common.required_for_input"
	nonZeroExtensionName    = "common.non_zero_for_input"
	maxBytesExtensionName   = "common.max_bytes_for_input"
	exactBytesExtensionName = "common.exact_bytes_for_input"
)

type fieldRule struct {
	field      *protogen.Field
	nonZero    bool
	maxBytes   uint64
	exactBytes uint64
}

type messageRules struct {
	message *protogen.Message
	fields  []fieldRule
}

func main() { protogen.Options{}.Run(generate) }

func generate(gen *protogen.Plugin) error {
	gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
	var requiredExtension, nonZeroExtension, maxBytesExtension, exactBytesExtension protoreflect.ExtensionDescriptor
	for _, file := range gen.Files {
		for _, candidate := range file.Extensions {
			switch string(candidate.Desc.FullName()) {
			case requiredExtensionName:
				requiredExtension = candidate.Desc
			case nonZeroExtensionName:
				nonZeroExtension = candidate.Desc
			case maxBytesExtensionName:
				maxBytesExtension = candidate.Desc
			case exactBytesExtensionName:
				exactBytesExtension = candidate.Desc
			}
		}
	}
	if requiredExtension == nil || nonZeroExtension == nil || maxBytesExtension == nil || exactBytesExtension == nil {
		return fmt.Errorf("input validation extensions not found")
	}
	resolver := &protoregistry.Types{}
	if err := resolver.RegisterExtension(dynamicpb.NewExtensionType(requiredExtension)); err != nil {
		return fmt.Errorf("register extension: %w", err)
	}
	if err := resolver.RegisterExtension(dynamicpb.NewExtensionType(nonZeroExtension)); err != nil {
		return fmt.Errorf("register extension: %w", err)
	}
	for _, extension := range []protoreflect.ExtensionDescriptor{maxBytesExtension, exactBytesExtension} {
		if err := resolver.RegisterExtension(dynamicpb.NewExtensionType(extension)); err != nil {
			return fmt.Errorf("register extension: %w", err)
		}
	}
	for _, file := range gen.Files {
		if !file.Generate {
			continue
		}
		var messages []messageRules
		var visit func(*protogen.Message) error
		visit = func(message *protogen.Message) error {
			if message.Desc.IsMapEntry() {
				return nil
			}
			var fields []fieldRule
			for _, field := range message.Fields {
				present, required, err := readRule(field, requiredExtension, resolver)
				if err != nil {
					return err
				}
				nonZeroPresent, nonZero, err := readRule(field, nonZeroExtension, resolver)
				if err != nil {
					return err
				}
				maxPresent, maxBytes, err := readUintRule(field, maxBytesExtension, resolver)
				if err != nil {
					return err
				}
				exactPresent, exactBytes, err := readUintRule(field, exactBytesExtension, resolver)
				if err != nil {
					return err
				}
				if present && !required {
					return fmt.Errorf("%s: required_for_input=false is invalid; remove the annotation", field.Desc.FullName())
				}
				if nonZeroPresent && !nonZero {
					return fmt.Errorf("%s: non_zero_for_input=false is invalid; remove the annotation", field.Desc.FullName())
				}
				if present && nonZeroPresent {
					return fmt.Errorf("%s: required_for_input and non_zero_for_input cannot be combined", field.Desc.FullName())
				}
				if !present && !nonZeroPresent && !maxPresent && !exactPresent {
					continue
				}
				if maxPresent && maxBytes == 0 || exactPresent && exactBytes == 0 {
					return fmt.Errorf("%s: byte limit must be positive", field.Desc.FullName())
				}
				if maxPresent && exactPresent {
					return fmt.Errorf("%s: max_bytes_for_input and exact_bytes_for_input cannot be combined", field.Desc.FullName())
				}
				if (maxPresent || exactPresent) && (present || nonZeroPresent) {
					return fmt.Errorf("%s: byte limits cannot be combined with presence or non-zero rules", field.Desc.FullName())
				}
				if (maxPresent || exactPresent) && field.Desc.Kind() != protoreflect.StringKind && field.Desc.Kind() != protoreflect.BytesKind {
					return fmt.Errorf("%s: byte limit requires a string or bytes field", field.Desc.FullName())
				}
				if field.Desc.IsList() || field.Desc.IsMap() || field.Desc.ContainingOneof() != nil && !field.Desc.ContainingOneof().IsSynthetic() {
					return fmt.Errorf("%s: input validation requires a singular field outside a oneof", field.Desc.FullName())
				}
				if present && !field.Desc.HasPresence() {
					return fmt.Errorf("%s: required_for_input requires a singular field with explicit presence outside a oneof (use optional or a message)", field.Desc.FullName())
				}
				if nonZeroPresent && (field.Desc.Kind() == protoreflect.MessageKind || field.Desc.Kind() == protoreflect.GroupKind) {
					return fmt.Errorf("%s: non_zero_for_input requires a scalar field", field.Desc.FullName())
				}
				fields = append(fields, fieldRule{field: field, nonZero: nonZeroPresent, maxBytes: maxBytes, exactBytes: exactBytes})
			}
			if len(fields) > 0 {
				for _, field := range message.Fields {
					if field.GoName == "ValidateInput" {
						return fmt.Errorf("%s: field name collides with generated ValidateInput method", field.Desc.FullName())
					}
				}
				slices.SortFunc(fields, func(a, b fieldRule) int { return int(a.field.Desc.Number() - b.field.Desc.Number()) })
				messages = append(messages, messageRules{message, fields})
			}
			for _, nested := range message.Messages {
				if err := visit(nested); err != nil {
					return err
				}
			}
			return nil
		}
		for _, message := range file.Messages {
			if err := visit(message); err != nil {
				return err
			}
		}
		if len(messages) == 0 {
			continue
		}
		g := gen.NewGeneratedFile(file.GeneratedFilenamePrefix+"_ledger_validate.pb.go", file.GoImportPath)
		g.P("// Code generated by protoc-gen-ledger-validate. DO NOT EDIT.")
		g.P("package ", file.GoPackageName)
		for _, item := range messages {
			g.P("// ValidateInput checks declared direct-field input requirements.")
			g.P("func (m *", item.message.GoIdent.GoName, ") ValidateInput() error {")
			for _, rule := range item.fields {
				condition := ""
				errorRule := ""
				lengthValue := "m." + rule.field.GoName
				if rule.field.Desc.Kind() == protoreflect.StringKind && rule.field.Desc.HasPresence() {
					lengthValue = "m.Get" + rule.field.GoName + "()"
				}
				switch {
				case rule.nonZero:
					condition = nonZeroCondition(rule.field)
				case rule.maxBytes > 0:
					condition = fmt.Sprintf("len(%s) > %d", lengthValue, rule.maxBytes)
					errorRule = `, Rule: "max_bytes"`
				case rule.exactBytes > 0:
					condition = fmt.Sprintf("len(%s) != %d", lengthValue, rule.exactBytes)
					errorRule = `, Rule: "exact_bytes"`
				default:
					condition = "m." + rule.field.GoName + " == nil"
				}
				g.P("if m == nil || ", condition, " {")
				g.P("return &", g.QualifiedGoIdent(protogen.GoIdent{GoName: "FieldError", GoImportPath: "github.com/formancehq/ledger/v3/internal/proto/inputvalidation"}), "{Path: ", fmt.Sprintf("%q", rule.field.Desc.Name()), errorRule, "}")
				g.P("}")
			}
			g.P("return nil")
			g.P("}")
		}
	}
	return nil
}

func nonZeroCondition(field *protogen.Field) string {
	value := "m." + field.GoName
	missing := ""
	switch field.Desc.Kind() {
	case protoreflect.StringKind:
		missing = value + ` == ""`
	case protoreflect.BytesKind:
		missing = "len(" + value + ") == 0"
	case protoreflect.BoolKind:
		missing = "!" + value
	default:
		missing = value + " == 0"
	}
	if field.Desc.HasPresence() && field.Desc.Kind() != protoreflect.BytesKind {
		return value + " == nil || " + nonZeroConditionValue(field, "*"+value)
	}
	return missing
}

func nonZeroConditionValue(field *protogen.Field, value string) string {
	switch field.Desc.Kind() {
	case protoreflect.StringKind:
		return value + ` == ""`
	case protoreflect.BoolKind:
		return "!" + value
	default:
		return value + " == 0"
	}
}

func readRule(field *protogen.Field, extension protoreflect.ExtensionDescriptor, resolver *protoregistry.Types) (bool, bool, error) {
	present, value, err := readExtension(field, extension, resolver)
	if err != nil || !present {
		return present, false, err
	}
	return present, value.Bool(), err
}

func readUintRule(field *protogen.Field, extension protoreflect.ExtensionDescriptor, resolver *protoregistry.Types) (bool, uint64, error) {
	present, value, err := readExtension(field, extension, resolver)
	if err != nil || !present {
		return present, 0, err
	}
	return present, value.Uint(), err
}

func readExtension(field *protogen.Field, extension protoreflect.ExtensionDescriptor, resolver *protoregistry.Types) (bool, protoreflect.Value, error) {
	opts := field.Desc.Options()
	if opts == nil {
		return false, protoreflect.Value{}, nil
	}
	raw, err := proto.Marshal(opts)
	if err != nil {
		return false, protoreflect.Value{}, fmt.Errorf("%s: marshal options: %w", field.Desc.FullName(), err)
	}
	resolved := dynamicpb.NewMessage(opts.ProtoReflect().Descriptor())
	if err := (proto.UnmarshalOptions{Resolver: resolver}).Unmarshal(raw, resolved); err != nil {
		return false, protoreflect.Value{}, fmt.Errorf("%s: parse options: %w", field.Desc.FullName(), err)
	}
	resolvedExtension := dynamicpb.NewExtensionType(extension).TypeDescriptor()
	if !resolved.Has(resolvedExtension) {
		return false, protoreflect.Value{}, nil
	}
	return true, resolved.Get(resolvedExtension), nil
}
