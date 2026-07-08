// protoc-gen-skippable generates the Go whitelist that admission consults to
// validate LedgerApplyRequest.skippable_reasons. It scans every oneof case
// annotated with the `(allowed_skippable_reasons)` field option (declared in
// misc/proto/bucket.proto) and emits, per containing message, a
// SkippableReasonsFor<Message> function that returns the accepted reasons for
// the currently-active oneof variant. Cases without the annotation get no
// entry — the returned slice is nil, which admission treats as "no skip
// allowed on this action".
//
// The generator keeps the whitelist co-located with the wire declaration: to
// add or extend a per-action whitelist, edit only the .proto annotation.
// The Go admission code consumes the generated switch and never has to be
// touched in lockstep.
//
// Install:  go build -o protoc-gen-skippable .
// Usage:    protoc --skippable_out=. --skippable_opt=module=<module> ...
package main

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// extensionFullName identifies the field option this plugin reads. Declared
// in misc/proto/bucket.proto:
//
//	extend google.protobuf.FieldOptions {
//	    repeated common.ErrorReason allowed_skippable_reasons = 71001;
//	}
//
// Matching by full name (rather than field number) is stable against
// number reassignment and keeps the plugin decoupled from the numeric
// tag.
const extensionFullName = "ledger.allowed_skippable_reasons"

// oneofCase records one oneof variant that carries the annotation. The
// tuple (parent message, wrapper go-type, accepted reasons) is what the
// generator emits into the switch.
type oneofCase struct {
	// wrapperGoType is the generated Go type for the oneof wrapper
	// (e.g., "LedgerAction_CreateTransaction").
	wrapperGoType string
	// reasons is the list of enum value names (short form, e.g.
	// "ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT") the annotation lists,
	// in declaration order.
	reasons []string
	// enumGoImportPath is the Go import path for the enum. Every reason
	// on a given case comes from the same enum (protobuf enforces this),
	// so tracking it per case is sufficient to import it once.
	enumGoImportPath protogen.GoImportPath
	// enumGoName is the enum's Go type name (e.g., "ErrorReason") — used
	// to build the ident. Enum values in generated Go are exposed as
	// `<enum>_<VALUE_NAME>`.
	enumGoName string
}

// annotatedMessage groups every annotated oneof case that lives under a
// single top-level message (typically LedgerAction). The generator emits
// one SkippableReasonsFor<Message> per group.
type annotatedMessage struct {
	// goName is the Go type name of the containing message (e.g.,
	// "LedgerAction").
	goName string
	// getter is the Go method that returns the oneof interface value
	// (e.g., "GetData"). Derived from the oneof name.
	getter string
	// cases is the ordered list of oneof cases that carry the annotation.
	// Sorted by wrapperGoType so the generated output is deterministic
	// across runs regardless of proto field ordering.
	cases []oneofCase
}

func main() {
	protogen.Options{}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		// Resolve the annotation extension type once — descriptors are
		// stable for the lifetime of the plugin invocation. The
		// resolution walks every input file (including transitive
		// imports) since the extension may be declared in a file the
		// current file merely imports.
		extType := findExtensionType(gen, extensionFullName)
		if extType == nil {
			// No annotation declared in any input file → nothing to
			// generate. This is a valid state; some codebases run the
			// plugin over a broader .proto set than the one using the
			// annotation.
			return nil
		}

		// A local resolver knowing about extType is required to decode
		// the annotation bytes carried on FieldOptions. protogen's
		// default decoder uses protoregistry.GlobalTypes, which does
		// not carry input-file extensions — leaving the annotation as
		// unknown bytes on the message.
		resolver := new(protoregistry.Types)
		if err := resolver.RegisterExtension(extType); err != nil {
			return fmt.Errorf("register extension %q: %w", extensionFullName, err)
		}

		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}

			generateFile(gen, f, extType, resolver)
		}

		return nil
	})
}

// findExtensionType resolves the `(allowed_skippable_reasons)` extension by
// full name across every file the plugin was given. Walks each file's
// top-level Extensions and nested Messages' Extensions — we don't
// hard-code the containing file so the extension can move without
// breaking the plugin.
func findExtensionType(gen *protogen.Plugin, fullName string) protoreflect.ExtensionType {
	for _, f := range gen.Files {
		if xt := findExtensionInFile(f.Desc, fullName); xt != nil {
			return xt
		}
	}

	return nil
}

func findExtensionInFile(file protoreflect.FileDescriptor, fullName string) protoreflect.ExtensionType {
	exts := file.Extensions()
	for i := 0; i < exts.Len(); i++ {
		xd := exts.Get(i)
		if string(xd.FullName()) == fullName {
			return dynamicpb.NewExtensionType(xd)
		}
	}

	msgs := file.Messages()
	for i := 0; i < msgs.Len(); i++ {
		if xt := findExtensionInMessage(msgs.Get(i), fullName); xt != nil {
			return xt
		}
	}

	return nil
}

func findExtensionInMessage(msg protoreflect.MessageDescriptor, fullName string) protoreflect.ExtensionType {
	exts := msg.Extensions()
	for i := 0; i < exts.Len(); i++ {
		xd := exts.Get(i)
		if string(xd.FullName()) == fullName {
			return dynamicpb.NewExtensionType(xd)
		}
	}

	nested := msg.Messages()
	for i := 0; i < nested.Len(); i++ {
		if xt := findExtensionInMessage(nested.Get(i), fullName); xt != nil {
			return xt
		}
	}

	return nil
}

// generateFile emits internal/proto/<pkg>/<file>_skippable.pb.go for the
// given .proto file when at least one of its messages contains an
// annotated oneof case.
func generateFile(gen *protogen.Plugin, file *protogen.File, extType protoreflect.ExtensionType, resolver *protoregistry.Types) {
	var annotated []annotatedMessage

	for _, msg := range file.Messages {
		if am, ok := collectAnnotatedMessage(msg, extType, resolver); ok {
			annotated = append(annotated, am)
		}
	}

	if len(annotated) == 0 {
		return
	}

	// Stable output order: sort by message name so a proto reordering
	// doesn't churn the generated file.
	sort.Slice(annotated, func(i, j int) bool {
		return annotated[i].goName < annotated[j].goName
	})

	filename := file.GeneratedFilenamePrefix + "_skippable.pb.go"
	g := gen.NewGeneratedFile(filename, file.GoImportPath)

	g.P("// Code generated by protoc-gen-skippable. DO NOT EDIT.")
	g.P("// Source: ", file.Desc.Path())
	g.P()
	g.P("package ", file.GoPackageName)
	g.P()

	for _, am := range annotated {
		emitFunction(g, am)
	}
}

// collectAnnotatedMessage walks a proto message's oneof cases, extracting
// every `(allowed_skippable_reasons)` annotation. Returns (result, true)
// when the message has at least one annotated case.
func collectAnnotatedMessage(msg *protogen.Message, extType protoreflect.ExtensionType, resolver *protoregistry.Types) (annotatedMessage, bool) {
	if len(msg.Oneofs) == 0 {
		return annotatedMessage{}, false
	}

	var (
		cases     []oneofCase
		oneofName protoreflect.Name
	)

	for _, of := range msg.Oneofs {
		for _, field := range of.Fields {
			reasons, enumField, ok := readAllowedReasons(field, extType, resolver)
			if !ok {
				continue
			}

			// Every annotation must come from the SAME containing oneof
			// on the SAME message so the generated switch is well-formed.
			// protogen guarantees this — Oneofs[].Fields[] never mixes
			// containers — but pin the assumption here.
			if oneofName == "" {
				oneofName = of.Desc.Name()
			}

			cases = append(cases, oneofCase{
				wrapperGoType:    field.GoIdent.GoName,
				reasons:          reasons,
				enumGoImportPath: enumField.Enum.GoIdent.GoImportPath,
				enumGoName:       enumField.Enum.GoIdent.GoName,
			})
		}
	}

	if len(cases) == 0 {
		return annotatedMessage{}, false
	}

	sort.Slice(cases, func(i, j int) bool {
		return cases[i].wrapperGoType < cases[j].wrapperGoType
	})

	return annotatedMessage{
		goName: msg.GoIdent.GoName,
		getter: "Get" + upperCamel(string(oneofName)),
		cases:  cases,
	}, true
}

// readAllowedReasons extracts the annotation payload from a oneof case
// field descriptor via the resolved extension type. Returns (reasons,
// enumField, true) when the field carries the extension.
//
// The generated Go extension type only becomes available after
// protoc-gen-go finishes, so this plugin cannot import a static
// extension identifier. dynamicpb.NewExtensionType (see main) builds one
// from the extension descriptor at runtime; proto.GetExtension then
// returns the annotation value as a protoreflect.List of enum numbers.
func readAllowedReasons(field *protogen.Field, extType protoreflect.ExtensionType, resolver *protoregistry.Types) ([]string, *protogen.Field, bool) {
	opts := field.Desc.Options()
	if opts == nil {
		return nil, nil, false
	}

	// protogen loads FieldOptions using the well-known descriptorpb
	// resolver, which doesn't know about our custom extension — so the
	// annotation lands in the message's unknown-fields blob rather than
	// being decoded onto the extension slot. Re-parse the options using
	// a resolver that DOES know about extType and the extension surfaces
	// on the fresh FieldOptions we can then read with proto.GetExtension.
	//
	// The alternative would be to hand-decode the wire bytes at the
	// extension's tag, but that duplicates protobuf's decoder and breaks
	// as soon as the annotation grows a new field.
	data, err := proto.Marshal(opts)
	if err != nil {
		return nil, nil, false
	}

	reparsed := &descriptorpb.FieldOptions{}
	if err := (proto.UnmarshalOptions{Resolver: resolver}).Unmarshal(data, reparsed); err != nil {
		return nil, nil, false
	}

	var (
		list  protoreflect.List
		found bool
	)

	// After decoding with our resolver, the extension surfaces on
	// reparsed under a FieldDescriptor whose FullName matches our
	// registered extType. proto.HasExtension expects strict descriptor
	// identity, which dynamicpb-built descriptors may not preserve
	// across resolver boundaries — iterate the reflected fields and
	// match by name instead.
	reparsed.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsExtension() && string(fd.FullName()) == extensionFullName {
			if lv, ok := v.Interface().(protoreflect.List); ok {
				list = lv
				found = true
			}

			return false
		}

		return true
	})

	if !found || list == nil {
		return nil, nil, false
	}

	extDesc := extType.TypeDescriptor().Descriptor()

	enumDesc := extDesc.Enum()
	if enumDesc == nil {
		return nil, nil, false
	}

	var reasons []string

	for i := 0; i < list.Len(); i++ {
		num := list.Get(i).Enum()

		enumVal := enumDesc.Values().ByNumber(num)
		if enumVal == nil {
			continue
		}

		reasons = append(reasons, string(enumVal.Name()))
	}

	if len(reasons) == 0 {
		return nil, nil, false
	}

	return reasons, &protogen.Field{Enum: synthesizeEnum(enumDesc)}, true
}

// synthesizeEnum builds the minimal *protogen.Enum the emitter needs
// (GoIdent only). protogen normally builds these through the plugin
// wiring, but the plugin doesn't need to bind the Enum to a file — the
// emitter just consumes GoIdent for import resolution and type naming.
func synthesizeEnum(desc protoreflect.EnumDescriptor) *protogen.Enum {
	fileOpts := desc.ParentFile().Options()
	if fileOpts == nil {
		return nil
	}

	goPkg, ok := extractGoPackage(fileOpts)
	if !ok {
		return nil
	}

	return &protogen.Enum{
		GoIdent: protogen.GoIdent{
			GoName:       goEnumTypeName(desc),
			GoImportPath: protogen.GoImportPath(goImportPath(goPkg)),
		},
		Desc: desc,
		// Values are not materialised — the emitter builds enum-value
		// idents from names directly (see emitFunction).
	}
}

// goEnumTypeName mirrors protoc-gen-go: nested enums join their parent
// message names with '_'. Only top-level enums are currently annotated,
// so the trivial case (last segment) suffices; extend when nested enums
// join the annotation set.
func goEnumTypeName(desc protoreflect.EnumDescriptor) string {
	name := string(desc.Name())
	parent := desc.Parent()
	for parent != nil {
		if _, ok := parent.(protoreflect.MessageDescriptor); !ok {
			break
		}

		name = string(parent.(protoreflect.MessageDescriptor).Name()) + "_" + name
		parent = parent.Parent()
	}

	return name
}

// emitFunction writes one SkippableReasonsFor<Message> function to the
// generated file. The function returns the annotated reasons for the
// active oneof variant, or nil for unannotated variants (including nil
// receivers).
//
// Generated shape (for LedgerAction):
//
//	func SkippableReasonsForLedgerAction(x *LedgerAction) []commonpb.ErrorReason {
//	    switch x.GetData().(type) {
//	    case *LedgerAction_CreateTransaction:
//	        return []commonpb.ErrorReason{
//	            commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
//	        }
//	    default:
//	        return nil
//	    }
//	}
func emitFunction(g *protogen.GeneratedFile, am annotatedMessage) {
	enumImport := am.cases[0].enumGoImportPath
	enumType := am.cases[0].enumGoName
	enumIdent := g.QualifiedGoIdent(protogen.GoIdent{
		GoName:       enumType,
		GoImportPath: enumImport,
	})

	g.P("// SkippableReasonsFor", am.goName, " returns the business ErrorReasons that")
	g.P("// admission accepts for the currently-active oneof variant of x. Variants")
	g.P("// without an (allowed_skippable_reasons) annotation return nil, which")
	g.P("// admission treats as \"no skip allowed on this action\".")
	g.P("//")
	g.P("// The table is derived from the .proto annotations by protoc-gen-skippable.")
	g.P("// To extend, add `(allowed_skippable_reasons) = ERROR_REASON_X` on the")
	g.P("// corresponding oneof case in the .proto and regenerate.")
	g.P("func SkippableReasonsFor", am.goName, "(x *", am.goName, ") []", enumIdent, " {")
	g.P("switch x.", am.getter, "().(type) {")

	for _, c := range am.cases {
		g.P("case *", c.wrapperGoType, ":")
		g.P("return []", enumIdent, "{")

		for _, reasonName := range c.reasons {
			ident := g.QualifiedGoIdent(protogen.GoIdent{
				GoName:       enumType + "_" + reasonName,
				GoImportPath: enumImport,
			})
			g.P(ident, ",")
		}

		g.P("}")
	}

	g.P("default:")
	g.P("return nil")
	g.P("}")
	g.P("}")
	g.P()
}

// upperCamel converts a snake_case proto name to UpperCamelCase for Go
// method derivation (e.g., "data" → "Data").
func upperCamel(s string) string {
	if s == "" {
		return s
	}

	parts := strings.Split(s, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}

		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}

	return strings.Join(parts, "")
}

// extractGoPackage pulls the go_package file option out of the raw
// FileOptions proto without importing descriptorpb (which would pull the
// whole descriptor package into the plugin). We iterate the options'
// reflected fields and match by name.
func extractGoPackage(opts protoreflect.ProtoMessage) (string, bool) {
	var goPkg string
	found := false

	opts.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Name() != "go_package" {
			return true
		}

		goPkg = v.String()
		found = true

		return false
	})

	return goPkg, found
}

// goImportPath extracts the import path from the "path[;name]" go_package
// syntax used across the ledger's protos. The optional ";alias" suffix is
// stripped — the emitter uses protogen.GoImportPath for import resolution
// so the alias is not needed here.
func goImportPath(spec string) string {
	path, _, _ := strings.Cut(spec, ";")
	return path
}

