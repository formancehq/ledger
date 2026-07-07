// protoc-gen-reader generates read-only interfaces and wrapper types for
// protobuf messages. Each message gets:
//   - A <Message>Reader interface with all Get* methods + Mutate() *<Message>
//   - An unexported <message>Readonly named type defined as `type X Msg` so
//     that AsReader() can convert *Msg -> *<message>Readonly without heap
//     allocation. The resulting *<message>Readonly fits in the interface data
//     word, and every transitive GetFoo() reader view is likewise zero-alloc.
//   - An AsReader() method on the concrete type returning the reader interface
//   - A Mutate() method on the concrete type returning CloneVT()
//
// Immutability rules (enforced at the type level so a Reader cannot leak a
// mutable view of the underlying message):
//   - Singular sub-message getters return the sub-message Reader interface.
//   - Repeated sub-message getters return a <Message>ListReader interface that
//     exposes only Len/Get/Range and yields Reader views of the elements.
//   - Map getters return a <Outer>_<Field>MapReader interface that exposes
//     only Len/Get/Range. When the value is a message, callers receive a
//     Reader view of it.
//   - Repeated scalar getters return a freshly-cloned slice (slices.Clone).
//   - Singular []byte getters return a freshly-cloned slice (bytes.Clone).
//   - Repeated []byte ([][]byte) getters return a deep clone of the slice.
//   - Singular scalar getters return value types directly.
//
// Install:  go build -o protoc-gen-reader .
// Usage:    protoc --reader_out=. --reader_opt=module=<module> ...
package main

import (
	"fmt"
	"strings"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// extraMethod describes a custom read-only method to add to a reader interface
// and its wrapper. The method must already exist on the concrete proto type.
type extraMethod struct {
	name      string // method name
	signature string // full Go signature (params + return)
	call      string // delegation call on wrapper (e.g., "r.v.IntoUint256(dst)")
}

// extraMethods registers custom read-only methods for specific proto messages.
// Key: proto full name (e.g., "common.Uint256").
// These methods are added to the Reader interface and delegated on the wrapper.
// In `call`, the token %BASE% is substituted with a cast of the wrapper receiver
// back to the concrete message pointer (e.g., "(*Uint256)(r)").
var extraMethods = map[string][]extraMethod{
	"common.Uint256": {
		{name: "IntoUint256", signature: "(dst *uint256.Int)", call: "%BASE%.IntoUint256(dst)"},
		{name: "IsZero", signature: "() bool", call: "return %BASE%.IsZero()"},
	},
}

var (
	uint256Pkg = protogen.GoImportPath("github.com/holiman/uint256")
	slicesPkg  = protogen.GoImportPath("slices")
	bytesPkg   = protogen.GoImportPath("bytes")
)

// valueTypePackages maps a Go package name (as it appears in
// `[(ledger.value_type) = "<pkg>.<Type>"]` proto annotations) to its full
// import path. Add new entries here when introducing a new value-typed wrapper.
var valueTypePackages = map[string]protogen.GoImportPath{
	"commonpb": "github.com/formancehq/ledger/v3/internal/proto/commonpb",
}

// valueTypeExt is the descriptor for the `[(ledger.value_type) = "..."]`
// field option (see misc/proto/ledger_options.proto). We declare it inline
// here rather than importing internal/proto/ledgeroptionspb, because
// protoc-gen-reader is a separate Go module and cannot reach `internal/`.
// The field number MUST match the .proto definition (91234).
var valueTypeExt = &protoimpl.ExtensionInfo{
	ExtendedType:  (*descriptorpb.FieldOptions)(nil),
	ExtensionType: (*string)(nil),
	Field:         91234,
	Name:          "ledger.value_type",
	Tag:           "bytes,91234,opt,name=value_type",
	Filename:      "ledger_options.proto",
}

func init() {
	// Register the extension so proto.HasExtension / GetExtension recognizes
	// the annotation bytes protoc packs into FieldOptions.
	if err := protoregistry.GlobalTypes.RegisterExtension(valueTypeExt); err != nil {
		panic(fmt.Errorf("registering ledger.value_type extension: %w", err))
	}
}

// fieldValueType returns the value_type extension string set on a field
// (e.g., "commonpb.Timestamp"), or "" when the annotation is absent.
func fieldValueType(field *protogen.Field) string {
	opts, ok := field.Desc.Options().(*descriptorpb.FieldOptions)
	if !ok || opts == nil {
		return ""
	}
	if !proto.HasExtension(opts, valueTypeExt) {
		return ""
	}
	v := proto.GetExtension(opts, valueTypeExt)
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// resolveValueType splits a "<pkg>.<Type>" annotation into a protogen.GoIdent
// backed by the mapped import path, so protogen can elide the qualifier when
// the current file lives in the same package.
func resolveValueType(v string) (protogen.GoIdent, bool) {
	dot := strings.LastIndex(v, ".")
	if dot <= 0 || dot == len(v)-1 {
		return protogen.GoIdent{}, false
	}
	pkg := v[:dot]
	name := v[dot+1:]
	imp, ok := valueTypePackages[pkg]
	if !ok {
		return protogen.GoIdent{}, false
	}
	return protogen.GoIdent{GoName: name, GoImportPath: imp}, true
}

// valueTypeMethodName is the Go method name emitted for a field carrying the
// value_type annotation. Suffix "Ts" avoids collision with the exported struct
// field of the same base name (Go forbids method+field sharing a name on a
// struct, and #1511's zero-alloc wrapper is a named type over that struct so
// inherits the same conflict).
func valueTypeMethodName(field *protogen.Field) string {
	return field.GoName + "Ts"
}

func main() {
	protogen.Options{}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}

			generateFile(gen, f)
		}

		return nil
	})
}

func generateFile(gen *protogen.Plugin, file *protogen.File) {
	if len(file.Messages) == 0 {
		return
	}

	g := gen.NewGeneratedFile(
		file.GeneratedFilenamePrefix+"_reader.pb.go",
		file.GoImportPath,
	)
	g.P("// Code generated by protoc-gen-reader. DO NOT EDIT.")
	g.P("// source: ", file.Desc.Path())
	g.P()
	g.P("package ", file.GoPackageName)

	for _, msg := range file.Messages {
		walkMessages(g, msg)
	}
}

func walkMessages(g *protogen.GeneratedFile, msg *protogen.Message) {
	if msg.Desc.IsMapEntry() {
		return
	}

	generateMessage(g, msg)

	for _, nested := range msg.Messages {
		walkMessages(g, nested)
	}
}

func generateMessage(g *protogen.GeneratedFile, msg *protogen.Message) {
	typeName := msg.GoIdent.GoName
	readerName := typeName + "Reader"
	wrapperName := unexport(typeName) + "Readonly"

	// --- Reader interface ---
	g.P()
	g.P("// ", readerName, " provides read-only access to ", typeName, ".")
	g.P("// Call Mutate() to obtain a mutable clone.")
	g.P("type ", readerName, " interface {")

	for _, field := range msg.Fields {
		if field.Oneof != nil && !field.Desc.HasOptionalKeyword() {
			// Oneof fields are accessed via the oneof getter, not individual field getters.
			// We'll handle the oneof getter below.
			continue
		}

		getterName := "Get" + field.GoName
		retType := readerFieldType(g, msg, field)
		g.P("\t", getterName, "() ", retType)
	}

	// Oneof getters (one per oneof group)
	for _, oneof := range msg.Oneofs {
		if oneof.Desc.IsSynthetic() {
			continue // proto3 optional — already handled as regular field
		}

		getterName := "Get" + oneof.GoName
		// The oneof getter returns the unexported interface type from protoc-gen-go.
		// We use the same type to maintain compatibility.
		oneofInterfaceName := fmt.Sprintf("is%s_%s", typeName, oneof.GoName)
		g.P("\t", getterName, "() ", oneofInterfaceName)
	}

	// Extra custom read-only methods
	protoFullName := string(msg.Desc.FullName())
	extras := extraMethods[protoFullName]
	for _, em := range extras {
		if strings.Contains(em.signature, "uint256.Int") {
			_ = g.QualifiedGoIdent(protogen.GoIdent{GoName: "Int", GoImportPath: uint256Pkg})
		}

		g.P("\t", em.name, em.signature)
	}

	// Typed value-type accessors: for each field annotated with the
	// [(ledger.value_type) = "<pkg>.<Type>"] option, emit a `<Field>Ts()`
	// method on the Reader that wraps the raw scalar as a typed value.
	for _, field := range msg.Fields {
		vt := fieldValueType(field)
		if vt == "" {
			continue
		}
		ident, ok := resolveValueType(vt)
		if !ok {
			continue
		}
		g.P("\t", valueTypeMethodName(field), "() ", g.QualifiedGoIdent(ident))
	}

	g.P("\tMutate() *", typeName)
	g.P("}")

	// --- Wrapper type ---
	// Defined as a named type over the concrete message so that AsReader() can
	// convert *typeName -> *wrapperName without allocating a heap wrapper. The
	// resulting *wrapperName fits in the interface data-word, keeping the whole
	// reader-view chain zero-alloc.
	g.P()
	g.P("type ", wrapperName, " ", typeName)

	// base is the receiver→concrete cast used inside every wrapper method.
	base := "(*" + typeName + ")(r)"

	for _, field := range msg.Fields {
		if field.Oneof != nil && !field.Desc.HasOptionalKeyword() {
			continue
		}

		writeWrapperGetter(g, msg, field, wrapperName, base)
	}

	// Oneof getters on wrapper
	for _, oneof := range msg.Oneofs {
		if oneof.Desc.IsSynthetic() {
			continue
		}

		getterName := "Get" + oneof.GoName
		oneofInterfaceName := fmt.Sprintf("is%s_%s", typeName, oneof.GoName)
		g.P()
		g.P("func (r *", wrapperName, ") ", getterName, "() ", oneofInterfaceName, " {")
		g.P("\treturn ", base, ".", getterName, "()")
		g.P("}")
	}

	// Extra custom methods on wrapper
	for _, em := range extras {
		if strings.Contains(em.signature, "uint256.Int") {
			_ = g.QualifiedGoIdent(protogen.GoIdent{GoName: "Int", GoImportPath: uint256Pkg})
		}

		g.P()
		g.P("func (r *", wrapperName, ") ", em.name, em.signature, " {")
		g.P("\t", strings.ReplaceAll(em.call, "%BASE%", base))
		g.P("}")
	}

	// Typed value-type accessors on wrapper — mirror the Reader interface.
	for _, field := range msg.Fields {
		vt := fieldValueType(field)
		if vt == "" {
			continue
		}
		ident, ok := resolveValueType(vt)
		if !ok {
			continue
		}
		methodName := valueTypeMethodName(field)
		qualifiedType := g.QualifiedGoIdent(ident)
		g.P()
		g.P("func (r *", wrapperName, ") ", methodName, "() ", qualifiedType, " {")
		g.P("\treturn ", qualifiedType, "(", base, ".Get", field.GoName, "())")
		g.P("}")
	}

	// Mutate on wrapper
	g.P()
	g.P("func (r *", wrapperName, ") Mutate() *", typeName, " {")
	g.P("\treturn ", base, ".CloneVT()")
	g.P("}")

	// --- Methods on concrete type ---
	g.P()
	g.P("// AsReader returns a read-only view of this ", typeName, ".")
	g.P("func (m *", typeName, ") AsReader() ", readerName, " {")
	g.P("\tif m == nil { return nil }")
	g.P("\treturn (*", wrapperName, ")(m)")
	g.P("}")

	g.P()
	g.P("// Mutate returns a mutable deep clone of this ", typeName, ".")
	g.P("func (m *", typeName, ") Mutate() *", typeName, " {")
	g.P("\treturn m.CloneVT()")
	g.P("}")

	// Typed value-type accessors on the concrete message.
	for _, field := range msg.Fields {
		vt := fieldValueType(field)
		if vt == "" {
			continue
		}
		ident, ok := resolveValueType(vt)
		if !ok {
			continue
		}
		methodName := valueTypeMethodName(field)
		qualifiedType := g.QualifiedGoIdent(ident)
		g.P()
		g.P("// ", methodName, " returns the ", field.GoName, " field wrapped in ", qualifiedType, ".")
		g.P("func (m *", typeName, ") ", methodName, "() ", qualifiedType, " {")
		g.P("\treturn ", qualifiedType, "(m.Get", field.GoName, "())")
		g.P("}")
	}

	// --- ListReader for this message ---
	// Emitted unconditionally so any other package can reference it.
	writeListReader(g, msg)

	// --- MapReader per map field on this message ---
	for _, field := range msg.Fields {
		if !field.Desc.IsMap() {
			continue
		}

		writeMapReader(g, msg, field)
	}
}

// readerFieldType returns the Go type of a field as exposed on the Reader interface.
func readerFieldType(g *protogen.GeneratedFile, owner *protogen.Message, field *protogen.Field) string {
	if field.Desc.IsMap() {
		return qualifiedMapReaderIdent(g, owner, field)
	}

	if field.Desc.IsList() {
		if field.Desc.Kind() == protoreflect.MessageKind || field.Desc.Kind() == protoreflect.GroupKind {
			return qualifiedListReaderIdent(g, field.Message)
		}

		return repeatedScalarType(g, field)
	}

	return singularReturnType(g, field, true)
}

// writeWrapperGetter emits the wrapper method for one field, applying the
// immutability rules described at the top of the file. `base` is the expression
// used inside method bodies to reach the concrete message (e.g., "(*Foo)(r)").
func writeWrapperGetter(g *protogen.GeneratedFile, owner *protogen.Message, field *protogen.Field, wrapperName string, base string) {
	getterName := "Get" + field.GoName

	switch {
	case field.Desc.IsMap():
		retType := qualifiedMapReaderIdent(g, owner, field)
		mapImpl := mapReadonlyName(owner, field)

		g.P()
		g.P("func (r *", wrapperName, ") ", getterName, "() ", retType, " {")
		g.P("\treturn ", mapImpl, "(", base, ".", getterName, "())")
		g.P("}")

	case field.Desc.IsList():
		kind := field.Desc.Kind()

		switch kind {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			retType := qualifiedListReaderIdent(g, field.Message)
			ctor := g.QualifiedGoIdent(protogen.GoIdent{
				GoName:       listReaderCtorName(field.Message),
				GoImportPath: field.Message.GoIdent.GoImportPath,
			})

			g.P()
			g.P("func (r *", wrapperName, ") ", getterName, "() ", retType, " {")
			g.P("\treturn ", ctor, "(", base, ".", getterName, "())")
			g.P("}")

		case protoreflect.BytesKind:
			// [][]byte — deep clone.
			g.P()
			g.P("func (r *", wrapperName, ") ", getterName, "() [][]byte {")
			g.P("\tsrc := ", base, ".", getterName, "()")
			g.P("\tif src == nil { return nil }")
			g.P("\tout := make([][]byte, len(src))")
			g.P("\tfor i, b := range src {")
			g.P("\t\tout[i] = ", g.QualifiedGoIdent(protogen.GoIdent{GoName: "Clone", GoImportPath: bytesPkg}), "(b)")
			g.P("\t}")
			g.P("\treturn out")
			g.P("}")

		default:
			// Repeated scalar — clone the slice.
			retType := repeatedScalarType(g, field)

			g.P()
			g.P("func (r *", wrapperName, ") ", getterName, "() ", retType, " {")
			g.P("\treturn ", g.QualifiedGoIdent(protogen.GoIdent{GoName: "Clone", GoImportPath: slicesPkg}), "(", base, ".", getterName, "())")
			g.P("}")
		}

	case field.Desc.Kind() == protoreflect.MessageKind || field.Desc.Kind() == protoreflect.GroupKind:
		retType := singularReturnType(g, field, true)

		g.P()
		g.P("func (r *", wrapperName, ") ", getterName, "() ", retType, " {")
		g.P("\tv := ", base, ".", getterName, "()")
		g.P("\tif v == nil { return nil }")
		g.P("\treturn v.AsReader()")
		g.P("}")

	case field.Desc.Kind() == protoreflect.BytesKind:
		// Singular []byte — defensive clone.
		g.P()
		g.P("func (r *", wrapperName, ") ", getterName, "() []byte {")
		g.P("\treturn ", g.QualifiedGoIdent(protogen.GoIdent{GoName: "Clone", GoImportPath: bytesPkg}), "(", base, ".", getterName, "())")
		g.P("}")

	default:
		// Scalar — return by value.
		retType := singularReturnType(g, field, true)

		g.P()
		g.P("func (r *", wrapperName, ") ", getterName, "() ", retType, " {")
		g.P("\treturn ", base, ".", getterName, "()")
		g.P("}")
	}
}

// writeListReader emits a <Message>ListReader interface and its implementation
// as a defined type over []*<Message>. Callers can iterate but cannot replace
// elements or mutate the underlying slice header of the source message.
func writeListReader(g *protogen.GeneratedFile, msg *protogen.Message) {
	typeName := msg.GoIdent.GoName
	readerName := typeName + "Reader"
	listReaderName := typeName + "ListReader"
	listImplName := listReadonlyName(msg)

	g.P()
	g.P("// ", listReaderName, " provides read-only iteration over []*", typeName, ".")
	g.P("type ", listReaderName, " interface {")
	g.P("\tLen() int")
	g.P("\tGet(i int) ", readerName)
	g.P("\tRange(yield func(int, ", readerName, ") bool)")
	g.P("}")

	g.P()
	g.P("type ", listImplName, " []*", typeName)

	g.P()
	g.P("func (l ", listImplName, ") Len() int { return len(l) }")

	g.P()
	g.P("func (l ", listImplName, ") Get(i int) ", readerName, " {")
	g.P("\tv := l[i]")
	g.P("\tif v == nil { return nil }")
	g.P("\treturn v.AsReader()")
	g.P("}")

	g.P()
	g.P("func (l ", listImplName, ") Range(yield func(int, ", readerName, ") bool) {")
	g.P("\tfor i, v := range l {")
	g.P("\t\tvar r ", readerName)
	g.P("\t\tif v != nil { r = v.AsReader() }")
	g.P("\t\tif !yield(i, r) { return }")
	g.P("\t}")
	g.P("}")

	g.P()
	g.P("// ", listReaderCtorName(msg), " wraps s for read-only iteration. The returned")
	g.P("// view aliases the underlying slice; do not mutate s afterwards.")
	g.P("func ", listReaderCtorName(msg), "(s []*", typeName, ") ", listReaderName, " { return ", listImplName, "(s) }")
}

// writeMapReader emits a <Outer>_<Field>MapReader interface and its
// implementation as a defined type over the underlying map type.
func writeMapReader(g *protogen.GeneratedFile, owner *protogen.Message, field *protogen.Field) {
	keyField := field.Message.Fields[0]
	valField := field.Message.Fields[1]

	keyType := singularReturnType(g, keyField, false)
	concreteValType := singularReturnType(g, valField, false)
	readerValType := singularReturnType(g, valField, true)

	mapReaderName := mapReaderInterfaceName(owner, field)
	mapImplName := mapReadonlyName(owner, field)

	valueIsMessage := valField.Desc.Kind() == protoreflect.MessageKind || valField.Desc.Kind() == protoreflect.GroupKind

	g.P()
	g.P("// ", mapReaderName, " provides read-only access to ", owner.GoIdent.GoName, ".", field.GoName, ".")
	g.P("type ", mapReaderName, " interface {")
	g.P("\tLen() int")
	g.P("\tGet(k ", keyType, ") (", readerValType, ", bool)")
	g.P("\tRange(yield func(", keyType, ", ", readerValType, ") bool)")
	g.P("}")

	g.P()
	g.P("type ", mapImplName, " map[", keyType, "]", concreteValType)

	g.P()
	g.P("func (m ", mapImplName, ") Len() int { return len(m) }")

	g.P()
	g.P("func (m ", mapImplName, ") Get(k ", keyType, ") (", readerValType, ", bool) {")
	g.P("\tv, ok := m[k]")

	if valueIsMessage {
		g.P("\tif !ok || v == nil { return nil, ok }")
		g.P("\treturn v.AsReader(), true")
	} else {
		g.P("\treturn v, ok")
	}

	g.P("}")

	g.P()
	g.P("func (m ", mapImplName, ") Range(yield func(", keyType, ", ", readerValType, ") bool) {")
	g.P("\tfor k, v := range m {")

	if valueIsMessage {
		g.P("\t\tvar r ", readerValType)
		g.P("\t\tif v != nil { r = v.AsReader() }")
		g.P("\t\tif !yield(k, r) { return }")
	} else {
		g.P("\t\tif !yield(k, v) { return }")
	}

	g.P("\t}")
	g.P("}")
}

// singularReturnType returns the Go return type for a singular field's getter.
// If reader is true and the field is a singular message, returns the Reader
// interface type. Otherwise returns the concrete type matching the
// protoc-gen-go getter signature.
func singularReturnType(g *protogen.GeneratedFile, field *protogen.Field, reader bool) string {
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "[]byte"
	case protoreflect.EnumKind:
		return g.QualifiedGoIdent(field.Enum.GoIdent)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if reader {
			return qualifiedReaderIdent(g, field.Message)
		}

		return "*" + g.QualifiedGoIdent(field.Message.GoIdent)
	default:
		return "interface{}"
	}
}

// repeatedScalarType returns the Go type for a repeated scalar field (i.e.
// for kinds other than message/group, which have their own list-reader path).
func repeatedScalarType(g *protogen.GeneratedFile, field *protogen.Field) string {
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		return "[]bool"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "[]int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "[]int64"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "[]uint32"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "[]uint64"
	case protoreflect.FloatKind:
		return "[]float32"
	case protoreflect.DoubleKind:
		return "[]float64"
	case protoreflect.StringKind:
		return "[]string"
	case protoreflect.BytesKind:
		return "[][]byte"
	case protoreflect.EnumKind:
		return "[]" + g.QualifiedGoIdent(field.Enum.GoIdent)
	default:
		return "[]interface{}"
	}
}

// qualifiedReaderIdent returns the Reader interface name for a message,
// qualified with package import if the message is in a different package.
func qualifiedReaderIdent(g *protogen.GeneratedFile, msg *protogen.Message) string {
	readerIdent := protogen.GoIdent{
		GoName:       msg.GoIdent.GoName + "Reader",
		GoImportPath: msg.GoIdent.GoImportPath,
	}

	return g.QualifiedGoIdent(readerIdent)
}

// qualifiedListReaderIdent returns the <Message>ListReader name qualified with
// the message's package import.
func qualifiedListReaderIdent(g *protogen.GeneratedFile, msg *protogen.Message) string {
	ident := protogen.GoIdent{
		GoName:       msg.GoIdent.GoName + "ListReader",
		GoImportPath: msg.GoIdent.GoImportPath,
	}

	return g.QualifiedGoIdent(ident)
}

// qualifiedMapReaderIdent returns the per-field MapReader name. The MapReader
// is emitted in the owner's package, so cross-package qualification happens
// only when the field's owner lives in a different file.
func qualifiedMapReaderIdent(g *protogen.GeneratedFile, owner *protogen.Message, field *protogen.Field) string {
	ident := protogen.GoIdent{
		GoName:       mapReaderInterfaceName(owner, field),
		GoImportPath: owner.GoIdent.GoImportPath,
	}

	return g.QualifiedGoIdent(ident)
}

func listReadonlyName(msg *protogen.Message) string {
	return unexport(msg.GoIdent.GoName) + "ListReadonly"
}

func listReaderCtorName(msg *protogen.Message) string {
	return "New" + msg.GoIdent.GoName + "ListReader"
}

func mapReaderInterfaceName(owner *protogen.Message, field *protogen.Field) string {
	return owner.GoIdent.GoName + "_" + field.GoName + "MapReader"
}

func mapReadonlyName(owner *protogen.Message, field *protogen.Field) string {
	return unexport(owner.GoIdent.GoName) + "_" + unexport(field.GoName) + "MapReadonly"
}

func unexport(s string) string {
	if s == "" {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToLower(r[0])

	return string(r)
}
