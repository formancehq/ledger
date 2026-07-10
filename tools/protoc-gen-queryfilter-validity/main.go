// protoc-gen-queryfilter-validity generates the single source of truth for
// per-target QueryFilter condition validity (EN-1504).
//
// It reads the `common.QueryFilter` message, iterates the arms of its `filter`
// oneof, and for each arm reads the `common.allowed_query_targets` field-option
// extension declaring which `common.QueryTarget` values the condition is valid
// on. From those annotations it emits query_target_validity.pb.go into the
// commonpb package, containing:
//
//   - a ConditionKind enum (one constant per oneof arm, plus a zero Unknown),
//   - ConditionKindOf(*QueryFilter) mapping a filter node to its kind,
//   - the ConditionValidForTarget(target, kind) table,
//   - TargetHumanName(target),
//   - and the enumerations (allConditionKinds, allQueryTargets) the
//     completeness test iterates.
//
// Both the compile layer (internal/query) and the REST decode layer
// (internal/adapter/http) consume the generated table, so they cannot drift.
// An arm with no annotation maps to "valid on no target" — the fail-safe
// default: a forgotten annotation rejects the condition everywhere (loud)
// rather than silently widening results.
//
// Install:  go build -o protoc-gen-queryfilter-validity .
// Usage:    protoc --queryfilter-validity_out=. --queryfilter-validity_opt=module=<module> ...
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
	// queryFilterMessage is the fully-qualified name of the oneof-bearing message.
	queryFilterMessage = "common.QueryFilter"
	// queryFilterOneof is the oneof whose arms are the condition kinds.
	queryFilterOneof = "filter"
	// queryTargetEnum is the fully-qualified name of the target enum.
	queryTargetEnum = "common.QueryTarget"
	// allowedTargetsExtension is the fully-qualified name of the field-option
	// extension declaring per-arm target validity.
	allowedTargetsExtension = "common.allowed_query_targets"
	// noneMarkerExtension is the fully-qualified name of the boolean field-option
	// extension declaring an arm is intentionally valid on no target. It is the
	// explicit opt-in the generator requires instead of inferring "valid
	// nowhere" from a missing annotation.
	noneMarkerExtension = "common.valid_on_no_query_target"
)

// armValidity captures one QueryFilter oneof arm and the targets it is valid on.
type armValidity struct {
	// kind is the generated ConditionKind constant name (e.g. "ConditionKindField").
	kind string
	// goOneofWrapper is the generated Go oneof wrapper type name
	// (e.g. "QueryFilter_Field") used by ConditionKindOf's type switch.
	goOneofWrapper string
	// allowed is the ordered set of allowed QueryTarget Go constant names.
	allowed []string
}

func main() {
	protogen.Options{}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		// Locate the extension descriptor (top-level extension in some compiled
		// file), the QueryFilter message, and the QueryTarget enum across all
		// files in the request (they all live in common.proto today, but resolve
		// generically so a future split does not break the plugin).
		extDesc := findExtension(gen, allowedTargetsExtension)
		if extDesc == nil {
			return fmt.Errorf("extension %q not found in compiled protos", allowedTargetsExtension)
		}

		noneDesc := findExtension(gen, noneMarkerExtension)
		if noneDesc == nil {
			return fmt.Errorf("extension %q not found in compiled protos", noneMarkerExtension)
		}

		// The extensions are declared in the compiled protos but not present in
		// the global registry, so options messages carry their values as
		// unresolved unknown fields. Register dynamic ExtensionTypes in a local
		// resolver; readArmAnnotation re-parses each FieldOptions through it so
		// the values can be decoded.
		extType := dynamicpb.NewExtensionType(extDesc)
		noneType := dynamicpb.NewExtensionType(noneDesc)
		resolver := &protoregistry.Types{}
		if err := resolver.RegisterExtension(extType); err != nil {
			return fmt.Errorf("registering %q extension type: %w", allowedTargetsExtension, err)
		}
		if err := resolver.RegisterExtension(noneType); err != nil {
			return fmt.Errorf("registering %q extension type: %w", noneMarkerExtension, err)
		}

		msg := findMessage(gen, queryFilterMessage)
		if msg == nil {
			return fmt.Errorf("message %q not found in compiled protos", queryFilterMessage)
		}

		enum := findEnum(gen, queryTargetEnum)
		if enum == nil {
			return fmt.Errorf("enum %q not found in compiled protos", queryTargetEnum)
		}

		// The file we generate into is the one that declares QueryFilter.
		targetFile := fileOf(gen, msg)
		if targetFile == nil {
			return fmt.Errorf("could not locate the generated file for %q", queryFilterMessage)
		}

		arms, err := collectArms(msg, extType, noneType, resolver)
		if err != nil {
			return err
		}

		targets := collectTargets(enum)

		generate(gen, targetFile, arms, targets)

		return nil
	})
}

// findExtension returns the descriptor for the named top-level extension.
func findExtension(gen *protogen.Plugin, fullName string) protoreflect.ExtensionDescriptor {
	for _, f := range gen.Files {
		for _, ext := range f.Extensions {
			if string(ext.Desc.FullName()) == fullName {
				return ext.Desc
			}
		}
	}

	return nil
}

// findMessage returns the protogen message with the given full name.
func findMessage(gen *protogen.Plugin, fullName string) *protogen.Message {
	for _, f := range gen.Files {
		for _, m := range f.Messages {
			if string(m.Desc.FullName()) == fullName {
				return m
			}
		}
	}

	return nil
}

// findEnum returns the protogen enum with the given full name.
func findEnum(gen *protogen.Plugin, fullName string) *protogen.Enum {
	for _, f := range gen.Files {
		for _, e := range f.Enums {
			if string(e.Desc.FullName()) == fullName {
				return e
			}
		}
	}

	return nil
}

// fileOf returns the protogen file that declares the given message.
func fileOf(gen *protogen.Plugin, msg *protogen.Message) *protogen.File {
	for _, f := range gen.Files {
		if slices.Contains(f.Messages, msg) {
			return f
		}
	}

	return nil
}

// collectArms reads every arm of the QueryFilter.filter oneof and its declared
// allowed targets from the extensions. The arm order follows the proto field
// declaration order, giving a stable, source-faithful generated file.
//
// Every arm MUST carry an explicit validity declaration — a non-empty
// allowed_query_targets OR valid_on_no_query_target = true. An arm with neither
// is a build error: it would otherwise become a distinct ConditionKind with an
// all-false row, silently swallowing a forgotten annotation and defeating the
// anti-drift gate (flemzord review, #1561). An arm declaring both is also
// rejected as contradictory.
func collectArms(msg *protogen.Message, extType, noneType protoreflect.ExtensionType, resolver *protoregistry.Types) ([]armValidity, error) {
	var oneof *protogen.Oneof
	for _, oo := range msg.Oneofs {
		if string(oo.Desc.Name()) == queryFilterOneof {
			oneof = oo

			break
		}
	}

	if oneof == nil {
		return nil, fmt.Errorf("oneof %q not found on %q", queryFilterOneof, queryFilterMessage)
	}

	arms := make([]armValidity, 0, len(oneof.Fields))
	for _, field := range oneof.Fields {
		allowed, validOnNone, err := readArmAnnotation(field, extType, noneType, resolver)
		if err != nil {
			return nil, err
		}

		switch {
		case len(allowed) > 0 && validOnNone:
			return nil, fmt.Errorf(
				"QueryFilter oneof arm %q declares both allowed_query_targets and valid_on_no_query_target=true; "+
					"they are contradictory — declare exactly one",
				field.Desc.Name())
		case len(allowed) == 0 && !validOnNone:
			return nil, fmt.Errorf(
				"QueryFilter oneof arm %q has no target-validity annotation: add one or more "+
					"[(common.allowed_query_targets) = QUERY_TARGET_...], or "+
					"[(common.valid_on_no_query_target) = true] if it is deliberately valid on no target",
				field.Desc.Name())
		}

		arms = append(arms, armValidity{
			kind:           "ConditionKind" + field.GoName,
			goOneofWrapper: field.GoIdent.GoName,
			allowed:        allowed,
		})
	}

	return arms, nil
}

// readArmAnnotation reads the two field-option extensions on a oneof-arm field:
// the repeated allowed_query_targets (returned as Go constant names) and the
// boolean valid_on_no_query_target marker. Presence/consistency of the pair is
// enforced by the caller.
//
// The extensions are declared in the compiled FileDescriptorSet but not in the
// global registry, so protogen leaves their values as unresolved unknown fields
// on the options message. We round-trip the FieldOptions bytes through a
// resolver that knows the extensions, then read them via reflection.
func readArmAnnotation(field *protogen.Field, extType, noneType protoreflect.ExtensionType, resolver *protoregistry.Types) (allowed []string, validOnNone bool, err error) {
	opts := field.Desc.Options()
	if opts == nil {
		return nil, false, nil
	}

	raw, err := proto.Marshal(opts)
	if err != nil {
		return nil, false, fmt.Errorf("marshaling options for %q: %w", field.Desc.FullName(), err)
	}

	resolved := dynamicpb.NewMessage(opts.ProtoReflect().Descriptor())
	if err := (proto.UnmarshalOptions{Resolver: resolver}).Unmarshal(raw, resolved); err != nil {
		return nil, false, fmt.Errorf("re-parsing options for %q: %w", field.Desc.FullName(), err)
	}

	noneDesc := noneType.TypeDescriptor()
	if resolved.Has(noneDesc) {
		validOnNone = resolved.Get(noneDesc).Bool()
	}

	typeDesc := extType.TypeDescriptor()
	if !resolved.Has(typeDesc) {
		return nil, validOnNone, nil
	}

	list := resolved.Get(typeDesc).List()

	enumValues := typeDesc.Enum().Values()

	allowed = make([]string, 0, list.Len())
	for i := range list.Len() {
		enumNum := list.Get(i).Enum()
		enumVal := enumValues.ByNumber(enumNum)
		if enumVal == nil {
			return nil, false, fmt.Errorf("field %q references unknown QueryTarget number %d",
				field.Desc.FullName(), enumNum)
		}

		allowed = append(allowed, goEnumConst(enumVal))
	}

	return allowed, validOnNone, nil
}

// targetInfo describes one QueryTarget enum value for the generated table.
type targetInfo struct {
	goConst string // Go constant name, e.g. QueryTarget_QUERY_TARGET_ACCOUNTS
	human   string // human-readable label, e.g. "accounts"
}

// collectTargets returns every QueryTarget value in declaration order.
func collectTargets(enum *protogen.Enum) []targetInfo {
	targets := make([]targetInfo, 0, len(enum.Values))
	for _, v := range enum.Values {
		targets = append(targets, targetInfo{
			goConst: goEnumConst(v.Desc),
			human:   humanTargetName(string(v.Desc.Name())),
		})
	}

	return targets
}

// goEnumConst builds the generated Go constant name for an enum value:
// "<EnumGoName>_<VALUE_NAME>" — matching protoc-gen-go's convention for a
// package-scoped enum.
func goEnumConst(v protoreflect.EnumValueDescriptor) string {
	return string(v.Parent().Name()) + "_" + string(v.Name())
}

// humanTargetName derives a lowercase, prefix-stripped label from an enum value
// name, e.g. QUERY_TARGET_ACCOUNTS -> "accounts".
func humanTargetName(name string) string {
	const prefix = "QUERY_TARGET_"
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		name = name[len(prefix):]
	}

	return lower(name)
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}

	return string(b)
}

func generate(gen *protogen.Plugin, file *protogen.File, arms []armValidity, targets []targetInfo) {
	g := gen.NewGeneratedFile(
		file.GeneratedFilenamePrefix+"_queryfilter_validity.pb.go",
		file.GoImportPath,
	)
	fmtPkg := protogen.GoImportPath("fmt")

	g.P("// Code generated by protoc-gen-queryfilter-validity. DO NOT EDIT.")
	g.P("// source: ", file.Desc.Path())
	g.P("//")
	g.P("// Single source of truth for per-target QueryFilter condition validity")
	g.P("// (EN-1504). Derived from the common.allowed_query_targets field-option")
	g.P("// annotations on the QueryFilter.filter oneof arms. Edit the annotations in")
	g.P("// the .proto and re-run `just generate-proto`, never this file.")
	g.P()
	g.P("package ", file.GoPackageName)
	g.P()

	// --- ConditionKind enum ---
	g.P("// ConditionKind enumerates the arms of the QueryFilter.filter oneof at the")
	g.P("// granularity per-target validity is decided. The zero value")
	g.P("// ConditionKindUnknown never corresponds to a real arm and is valid on no")
	g.P("// target, so a nil / empty / unmapped filter is rejected everywhere.")
	g.P("type ConditionKind int")
	g.P()
	g.P("const (")
	g.P("ConditionKindUnknown ConditionKind = iota")
	for _, a := range arms {
		g.P(a.kind)
	}
	g.P(")")
	g.P()

	// --- names ---
	g.P("// conditionKindNames gives each kind a human-readable label used in")
	g.P("// validation error messages.")
	g.P("var conditionKindNames = map[ConditionKind]string{")
	g.P("ConditionKindUnknown: ", quote("unknown"), ",")
	for _, a := range arms {
		g.P(a.kind, ": ", quote(kindLabel(a.kind)), ",")
	}
	g.P("}")
	g.P()

	// --- allConditionKinds ---
	g.P("// allConditionKinds lists every real ConditionKind (excluding Unknown), in")
	g.P("// proto declaration order. The completeness test iterates it.")
	g.P("var allConditionKinds = []ConditionKind{")
	for _, a := range arms {
		g.P(a.kind, ",")
	}
	g.P("}")
	g.P()

	// --- String ---
	g.P("// String returns the human-readable label for the kind.")
	g.P("func (k ConditionKind) String() string {")
	g.P("if name, ok := conditionKindNames[k]; ok {")
	g.P("return name")
	g.P("}")
	g.P()
	g.P("return ", fmtPkg.Ident("Sprintf"), "(", quote("ConditionKind(%d)"), ", int(k))")
	g.P("}")
	g.P()

	// --- ConditionKindOf ---
	g.P("// ConditionKindOf maps a QueryFilter node to its ConditionKind. A nil filter")
	g.P("// or an unmapped arm returns ConditionKindUnknown (valid on no target).")
	g.P("func ConditionKindOf(f *QueryFilter) ConditionKind {")
	g.P("if f == nil {")
	g.P("return ConditionKindUnknown")
	g.P("}")
	g.P()
	g.P("switch f.GetFilter().(type) {")
	for _, a := range arms {
		g.P("case *", a.goOneofWrapper, ":")
		g.P("return ", a.kind)
	}
	g.P("default:")
	g.P("return ConditionKindUnknown")
	g.P("}")
	g.P("}")
	g.P()

	// --- allQueryTargets ---
	g.P("// allQueryTargets lists every QueryTarget in declaration order. The")
	g.P("// completeness test iterates it to assert every (target, kind) pair is")
	g.P("// explicitly decided by the table.")
	g.P("var allQueryTargets = []QueryTarget{")
	for _, t := range targets {
		g.P(t.goConst, ",")
	}
	g.P("}")
	g.P()

	// --- validity table ---
	g.P("// targetConditionValidity is the generated per-target validity table. Every")
	g.P("// (target, kind) pair is present with an explicit verdict — a missing pair")
	g.P("// is impossible by construction (the generator emits the full cross-product).")
	g.P("var targetConditionValidity = map[QueryTarget]map[ConditionKind]bool{")
	for _, t := range targets {
		g.P(t.goConst, ": {")
		for _, a := range arms {
			g.P(a.kind, ": ", validFor(a, t.goConst), ",")
		}
		g.P("},")
	}
	g.P("}")
	g.P()

	// --- ConditionValidForTarget ---
	g.P("// ConditionValidForTarget reports whether the ConditionKind is valid on the")
	g.P("// QueryTarget, per the single source of truth. An unknown kind or target is")
	g.P("// never valid.")
	g.P("func ConditionValidForTarget(target QueryTarget, kind ConditionKind) bool {")
	g.P("byKind, ok := targetConditionValidity[target]")
	g.P("if !ok {")
	g.P("return false")
	g.P("}")
	g.P()
	g.P("return byKind[kind]")
	g.P("}")
	g.P()

	// --- TargetHumanName ---
	g.P("// TargetHumanName returns a human-readable name for a query target, used in")
	g.P("// uniform validation error messages.")
	g.P("func TargetHumanName(target QueryTarget) string {")
	g.P("switch target {")
	for _, t := range targets {
		g.P("case ", t.goConst, ":")
		g.P("return ", quote(t.human))
	}
	g.P("default:")
	g.P("return ", fmtPkg.Ident("Sprintf"), "(", quote("QueryTarget(%d)"), ", int(target))")
	g.P("}")
	g.P("}")
}

// validFor returns the Go bool literal for whether arm a is valid on the given
// QueryTarget Go constant.
func validFor(a armValidity, targetConst string) string {
	if slices.Contains(a.allowed, targetConst) {
		return "true"
	}

	return "false"
}

// kindLabel returns a stable human-readable label for a ConditionKind constant
// name, used in error messages. It maps the small closed set of arms to the
// same wording the previous hand-written guards used, keeping error messages
// familiar; unknown future arms fall back to a lowercased constant tail.
func kindLabel(kind string) string {
	if label, ok := kindLabels[kind]; ok {
		return label
	}

	// Fall back to the constant tail (after "ConditionKind"), lowercased.
	return lower(kind[len("ConditionKind"):])
}

var kindLabels = map[string]string{
	"ConditionKindField":           "metadata",
	"ConditionKindAddress":         "address",
	"ConditionKindAnd":             "and",
	"ConditionKindOr":              "or",
	"ConditionKindNot":             "not",
	"ConditionKindReference":       "reference",
	"ConditionKindBuiltinUint":     "builtin field (id/timestamp/insertedAt/revertedAt)",
	"ConditionKindLedger":          "ledger",
	"ConditionKindLogId":           "logId",
	"ConditionKindLogBuiltinUint":  "log field (date)",
	"ConditionKindAccountHasAsset": "accountHasAsset",
	"ConditionKindReverted":        "reverted",
	"ConditionKindAudit":           "audit",
}

func quote(s string) string {
	return fmt.Sprintf("%q", s)
}
