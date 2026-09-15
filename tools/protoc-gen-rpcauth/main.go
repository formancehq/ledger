// protoc-gen-rpcauth validates and generates the public gRPC authentication
// policy registry declared through common.auth_policy method options.
package main

import (
	"fmt"
	"slices"
	"strconv"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/pluginpb"
)

const authPolicyExtension = "common.auth_policy"

var policyServices = map[protoreflect.FullName]struct{}{
	"ledger.BucketService":   {},
	"cluster.ClusterService": {},
}

type policyKind uint8

const (
	policyPublic policyKind = iota + 1
	policyFixedScope
	policyDynamicResolver
)

type methodPolicy struct {
	fullMethod string
	kind       policyKind
	value      string
}

func main() {
	protogen.Options{}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		extDesc := findExtension(gen, authPolicyExtension)
		if extDesc == nil {
			return fmt.Errorf("extension %q not found in compiled protos", authPolicyExtension)
		}

		extType := dynamicpb.NewExtensionType(extDesc)
		resolver := &protoregistry.Types{}
		if err := resolver.RegisterExtension(extType); err != nil {
			return fmt.Errorf("registering %q extension type: %w", authPolicyExtension, err)
		}

		policies, err := collectPolicies(gen, extType, resolver)
		if err != nil {
			return err
		}

		targetFile := fileByPath(gen, extDesc.ParentFile().Path())
		if targetFile == nil {
			return fmt.Errorf("could not locate the generated file declaring %q", authPolicyExtension)
		}

		generate(gen, targetFile, policies)

		return nil
	})
}

func findExtension(gen *protogen.Plugin, fullName string) protoreflect.ExtensionDescriptor {
	for _, file := range gen.Files {
		for _, extension := range file.Extensions {
			if string(extension.Desc.FullName()) == fullName {
				return extension.Desc
			}
		}
	}

	return nil
}

func fileByPath(gen *protogen.Plugin, path string) *protogen.File {
	for _, file := range gen.Files {
		if file.Desc.Path() == path {
			return file
		}
	}

	return nil
}

func collectPolicies(gen *protogen.Plugin, extType protoreflect.ExtensionType, resolver *protoregistry.Types) ([]methodPolicy, error) {
	policies := make([]methodPolicy, 0)
	foundServices := make(map[protoreflect.FullName]struct{}, len(policyServices))

	for _, file := range gen.Files {
		for _, service := range file.Services {
			if _, ok := policyServices[service.Desc.FullName()]; !ok {
				continue
			}
			foundServices[service.Desc.FullName()] = struct{}{}

			for _, method := range service.Methods {
				policy, err := readMethodPolicy(method, extType, resolver)
				if err != nil {
					return nil, err
				}

				policies = append(policies, policy)
			}
		}
	}

	missingServices := make([]string, 0, len(policyServices))
	for service := range policyServices {
		if _, ok := foundServices[service]; !ok {
			missingServices = append(missingServices, string(service))
		}
	}
	if len(missingServices) > 0 {
		slices.Sort(missingServices)

		return nil, fmt.Errorf("Ledger public service %q not found", missingServices[0])
	}

	return policies, nil
}

func readMethodPolicy(method *protogen.Method, extType protoreflect.ExtensionType, resolver *protoregistry.Types) (methodPolicy, error) {
	fullName := method.Desc.Parent().FullName()
	fullMethod := "/" + string(fullName) + "/" + string(method.Desc.Name())
	opts := method.Desc.Options()
	if opts == nil {
		return methodPolicy{}, fmt.Errorf("RPC %q has no auth policy", fullMethod)
	}

	raw, err := proto.Marshal(opts)
	if err != nil {
		return methodPolicy{}, fmt.Errorf("marshaling options for RPC %q: %w", fullMethod, err)
	}

	resolved := dynamicpb.NewMessage(opts.ProtoReflect().Descriptor())
	if err := (proto.UnmarshalOptions{Resolver: resolver}).Unmarshal(raw, resolved); err != nil {
		return methodPolicy{}, fmt.Errorf("re-parsing options for RPC %q: %w", fullMethod, err)
	}

	extDesc := extType.TypeDescriptor()
	if !resolved.Has(extDesc) {
		return methodPolicy{}, fmt.Errorf("RPC %q has no auth policy", fullMethod)
	}

	policy := resolved.Get(extDesc).Message()
	oneof := policy.Descriptor().Oneofs().ByName("policy")
	if oneof == nil {
		return methodPolicy{}, fmt.Errorf("auth policy for RPC %q has no policy oneof", fullMethod)
	}

	field := policy.WhichOneof(oneof)
	if field == nil {
		return methodPolicy{}, fmt.Errorf("RPC %q has no auth policy", fullMethod)
	}

	switch field.Name() {
	case "public":
		if !policy.Get(field).Bool() {
			return methodPolicy{}, fmt.Errorf("RPC %q must declare public=true", fullMethod)
		}

		return methodPolicy{fullMethod: fullMethod, kind: policyPublic}, nil
	case "fixed_scope":
		return enumPolicy(fullMethod, policy, field, policyFixedScope, "fixed scope")
	case "dynamic_resolver":
		return enumPolicy(fullMethod, policy, field, policyDynamicResolver, "dynamic resolver")
	default:
		return methodPolicy{}, fmt.Errorf("RPC %q uses unknown auth policy field %q", fullMethod, field.Name())
	}
}

func enumPolicy(fullMethod string, policy protoreflect.Message, field protoreflect.FieldDescriptor, kind policyKind, label string) (methodPolicy, error) {
	number := policy.Get(field).Enum()
	if number == 0 {
		return methodPolicy{}, fmt.Errorf("RPC %q declares an unspecified %s", fullMethod, label)
	}

	value := field.Enum().Values().ByNumber(number)
	if value == nil {
		return methodPolicy{}, fmt.Errorf("RPC %q declares unknown %s number %d", fullMethod, label, number)
	}

	return methodPolicy{
		fullMethod: fullMethod,
		kind:       kind,
		value:      string(value.Parent().Name()) + "_" + string(value.Name()),
	}, nil
}

func generate(gen *protogen.Plugin, file *protogen.File, policies []methodPolicy) {
	g := gen.NewGeneratedFile(file.GeneratedFilenamePrefix+"_rpc_auth_policy.pb.go", file.GoImportPath)

	g.P("// Code generated by protoc-gen-rpcauth. DO NOT EDIT.")
	g.P("// source: ", file.Desc.Path())
	g.P("//")
	g.P("// Authentication policies are derived from common.auth_policy method")
	g.P("// options on Ledger's public gRPC services. Edit the proto declarations")
	g.P("// and re-run `just generate-proto`, never this file.")
	g.P()
	g.P("package ", file.GoPackageName)
	g.P()
	g.P("// UnknownRPCMethodError reports a method outside the generated fail-closed policy registry.")
	g.P("type UnknownRPCMethodError struct {")
	g.P("FullMethod string")
	g.P("}")
	g.P()
	g.P("func (e *UnknownRPCMethodError) Error() string {")
	g.P("return ", strconv.Quote("no RPC authentication policy for "), " + e.FullMethod")
	g.P("}")
	g.P()
	g.P("// RPCAuthPolicyForMethod returns a fresh policy value for a known full gRPC method name.")
	g.P("// Unknown methods fail closed with UnknownRPCMethodError.")
	g.P("func RPCAuthPolicyForMethod(fullMethod string) (*MethodAuthPolicy, error) {")
	g.P("switch fullMethod {")
	for _, policy := range policies {
		g.P("case ", strconv.Quote(policy.fullMethod), ":")
		emitPolicy(g, policy)
	}
	g.P("default:")
	g.P("return nil, &UnknownRPCMethodError{FullMethod: fullMethod}")
	g.P("}")
	g.P("}")
	g.P()
	g.P("// AllRPCAuthPolicies returns every generated method policy keyed by full method name.")
	g.P("func AllRPCAuthPolicies() map[string]*MethodAuthPolicy {")
	g.P("return map[string]*MethodAuthPolicy{")
	for _, policy := range policies {
		g.P(strconv.Quote(policy.fullMethod), ": ", policyLiteral(policy), ",")
	}
	g.P("}")
	g.P("}")
}

func emitPolicy(g *protogen.GeneratedFile, policy methodPolicy) {
	g.P("return ", policyLiteral(policy), ", nil")
}

func policyLiteral(policy methodPolicy) string {
	switch policy.kind {
	case policyPublic:
		return "&MethodAuthPolicy{Policy: &MethodAuthPolicy_Public{Public: true}}"
	case policyFixedScope:
		return "&MethodAuthPolicy{Policy: &MethodAuthPolicy_FixedScope{FixedScope: " + policy.value + "}}"
	case policyDynamicResolver:
		return "&MethodAuthPolicy{Policy: &MethodAuthPolicy_DynamicResolver{DynamicResolver: " + policy.value + "}}"
	default:
		panic("unknown policy kind")
	}
}
