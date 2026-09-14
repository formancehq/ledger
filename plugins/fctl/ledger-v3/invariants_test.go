package ledgerv3

import (
	"context"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/adapter/auth"
)

func TestComponentGuestDependencyClosureExcludesGRPCGo(t *testing.T) {
	t.Parallel()

	command := exec.Command("go", "list", "-deps", "-tags=fctl_component_guest", "./entrypoints/...")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list component guest dependencies: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if dependency == "google.golang.org/grpc" || strings.HasPrefix(dependency, "google.golang.org/grpc/") {
			t.Fatalf("component guest dependency closure contains grpc-go package %q", dependency)
		}
	}
}

func TestEveryOperationBindsTheGeneratedBucketDescriptor(t *testing.T) {
	t.Parallel()

	for _, command := range (Plugin{}).Commands() {
		for _, operation := range command.Operations {
			method, err := bucketMethodDescriptor(operation)
			if err != nil {
				t.Fatalf("%s operation %s: %v", command.ID, operation.ID, err)
			}
			fullMethod := "/" + string(method.Parent().FullName()) + "/" + string(method.Name())
			if fullMethod != operation.GRPC.FullMethod {
				t.Fatalf("%s operation %s descriptor = %q, want %q", command.ID, operation.ID, fullMethod, operation.GRPC.FullMethod)
			}
			if method.IsStreamingClient() {
				t.Fatalf("%s operation %s unexpectedly uses client streaming", command.ID, operation.ID)
			}
			if method.IsStreamingServer() != operation.GRPC.ServerStreaming {
				t.Fatalf("%s operation %s server-streaming descriptor = %v, policy = %v", command.ID, operation.ID, method.IsStreamingServer(), operation.GRPC.ServerStreaming)
			}
		}
	}
}

func TestPluginExecuteRoutesEveryCataloguedCommand(t *testing.T) {
	commands := (Plugin{}).Commands()
	if len(commands) != 48 {
		t.Fatalf("catalogue has %d commands, want 48", len(commands))
	}
	for _, command := range commands {
		command := command
		t.Run(command.ID, func(t *testing.T) {
			request := routableV3Request(command)
			if _, err := decode(command, request); err != nil {
				t.Fatalf("route fixture does not satisfy descriptor: %v", err)
			}
			err := (Plugin{}).Execute(context.Background(), request, sdk.NewMemoryHost(nil))
			if err != nil && strings.Contains(err.Error(), "has no Ledger v3 adapter") {
				t.Fatalf("Plugin.Execute() did not route command: %v", err)
			}
		})
	}
}

func routableV3Request(command sdk.Command) sdk.ExecuteRequest {
	request := sdk.ExecuteRequest{
		CommandID:       command.ID,
		Target:          sdk.TargetSelection{OrganizationID: "org", StackID: "stack"},
		ServiceVersions: []sdk.ServiceVersion{{Service: sdk.ServiceLedger, Version: "3.0.0", Major: 3}},
	}
	for _, argument := range command.Arguments {
		if argument.Required {
			request.Arguments = append(request.Arguments, routeFixtureValue(argument.Name, argument.Completion, string(argument.Type)))
		}
	}
	for _, flag := range command.Flags {
		if flag.Required {
			request.Flags = append(request.Flags, sdk.FlagOccurrence{Name: flag.Name, Value: routeFixtureValue(flag.Name, flag.Completion, string(flag.Type))})
		}
	}
	return request
}

func routeFixtureValue(name string, completion sdk.CompletionSpec, fieldType string) string {
	if completion.Kind == sdk.CompletionStatic && len(completion.Candidates) != 0 {
		return completion.Candidates[0].Value
	}
	switch fieldType {
	case "bool":
		return "true"
	case "int32":
		return "1"
	}
	switch name {
	case "transaction-id", "sequence":
		return "1"
	case "cron":
		return "* * * * *"
	case "address":
		return "users:1"
	case "filter":
		return "true"
	default:
		return "fixture"
	}
}

func TestEveryCommandDeclaresLedgerMajorThreeAndNoOther(t *testing.T) {
	t.Parallel()

	for _, command := range (Plugin{}).Commands() {
		if len(command.Compatibility) != 1 {
			t.Fatalf("%s declares %d compatibilities, want exactly one", command.ID, len(command.Compatibility))
		}
		compatibility := command.Compatibility[0]
		if compatibility.Service != sdk.ServiceLedger {
			t.Fatalf("%s declares service %q, want ledger", command.ID, compatibility.Service)
		}
		if len(compatibility.Majors) != 1 || compatibility.Majors[0] != 3 {
			t.Fatalf("%s declares majors %v, want [3]", command.ID, compatibility.Majors)
		}
	}
}

func TestRequestSigningIsDeclaredExactlyOnCommandsUsingApply(t *testing.T) {
	t.Parallel()

	for _, command := range (Plugin{}).Commands() {
		usesApply := false
		for _, operation := range command.Operations {
			if operation.GRPC != nil && operation.GRPC.FullMethod == bucketFullMethod("Apply") {
				usesApply = true
			}
		}

		if usesApply != (command.RequestSigning != nil) {
			t.Fatalf("%s: uses-apply=%v but request signing declared=%v", command.ID, usesApply, command.RequestSigning != nil)
		}
		if command.RequestSigning == nil {
			continue
		}
		signing := *command.RequestSigning
		if signing.Capability != sdk.CapabilitySignLedgerApplyBatch {
			t.Fatalf("%s signing capability = %q", command.ID, signing.Capability)
		}
		if signing.ProductMajor != 3 {
			t.Fatalf("%s signing major = %d, want 3", command.ID, signing.ProductMajor)
		}
		if signing.PayloadType != "formance.ledger.v3.ApplyBatch" {
			t.Fatalf("%s signing payload type = %q", command.ID, signing.PayloadType)
		}
	}
}

func TestCommandFacetRequiresExactlyTheDerivedHostCapabilities(t *testing.T) {
	t.Parallel()

	plugin := Plugin{}
	if err := sdk.ValidateCommandFacetHostRequirements(plugin.Metadata().Facets, plugin.Commands()); err != nil {
		t.Fatalf("ValidateCommandFacetHostRequirements: %v", err)
	}
}

func TestEveryOperationScopeIsAGranularLedgerScope(t *testing.T) {
	t.Parallel()

	for _, command := range (Plugin{}).Commands() {
		for _, operation := range command.Operations {
			if !sdk.ExactScopeSet(operation.Scopes) {
				t.Fatalf("%s operation %s scopes %v are not an exact set", command.ID, operation.ID, operation.Scopes)
			}
			if len(operation.Scopes) == 0 {
				t.Fatalf("%s operation %s declares no scope", command.ID, operation.ID)
			}
			for _, scope := range operation.Scopes {
				if _, ok := auth.AllGranularScopes[auth.Scope(scope)]; !ok {
					t.Fatalf("%s operation %s declares %q, which is not one of the product's 14 granular scopes", command.ID, operation.ID, scope)
				}
			}
		}
	}
}

func TestPaginatedCommandsDeclareExactlyOneServerStreamingOperation(t *testing.T) {
	t.Parallel()

	for _, command := range (Plugin{}).Commands() {
		if !command.Pagination.Supported {
			continue
		}
		if len(command.Operations) != 1 {
			t.Fatalf("%s is paginated with %d operations", command.ID, len(command.Operations))
		}
		grpc := command.Operations[0].GRPC
		if grpc == nil || !grpc.ServerStreaming {
			t.Fatalf("%s is paginated but its operation is not server-streaming", command.ID)
		}
		if command.ExecutionPolicy == nil || command.ExecutionPolicy.MaxHostRequests < sdk.DefaultAllPagesMaxPages {
			t.Fatalf("%s cannot honour the host's all-pages page ceiling", command.ID)
		}
	}
}

func TestCommandAndOperationIdentifiersAreUnique(t *testing.T) {
	t.Parallel()

	ids := map[string]struct{}{}
	operationScopes := map[string][]string{}
	for _, command := range (Plugin{}).Commands() {
		if _, exists := ids[command.ID]; exists {
			t.Fatalf("command id %q is declared twice", command.ID)
		}
		ids[command.ID] = struct{}{}
		for _, operation := range command.Operations {
			scopes := append([]string(nil), operation.Scopes...)
			sort.Strings(scopes)
			previous, seen := operationScopes[operation.ID]
			if seen && strings.Join(previous, ",") != strings.Join(scopes, ",") {
				t.Fatalf("operation %q carries scopes %v here and %v elsewhere", operation.ID, scopes, previous)
			}
			operationScopes[operation.ID] = scopes
		}
	}
}
