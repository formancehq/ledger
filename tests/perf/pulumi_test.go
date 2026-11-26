package main

import (
	"sync"
	"testing"

	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Pulumi's mock monitor executes resource serialization without a provider or
// Kubernetes connection. This is not a mockgen-managed repository interface.
type benchmarkMonitor struct {
	mu        sync.Mutex
	resources map[string]resource.PropertyMap
}

func (m *benchmarkMonitor) Call(args pulumi.MockCallArgs) (resource.PropertyMap, error) {
	return args.Args, nil
}

func (m *benchmarkMonitor) NewResource(args pulumi.MockResourceArgs) (string, resource.PropertyMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resources[args.TypeToken] = args.Inputs
	return args.Name + "-id", args.Inputs, nil
}

func TestPulumiSerializesAsynchronousRun(t *testing.T) {
	t.Parallel()
	identity := testIdentity()
	monitor := &benchmarkMonitor{resources: map[string]resource.PropertyMap{}}
	err := pulumi.RunErr(runBenchmark, pulumi.WithMocks("perf", "isolated-test", monitor), func(info *pulumi.RunInfo) {
		info.Config = map[string]string{
			"perf:profile": "smoke-5m", "perf:commit": identity.Commit,
			"perf:image": identity.Image, "perf:run-id": identity.RunID,
			"perf:run-attempt": identity.Attempt, "perf:pr": identity.PR,
			"perf:k8s-context": "must-not-contact-this-context",
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	benchmark, ok := monitor.resources["kubernetes:benchmark.formance.com/v1alpha1:Benchmark"]
	if !ok {
		t.Fatal("Benchmark was not registered")
	}
	metadata := benchmark["metadata"].ObjectValue()
	name := metadata["name"].StringValue()
	if metadata["annotations"].ObjectValue()["pulumi.com/skipAwait"].StringValue() != "true" {
		t.Fatal("submission would wait for benchmark readiness")
	}
	spec := benchmark["spec"].ObjectValue()
	if spec["executionGroup"].StringValue() != "acme-ledger-m8a" || spec["activeDeadlineSeconds"].NumberValue() != 7200 {
		t.Fatal("operator lifecycle constraints lost during Pulumi serialization")
	}
	if spec["grafanaVariables"].ObjectValue()["cluster"].StringValue() != name+"-res-0" {
		t.Fatal("snapshot variable lost during Pulumi serialization")
	}
	archive, ok := monitor.resources["kubernetes:core/v1:ConfigMap"]
	if !ok {
		t.Fatal("archive ConfigMap was not registered")
	}
	if archive["metadata"].ObjectValue()["labels"].ObjectValue()["benchmark.formance.com/name"].StringValue() != name {
		t.Fatal("archive cannot be adopted by its Benchmark")
	}
	if archive["binaryData"].ObjectValue()["world_to_bank.tar"].StringValue() == "" {
		t.Fatal("archive is empty")
	}
}
