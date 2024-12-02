package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	pulumi_ledger "github.com/formancehq/ledger/deployments/pulumi/ledger/pkg"
	pulumi_postgres "github.com/formancehq/ledger/deployments/pulumi/postgres/pkg"
	"github.com/formancehq/ledger/tools/generator/pulumi/pkg"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var (
	latestVersion      = flag.String("latest-version", "latest", "The version to deploy first")
	actualVersion      = flag.String("actual-version", "latest", "The version to upgrade")
	noCleanup          = flag.Bool("no-cleanup", false, "Disable cleanup of created resources")
	noCleanupOnFailure = flag.Bool("no-cleanup-on-failure", false, "Disable cleanup of created resources on failure")
	stackName          = flag.String("stack", "", "Pulumi stack name")
	testImage          = flag.String("test-image", "", "Test image")
)

func TestK8SRollingUpgrades(t *testing.T) {

	flag.Parse()

	ctx := logging.TestingContext()

	testFailure := false

	// Installing the stack
	stack, err := auto.UpsertStackInlineSource(ctx, *stackName, "rolling-upgrades-tests", deployStack)
	require.NoError(t, err, "creating stack")

	t.Cleanup(func() {
		if testFailure && *noCleanupOnFailure {
			return
		}

		if *noCleanup {
			return
		}

		if _, err := stack.Destroy(ctx); err != nil {
			logging.FromContext(ctx).Errorf("destroying stack: %v", err)
		}

		if err := stack.Workspace().RemoveStack(ctx, stack.Name()); err != nil {
			logging.FromContext(ctx).Errorf("removing stack: %v", err)
		}
	})

	_, err = upAndPrintOutputs(ctx, stack, map[string]auto.ConfigValue{
		"ledger:version":  {Value: *latestVersion},
		"generator:image": {Value: *testImage},
	})
	require.NoError(t, err, "upping base stack")

	// Let a moment ensure the test image is actually sending requests
	// We could maybe find a dynamic way to do that
	<-time.After(5 * time.Second)

	_, err = upAndPrintOutputs(ctx, stack, map[string]auto.ConfigValue{
		"ledger:version":  {Value: *actualVersion},
		"generator:image": {Value: *testImage},
	})
	require.NoError(t, err, "upping ledger stack second time")

	<-time.After(5 * time.Second)

	stackOutputs, err := stack.Outputs(ctx)
	require.NoError(t, err, "unable to extract test stack outputs")

	projectSettings, err := stack.Workspace().ProjectSettings(ctx)
	require.NoError(t, err, "unable to extract project settings")

	// Check the test stack
	checkStack, err := auto.UpsertStackInlineSource(
		ctx,
		*stackName+"-check",
		string(projectSettings.Name),
		func(ctx *pulumi.Context) error {
			pod, err := corev1.GetPod(
				ctx,
				stackOutputs["generator:pod-name"].Value.(string),
				pulumi.ID(stackOutputs["generator:pod-id"].Value.(string)),
				nil,
			)
			if err != nil {
				return err
			}

			ctx.Export("phase", pod.Status.Phase().Elem())

			return nil
		},
	)
	require.NoError(t, err, "creating test stack")
	t.Cleanup(func() {
		_, err := checkStack.Destroy(ctx)
		if err != nil {
			t.Log(err)
			return
		}

		err = checkStack.Workspace().RemoveStack(ctx, stack.Name())
		if err != nil {
			t.Log(err)
			return
		}
	})

	ret, err := upAndPrintOutputs(ctx, checkStack, map[string]auto.ConfigValue{})
	require.NoError(t, err, "upping check stack")

	testFailure = ret.Outputs["phase"].Value.(string) == "Failed"
	require.False(t, testFailure)
}

func upAndPrintOutputs(ctx context.Context, stack auto.Stack, configs map[string]auto.ConfigValue) (auto.UpResult, error) {

	if err := stack.SetAllConfig(ctx, configs); err != nil {
		return auto.UpResult{}, fmt.Errorf("setting config: %w", err)
	}

	out, err := stack.Up(ctx)
	if out.StdErr != "" {
		fmt.Println(out.StdErr)
	}
	if err != nil {
		return auto.UpResult{}, fmt.Errorf("upping stack '%s': %w", stack.Name(), err)
	}

	if out.StdOut != "" {
		fmt.Println(out.StdOut)
	}

	return out, nil
}

func deployStack(ctx *pulumi.Context) error {
	stack, err := NewStackComponent(ctx, "full", &StackComponentArgs{
		GeneratorImage: pulumi.String(config.Get(ctx, "generator:image")),
		LedgerVersion:  pulumi.String(config.Get(ctx, "ledger:version")),
	})
	if err != nil {
		return err
	}

	ctx.Export("generator:pod-namespace", stack.GeneratorComponent.JobNamespace)
	ctx.Export("generator:pod-name", stack.GeneratorComponent.JobName)
	ctx.Export("generator:pod-id", stack.GeneratorComponent.JobID)

	return nil
}

type StackComponent struct {
	pulumi.ResourceState

	PostgresComponent  *pulumi_postgres.PostgresComponent
	LedgerComponent    *pulumi_ledger.LedgerComponent
	GeneratorComponent *pulumi_generator.GeneratorComponent
}

type StackComponentArgs struct {
	Namespace      pulumi.StringInput
	LedgerVersion  pulumi.StringInput
	GeneratorImage pulumi.StringInput
}

func NewStackComponent(ctx *pulumi.Context, name string, args *StackComponentArgs, opts ...pulumi.ResourceOption) (*StackComponent, error) {
	cmp := &StackComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:RollingUpgradesTests", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.PostgresComponent, err = pulumi_postgres.NewPostgresComponent(
		ctx,
		"postgres",
		&pulumi_postgres.PostgresComponentArgs{},
		pulumi.Parent(cmp),
	)
	if err != nil {
		return nil, fmt.Errorf("creating postgres component: %w", err)
	}

	cmp.LedgerComponent, err = pulumi_ledger.NewLedgerComponent(ctx, "ledger", &pulumi_ledger.LedgerComponentArgs{
		ImagePullPolicy:      pulumi.String("Always"),
		PostgresURI:          pulumi.Sprintf("postgres://postgres:postgres@%s.svc.cluster.local:5432/ledger?sslmode=disable", cmp.PostgresComponent.Service),
		Debug:                pulumi.Bool(true),
		ReplicaCount:         pulumi.Int(1),
		ExperimentalFeatures: pulumi.Bool(true),
		Timeout:              pulumi.Int(30),
		Tag:                  args.LedgerVersion,
	}, pulumi.Transforms([]pulumi.ResourceTransform{
		// Update relative location of the helm chart
		func(context context.Context, args *pulumi.ResourceTransformArgs) *pulumi.ResourceTransformResult {
			if args.Type == "kubernetes:helm.sh/v3:Release" {
				args.Props["chart"] = pulumi.String("../../deployments/helm")
			}

			return &pulumi.ResourceTransformResult{
				Props: args.Props,
			}
		},
	}), pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating ledger component: %w", err)
	}

	cmp.GeneratorComponent, err = pulumi_generator.NewGeneratorComponent(ctx, "generator", &pulumi_generator.GeneratorComponentArgs{
		Namespace: args.Namespace,
		LedgerURL: cmp.LedgerComponent.ServiceInternalURL,
		Version:   args.GeneratorImage,
	})
	if err != nil {
		return nil, err
	}

	return cmp, nil
}
