package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
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
	projectName        = flag.String("project", "", "Pulumi project")
	stackPrefixName    = flag.String("stack-prefix-name", "", "Pulumi stack prefix for names")
	testImage          = flag.String("test-image", "", "Test image")
)

func TestK8SRollingUpgrades(t *testing.T) {

	flag.Parse()

	ctx := logging.TestingContext()

	testFailure := false
	cleanup := func(stack auto.Stack) func() {
		return func() {
			if testFailure && *noCleanupOnFailure {
				return
			}
			cleanup(ctx, stack)
		}
	}

	logging.FromContext(ctx).Info("Installing Postgres")
	pgStack, err := auto.UpsertStackInlineSource(ctx, *stackPrefixName+"postgres", *projectName, deployPostgres)
	require.NoError(t, err, "creating ledger stack")
	t.Cleanup(cleanup(pgStack))

	_, err = upAndPrintOutputs(ctx, pgStack)
	require.NoError(t, err, "upping pg stack")

	ledgerStack, err := auto.UpsertStackLocalSource(ctx, *stackPrefixName+"ledger", "../../deployments/pulumi")
	require.NoError(t, err, "creating ledger stack")
	t.Cleanup(cleanup(ledgerStack))

	pgStackOutputs, err := pgStack.Outputs(ctx)
	require.NoError(t, err, "unable to extract pg stack outputs")

	err = ledgerStack.SetAllConfig(
		ctx,
		auto.ConfigMap{
			"version": auto.ConfigValue{Value: *latestVersion},
			"postgres.uri": auto.ConfigValue{
				Value: "postgres://ledger:ledger@" + pgStackOutputs["service-name"].Value.(string) + ".svc.cluster.local:5432/ledger?sslmode=disable",
			},
			"debug":            auto.ConfigValue{Value: "true"},
			"image.pullPolicy": auto.ConfigValue{Value: "Always"},
			"replicaCount":     auto.ConfigValue{Value: "1"},
		},
	)
	require.NoError(t, err, "setting config on ledger stack")

	_, err = upAndPrintOutputs(ctx, ledgerStack)
	require.NoError(t, err, "upping ledger stack first time")

	testStack, err := auto.UpsertStackInlineSource(ctx, *stackPrefixName+"test", *projectName, deployTest)
	require.NoError(t, err, "creating test stack")
	t.Cleanup(cleanup(testStack))

	ledgerStackOutputs, err := ledgerStack.Outputs(ctx)
	require.NoError(t, err, "unable to extract ledger stack outputs")

	ledgerURL := fmt.Sprintf(
		"http://%s.%s.svc.cluster.local:%.0f",
		ledgerStackOutputs["service-name"].Value,
		ledgerStackOutputs["service-namespace"].Value,
		ledgerStackOutputs["service-port"].Value,
	)

	err = testStack.SetAllConfig(ctx, auto.ConfigMap{
		"ledger-url": auto.ConfigValue{Value: ledgerURL},
		"image":      auto.ConfigValue{Value: *testImage},
	})
	require.NoError(t, err, "setting config on test stack")

	_, err = testStack.Destroy(ctx)
	require.NoError(t, err, "destroying test stack")

	_, err = upAndPrintOutputs(ctx, testStack)
	require.NoError(t, err, "upping test stack")

	// Let a moment ensure the test image is actually sending requests
	<-time.After(5 * time.Second)

	err = ledgerStack.SetConfig(ctx, "version", auto.ConfigValue{
		Value: *actualVersion,
	})
	require.NoError(t, err, "setting version on ledger stack")

	_, err = upAndPrintOutputs(ctx, ledgerStack)
	require.NoError(t, err, "upping ledger stack second time")

	testStackOutputs, err := testStack.Outputs(ctx)
	require.NoError(t, err, "unable to extract test stack outputs")

	checkStack, err := auto.UpsertStackInlineSource(
		ctx,
		*stackPrefixName+"check",
		*projectName,
		func(ctx *pulumi.Context) error {
			pod, err := corev1.GetPod(ctx, testStackOutputs["name"].Value.(string), pulumi.ID(testStackOutputs["id"].Value.(string)), nil)
			if err != nil {
				return err
			}

			ctx.Export("phase", pod.Status.Phase().ApplyT(func(phase *string) string {
				return *phase
			}))

			return nil
		},
	)
	require.NoError(t, err, "creating test stack")
	t.Cleanup(cleanup(checkStack))

	ret, err := upAndPrintOutputs(ctx, checkStack)
	require.NoError(t, err, "upping check stack")

	testFailure = ret.Outputs["phase"].Value.(string) == "Failed"
	require.False(t, testFailure)
}

func cleanup(ctx context.Context, stack auto.Stack) {
	if *noCleanup {
		return
	}

	if _, err := stack.Destroy(ctx); err != nil {
		logging.FromContext(ctx).Errorf("destroying stack: %v", err)
	}
}

func upAndPrintOutputs(ctx context.Context, stack auto.Stack) (auto.UpResult, error) {
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

func deployTest(ctx *pulumi.Context) error {
	conf := config.New(ctx, "")
	namespace, err := conf.Try("namespace")
	if err != nil {
		namespace = "default"
	}
	image := conf.Require("image")
	ledgerURL := conf.Require("ledger-url")

	rel, err := corev1.NewPod(
		ctx,
		"test",
		&corev1.PodArgs{
			Metadata: metav1.ObjectMetaArgs{
				Namespace: pulumi.String(namespace),
			},
			Spec: corev1.PodSpecArgs{
				RestartPolicy: pulumi.String("Never"),
				Containers: corev1.ContainerArray{
					corev1.ContainerArgs{
						Name: pulumi.String("test"),
						Args: pulumi.StringArray{
							pulumi.String(ledgerURL),
							pulumi.String("/examples/example1.js"),
							pulumi.String("-p"),
							pulumi.String("100"),
						},
						Image:           pulumi.String(image),
						ImagePullPolicy: pulumi.String("Always"),
					},
				},
			},
		},
		pulumi.Timeouts(&pulumi.CustomTimeouts{
			Create: "10s",
			Update: "10s",
			Delete: "10s",
		}),
		pulumi.DeleteBeforeReplace(true),
	)
	if err != nil {
		return err
	}

	ctx.Export("name", rel.Metadata.Name())
	ctx.Export("id", rel.ID())

	return nil
}

func deployPostgres(ctx *pulumi.Context) error {
	conf := config.New(ctx, "")
	namespace, err := conf.Try("namespace")
	if err != nil {
		namespace = "default"
	}

	rel, err := helm.NewRelease(ctx, "postgres", &helm.ReleaseArgs{
		Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/postgresql"),
		Version:   pulumi.String("16.1.1"),
		Namespace: pulumi.String(namespace),
		Values: pulumi.Map(map[string]pulumi.Input{
			"auth": pulumi.Map{
				"password": pulumi.String("ledger"),
				"username": pulumi.String("ledger"),
				"database": pulumi.String("ledger"),
			},
		}),
		CreateNamespace: pulumi.BoolPtr(true),
	})
	if err != nil {
		return fmt.Errorf("installing release")
	}

	svc := pulumi.All(rel.Status.Namespace(), rel.Status.Name()).
		ApplyT(func(r any) string {
			arr := r.([]interface{})
			namespace := arr[0].(*string)
			name := arr[1].(*string)

			return fmt.Sprintf("%s-postgresql.%s", *name, *namespace)
		})

	ctx.Export("service-name", svc)

	return nil
}
