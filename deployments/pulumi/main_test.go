package main

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/google/uuid"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/pkg/v3/testing/integration"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optdestroy"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestProgram(t *testing.T) {

	ctx := logging.TestingContext()
	stackName := "ledger-tests-pulumi-" + uuid.NewString()[:8]

	stack, err := auto.UpsertStackInlineSource(ctx, stackName, "ledger-tests-pulumi-postgres", deployPostgres(stackName))
	require.NoError(t, err)

	t.Log("Deploy pg stack")
	up, err := stack.Up(ctx, optup.ProgressStreams(os.Stdout), optup.ErrorProgressStreams(os.Stderr))
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Destroy stack")
		_, err := stack.Destroy(ctx, optdestroy.Remove(), optdestroy.ProgressStreams(os.Stdout), optdestroy.ErrorProgressStreams(os.Stderr))
		require.NoError(t, err)
	})

	postgresURI := up.Outputs["uri"].Value.(string)

	t.Log("Test program")
	integration.ProgramTest(t, &integration.ProgramTestOptions{
		Quick:       true,
		SkipRefresh: true,
		Dir:         ".",
		Config: map[string]string{
			"namespace":    stackName,
			"postgres.uri": postgresURI,
			"timeout":      "30",
		},
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Verbose: testing.Verbose(),
	})
}

func deployPostgres(stackName string) func(ctx *pulumi.Context) error {
	return func(ctx *pulumi.Context) error {
		namespace, err := corev1.NewNamespace(ctx, "namespace", &corev1.NamespaceArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Name: pulumi.String(stackName),
			},
		})
		if err != nil {
			return fmt.Errorf("creating namespace")
		}

		rel, err := helm.NewRelease(ctx, "postgres", &helm.ReleaseArgs{
			Chart:     pulumi.String("oci://registry-1.docker.io/bitnamicharts/postgresql"),
			Version:   pulumi.String("16.1.1"),
			Namespace: namespace.Metadata.Name(),
			Values: pulumi.Map(map[string]pulumi.Input{
				"auth": pulumi.Map{
					"postgresPassword": pulumi.String("postgres"),
					"database":         pulumi.String("ledger"),
				},
				"primary": pulumi.Map{
					"resources": pulumi.Map{
						"requests": pulumi.Map{
							"memory": pulumi.String("256Mi"),
							"cpu":    pulumi.String("256m"),
						},
					},
				},
			}),
			CreateNamespace: pulumi.BoolPtr(true),
		})
		if err != nil {
			return fmt.Errorf("installing release")
		}

		ctx.Export("uri", pulumi.Sprintf(
			"postgres://postgres:postgres@%s-postgresql.%s:5432/ledger?sslmode=disable",
			rel.Status.Name().Elem(),
			rel.Status.Namespace().Elem(),
		))
		return nil
	}
}
