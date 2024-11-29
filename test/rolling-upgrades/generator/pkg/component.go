package pulumi_generator

import (
	"fmt"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	v2 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type GeneratorComponent struct {
	pulumi.ResourceState

	PodNamespace pulumi.StringOutput
	PodName      pulumi.StringOutput
	PodID        pulumi.IDOutput
}

type GeneratorComponentArgs struct {
	Namespace pulumi.StringInput
	LedgerURL pulumi.StringInput
	Image     pulumi.StringInput
}

func NewGeneratorComponent(ctx *pulumi.Context, name string, args *GeneratorComponentArgs, opts ...pulumi.ResourceOption) (*GeneratorComponent, error) {
	cmp := &GeneratorComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Generator", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	var image pulumi.StringInput = pulumi.String("ghcr.io/formancehq/ledger-generator:latest")
	if args.Image != nil {
		image = args.Image.ToStringOutput().ApplyT(func(v string) string {
			if v == "" {
				return "ghcr.io/formancehq/ledger-generator:latest"
			}
			return v
		}).(pulumi.StringInput)
	}

	generatorArgs := pulumi.StringArray{
		args.LedgerURL,
		pulumi.String("/examples/example1.js"),
		pulumi.String("-p"),
		pulumi.String("30"),
	}

	for _, key := range features.MinimalFeatureSet.SortedKeys() {
		generatorArgs = append(generatorArgs,
			pulumi.String("--ledger-feature"),
			pulumi.String(key+"="+features.MinimalFeatureSet[key]),
		)
	}

	rel, err := v1.NewPod(
		ctx,
		"generator",
		&v1.PodArgs{
			Metadata: v2.ObjectMetaArgs{
				Namespace: args.Namespace,
			},
			Spec: v1.PodSpecArgs{
				RestartPolicy: pulumi.String("Never"),
				Containers: v1.ContainerArray{
					v1.ContainerArgs{
						Name:            pulumi.String("test"),
						Args:            generatorArgs,
						Image:           image,
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
		return nil, fmt.Errorf("creating pod: %w", err)
	}

	cmp.PodNamespace = rel.Metadata.Namespace().Elem()
	cmp.PodName = rel.Metadata.Name().Elem()
	cmp.PodID = rel.ID()

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"pod-name":      cmp.PodName,
		"pod-namespace": cmp.PodNamespace,
		"id":            cmp.PodID,
	}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
