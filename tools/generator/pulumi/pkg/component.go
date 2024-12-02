package pulumi_generator

import (
	"fmt"
	"github.com/formancehq/ledger/pkg/features"
	v3 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	v2 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type GeneratorComponent struct {
	pulumi.ResourceState

	JobNamespace pulumix.Output[string]
	JobName      pulumix.Output[string]
	JobID        pulumix.Output[pulumi.ID]
	Job          pulumix.Output[*v3.Job]
}

type GeneratorComponentArgs struct {
	Namespace  pulumix.Input[string]
	LedgerURL  pulumix.Input[string]
	Version    pulumix.Input[string]
	UntilLogID pulumix.Input[int]
}

func NewGeneratorComponent(ctx *pulumi.Context, name string, args *GeneratorComponentArgs, opts ...pulumi.ResourceOption) (*GeneratorComponent, error) {
	cmp := &GeneratorComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Tools:Generator", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	var version = pulumix.Val("latest")
	if args.Version != nil {
		version = pulumix.Apply(args.Version, func(version string) string {
			if version == "" {
				return "latest"
			}
			return version
		})
	}

	generatorArgs := pulumix.Array[string]{
		args.LedgerURL,
		pulumix.Val("/examples/example1.js"),
		pulumix.Val("-p"),
		pulumix.Val("30"),
	}

	if args.UntilLogID != nil {
		generatorArgs = append(generatorArgs,
			pulumix.Val("--until-log-id"),
			pulumix.Apply(args.UntilLogID, func(v int) string {
				return fmt.Sprintf("%d", v)
			}),
		)
	}

	for _, key := range features.MinimalFeatureSet.SortedKeys() {
		generatorArgs = append(generatorArgs,
			pulumix.Val("--ledger-feature"),
			pulumix.Val(key+"="+features.MinimalFeatureSet[key]),
		)
	}

	namespace := pulumix.Val[string]("")
	if args.Namespace != nil {
		namespace = args.Namespace.ToOutput(ctx.Context())
	}

	cmp.Job = pulumix.ApplyErr(args.UntilLogID, func(untilLogID int) (*v3.Job, error) {
		return v3.NewJob(ctx, fmt.Sprintf("generator-%d", untilLogID), &v3.JobArgs{
			Metadata: v2.ObjectMetaArgs{
				Namespace:   namespace.Untyped().(pulumi.StringOutput),
			},
			Spec: v3.JobSpecArgs{
				Template: v1.PodTemplateSpecArgs{
					Spec: v1.PodSpecArgs{
						RestartPolicy: pulumi.String("OnFailure"),
						Containers: v1.ContainerArray{
							v1.ContainerArgs{
								Name:            pulumi.String("test"),
								Args:            generatorArgs.ToOutput(ctx.Context()).Untyped().(pulumi.StringArrayOutput),
								Image:           pulumi.Sprintf("ghcr.io/formancehq/ledger-generator:%s", version),
								ImagePullPolicy: pulumi.String("Always"),
							},
						},
					},
				},
			},
		},
			pulumi.DeleteBeforeReplace(true),
			pulumi.Parent(cmp),
		)
	})

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
