package pulumi_generator

import (
	"errors"
	"fmt"
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
	Script     pulumix.Input[string]
	VUs        pulumix.Input[int]
	Features   pulumix.Map[string]
}

func NewGeneratorComponent(ctx *pulumi.Context, name string, args *GeneratorComponentArgs, opts ...pulumi.ResourceOption) (*GeneratorComponent, error) {
	cmp := &GeneratorComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Tools:Generator", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	if args.UntilLogID == nil {
		return nil, errors.New("untilLogID is required")
	}

	if args.Script == nil {
		return nil, errors.New("script is required")
	}

	untilLogID := pulumix.ApplyErr(args.UntilLogID, func(untilLogID int) (int, error) {
		if untilLogID == 0 {
			return 0, errors.New("untilLogID must be greater than 0")
		}

		return untilLogID, nil
	})

	script := pulumix.ApplyErr(args.Script, func(script string) (string, error) {
		if script == "" {
			return "", errors.New("script is required")
		}

		return script, nil
	})

	var version = pulumix.Val("latest")
	if args.Version != nil {
		version = pulumix.Apply(args.Version, func(version string) string {
			if version == "" {
				return "latest"
			}
			return version
		})
	}

	namespace := pulumix.Val[string]("")
	if args.Namespace != nil {
		namespace = args.Namespace.ToOutput(ctx.Context())
	}

	scriptConfigMap := pulumix.ApplyErr(script, func(script string) (*v1.ConfigMap, error) {
		return v1.NewConfigMap(ctx, "generator-script", &v1.ConfigMapArgs{
			Metadata: v2.ObjectMetaArgs{
				Namespace: namespace.Untyped().(pulumi.StringOutput),
			},
			Data: pulumi.StringMap{
				"script.js": pulumi.String(script),
			},
		})
	})

	vus := pulumix.Val(30)
	if args.VUs != nil {
		vus = args.VUs.ToOutput(ctx.Context())
	}

	generatorArgs := pulumix.Apply4Err(
		args.LedgerURL,
		args.Features,
		vus,
		untilLogID,
		func(ledgerURL string, features map[string]string, vus, untilLogID int) ([]string, error) {
			ret := make([]string, 0)
			for key, value := range features {
				ret = append(ret, "--ledger-feature", key+"="+value)
			}
			ret = append(ret,
				ledgerURL,
				"/scripts/script.js",
				"-p", fmt.Sprint(vus),
				"--until-log-id", fmt.Sprint(untilLogID))
			return ret, nil
		},
	)

	cmp.Job = pulumix.Apply2Err(args.UntilLogID, scriptConfigMap, func(untilLogID int, configMap *v1.ConfigMap) (*v3.Job, error) {
		return v3.NewJob(ctx, fmt.Sprintf("generator-%d", untilLogID), &v3.JobArgs{
			Metadata: v2.ObjectMetaArgs{
				Namespace: namespace.Untyped().(pulumi.StringOutput),
				//Annotations: pulumi.StringMap{
				//	"pulumi.com/skipAwait": pulumi.String("true"),
				//},
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
								VolumeMounts: v1.VolumeMountArray{
									v1.VolumeMountArgs{
										MountPath: pulumi.String("/scripts"),
										Name:      pulumi.String("scripts"),
										ReadOnly:  pulumi.BoolPtr(true),
									},
								},
							},
						},
						Volumes: v1.VolumeArray{
							v1.VolumeArgs{
								Name: pulumi.String("scripts"),
								ConfigMap: &v1.ConfigMapVolumeSourceArgs{
									Name: configMap.Metadata.Name(),
								},
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
