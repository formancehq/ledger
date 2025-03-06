package generator

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"os"
	"path/filepath"
	"time"
)

type Component struct {
	pulumi.ResourceState
}

type Args struct {
	GeneratorVersion  pulumix.Input[string]
	UntilLogID        pulumix.Input[uint]
	Script            pulumix.Input[string]
	VUs               pulumix.Input[int]
	Ledger            pulumix.Input[string]
	HTTPClientTimeout pulumix.Input[time.Duration]
	ScriptFromFile    pulumix.Output[string]
}

func (a *Args) SetDefaults() {
	if a.VUs == nil {
		a.VUs = pulumix.Val(0)
	}
	a.VUs = pulumix.Apply(a.VUs, func(vus int) int {
		if vus == 0 {
			return 1
		}
		return vus
	})

	if a.Ledger == nil {
		a.Ledger = pulumix.Val("")
	}
	a.Ledger = pulumix.Apply(a.Ledger, func(ledger string) string {
		if ledger == "" {
			return "default"
		}
		return ledger
	})
}

type ComponentArgs struct {
	common.CommonArgs
	Args
	API *api.Component
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Tools:Generator", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	scriptConfigMap, err := corev1.NewConfigMap(ctx, "generator-script", &corev1.ConfigMapArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
		},
		Data: pulumi.StringMap{
			"script.js": pulumix.Apply2Err(args.Script, args.ScriptFromFile, func(script, scriptFromFile string) (string, error) {
				if script == "" && scriptFromFile == "" {
					return "", errors.New("script is required")
				}
				if script != "" && scriptFromFile != "" {
					return "", errors.New("either script or script-from-file must be specified")
				}
				if script != "" {
					return script, nil
				}

				scriptData, err := os.ReadFile(filepath.Join(ctx.RootDirectory(), scriptFromFile))
				if err != nil {
					return "", err
				}

				return string(scriptData), nil
			}).Untyped().(pulumi.StringOutput),
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating config map: %w", err)
	}

	generatorArgs := pulumix.Apply7Err(
		args.Ledger,
		args.API.Service.Metadata.Name().Elem(),
		args.API.Service.Spec.Ports().Index(pulumi.Int(0)).Port(),
		args.VUs,
		args.UntilLogID,
		args.HTTPClientTimeout,
		args.Debug,
		func(
			ledger string,
			serviceEndpoint string,
			servicePort int,
			vus int,
			untilLogID uint,
			httpClientTimeout time.Duration,
			debug bool,
		) ([]string, error) {
			ret := []string{
				fmt.Sprintf("http://%s:%d", serviceEndpoint, servicePort),
				"/scripts/script.js",
				"-p", fmt.Sprint(vus),
				"--ledger", ledger,
			}
			if untilLogID > 0 {
				ret = append(ret, "--until-log-id", fmt.Sprint(untilLogID))
			}
			if httpClientTimeout > 0 {
				ret = append(ret, "--http-client-timeout", httpClientTimeout.String())
			}
			if debug {
				ret = append(ret, "--debug")
			}
			return ret, nil
		},
	)

	podSpec := corev1.PodSpecArgs{
		Containers: corev1.ContainerArray{
			corev1.ContainerArgs{
				Name: pulumi.String("generator"),
				Args: generatorArgs.ToOutput(ctx.Context()).Untyped().(pulumi.StringArrayOutput),
				Image: utils.GetImage(pulumi.String("ledger-generator"), pulumix.Apply2(args.GeneratorVersion, args.Tag, func(generatorVersion string, ledgerVersion string) string {
					if generatorVersion == "" {
						return ledgerVersion
					}
					return generatorVersion
				})),
				ImagePullPolicy: pulumi.String("Always"),
				VolumeMounts: corev1.VolumeMountArray{
					corev1.VolumeMountArgs{
						MountPath: pulumi.String("/scripts"),
						Name:      pulumi.String("scripts"),
						ReadOnly:  pulumi.BoolPtr(true),
					},
				},
			},
		},
		Volumes: corev1.VolumeArray{
			corev1.VolumeArgs{
				Name: pulumi.String("scripts"),
				ConfigMap: &corev1.ConfigMapVolumeSourceArgs{
					Name: scriptConfigMap.Metadata.Name(),
				},
			},
		},
	}

	pulumix.ApplyErr(args.UntilLogID, func(untilLogID uint) (any, error) {
		resourceOptions := []pulumi.ResourceOption{
			pulumi.DeleteBeforeReplace(true),
			pulumi.Parent(cmp),
		}
		if untilLogID > 0 {
			podSpec.RestartPolicy = pulumi.String("OnFailure")
			_, err = batchv1.NewJob(ctx, "generator", &batchv1.JobArgs{
				Metadata: metav1.ObjectMetaArgs{
					Namespace: args.Namespace.
						ToOutput(ctx.Context()).
						Untyped().(pulumi.StringOutput),
					Annotations: pulumi.StringMap{
						"pulumi.com/skipAwait": pulumi.String("true"),
					},
				},
				Spec: batchv1.JobSpecArgs{
					Template: corev1.PodTemplateSpecArgs{
						Spec: podSpec,
					},
				},
			}, resourceOptions...)
			return nil, err
		}

		_, err := appsv1.NewDeployment(ctx, "generator", &appsv1.DeploymentArgs{
			Metadata: metav1.ObjectMetaArgs{
				Namespace: args.Namespace.
					ToOutput(ctx.Context()).
					Untyped().(pulumi.StringOutput),
				Labels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger-generator"),
				},
			},
			Spec: appsv1.DeploymentSpecArgs{
				Replicas: pulumi.Int(1),
				Selector: &metav1.LabelSelectorArgs{
					MatchLabels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger-generator"),
					},
				},
				Template: corev1.PodTemplateSpecArgs{
					Metadata: &metav1.ObjectMetaArgs{
						Labels: pulumi.StringMap{
							"com.formance.stack/app": pulumi.String("ledger-generator"),
						},
					},
					Spec: podSpec,
				},
			},
		}, resourceOptions...)

		return nil, err
	})

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
