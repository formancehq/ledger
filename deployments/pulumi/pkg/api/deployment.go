package api

import (
	"fmt"
	common "github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"time"
)

type Args struct {
	ReplicaCount                     pulumix.Input[*int]
	GracePeriod                      pulumix.Input[time.Duration]
	BallastSizeInBytes               pulumix.Input[int]
	NumscriptCacheMaxCount           pulumix.Input[int]
	BulkMaxSize                      pulumix.Input[int]
	BulkParallel                     pulumix.Input[int]
	TerminationGracePeriodSeconds    pulumix.Input[*int]
	ExperimentalFeatures             pulumix.Input[bool]
	ExperimentalNumscriptInterpreter pulumix.Input[bool]
}

func (args *Args) SetDefaults() {
	if args.GracePeriod == nil {
		args.GracePeriod = pulumix.Val(time.Duration(0))
	}
	if args.ExperimentalFeatures == nil {
		args.ExperimentalFeatures = pulumi.Bool(false)
	}
	if args.ExperimentalNumscriptInterpreter == nil {
		args.ExperimentalNumscriptInterpreter = pulumi.Bool(false)
	}
	if args.NumscriptCacheMaxCount == nil {
		args.NumscriptCacheMaxCount = pulumi.Int(0)
	}
	if args.BulkParallel == nil {
		args.BulkParallel = pulumi.Int(0)
	}
	if args.BallastSizeInBytes == nil {
		args.BallastSizeInBytes = pulumi.Int(0)
	}
	if args.BulkMaxSize == nil {
		args.BulkMaxSize = pulumi.Int(0)
	}
	if args.TerminationGracePeriodSeconds == nil {
		args.TerminationGracePeriodSeconds = pulumix.Val((*int)(nil))
	}
	if args.ReplicaCount == nil {
		args.ReplicaCount = pulumix.Val((*int)(nil))
	}
}

type createDeploymentArgs struct {
	common.CommonArgs
	Args
	Database *storage.Component
}

func createDeployment(ctx *pulumi.Context, args createDeploymentArgs, resourceOptions ...pulumi.ResourceOption) (*appsv1.Deployment, error) {
	envVars := corev1.EnvVarArray{}
	envVars = append(envVars,
		corev1.EnvVarArgs{
			Name:  pulumi.String("BIND"),
			Value: pulumi.String(":8080"),
		},
		corev1.EnvVarArgs{
			Name:  pulumi.String("DEBUG"),
			Value: utils.BoolToString(args.Debug).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("BULK_MAX_SIZE"),
			Value: pulumix.Apply(args.BulkMaxSize, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("BALLAST_SIZE"),
			Value: pulumix.Apply(args.BallastSizeInBytes, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("BULK_PARALLEL"),
			Value: pulumix.Apply(args.BulkParallel, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("NUMSCRIPT_CACHE_MAX_COUNT"),
			Value: pulumix.Apply(args.NumscriptCacheMaxCount, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name:  pulumi.String("EXPERIMENTAL_NUMSCRIPT_INTERPRETER"),
			Value: utils.BoolToString(args.ExperimentalNumscriptInterpreter).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name:  pulumi.String("EXPERIMENTAL_FEATURES"),
			Value: utils.BoolToString(args.ExperimentalFeatures).Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name: pulumi.String("GRACE_PERIOD"),
			Value: pulumix.Apply(args.GracePeriod, time.Duration.String).
				Untyped().(pulumi.StringOutput),
		},
	)

	envVars = append(envVars, args.Database.GetEnvVars()...)
	if otel := args.Monitoring; otel != nil {
		envVars = append(envVars, args.Monitoring.GetEnvVars(ctx)...)
	}

	return appsv1.NewDeployment(ctx, "ledger-api", &appsv1.DeploymentArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger-api"),
			},
		},
		Spec: appsv1.DeploymentSpecArgs{
			Replicas: args.ReplicaCount.ToOutput(ctx.Context()).Untyped().(pulumi.IntPtrOutput),
			Selector: &metav1.LabelSelectorArgs{
				MatchLabels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger-api"),
				},
			},
			Template: &corev1.PodTemplateSpecArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Labels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger-api"),
					},
				},
				Spec: corev1.PodSpecArgs{
					TerminationGracePeriodSeconds: args.TerminationGracePeriodSeconds.ToOutput(ctx.Context()).Untyped().(pulumi.IntPtrOutput),
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name:            pulumi.String("ledger-api"),
							Image:           utils.GetMainImage(args.Tag),
							ImagePullPolicy: args.ImagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
							Args: pulumi.StringArray{
								pulumi.String("serve"),
							},
							Ports: corev1.ContainerPortArray{
								corev1.ContainerPortArgs{
									ContainerPort: pulumi.Int(8080),
									Name:          pulumi.String("http"),
									Protocol:      pulumi.String("TCP"),
								},
							},
							LivenessProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
								FailureThreshold: pulumi.Int(1),
								PeriodSeconds:    pulumi.Int(60),
								TimeoutSeconds:   pulumi.IntPtr(3),
							},
							ReadinessProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
								FailureThreshold: pulumi.Int(1),
								PeriodSeconds:    pulumi.Int(60),
								TimeoutSeconds:   pulumi.IntPtr(3),
							},
							StartupProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
								PeriodSeconds:       pulumi.Int(5),
								InitialDelaySeconds: pulumi.IntPtr(2),
								TimeoutSeconds:      pulumi.IntPtr(3),
							},
							Env: envVars,
						},
					},
				},
			},
		},
	}, resourceOptions...)
}
