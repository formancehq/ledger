package pulumi_ledger

import (
	"fmt"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

var ErrPostgresURIRequired = fmt.Errorf("postgresURI is required")

type Component struct {
	pulumi.ResourceState

	ServiceName        pulumix.Output[string]
	ServiceNamespace   pulumix.Output[string]
	ServicePort        pulumix.Output[int]
	ServiceInternalURL pulumix.Output[string]
	Migrations         pulumix.Output[*batchv1.Job]
}

type ComponentArgs struct {
	Namespace            pulumix.Input[string]
	Timeout              pulumix.Input[int]
	Tag                  pulumix.Input[string]
	ImagePullPolicy      pulumix.Input[string]
	PostgresURI          pulumix.Input[string]
	Debug                pulumix.Input[bool]
	ReplicaCount         pulumix.Input[int]
	ExperimentalFeatures pulumix.Input[bool]
	GracePeriod          pulumix.Input[string]
	AutoUpgrade          pulumix.Input[bool]
	WaitUpgrade          pulumix.Input[bool]
}

func NewComponent(ctx *pulumi.Context, name string, args *ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	namespace := pulumix.Val[string]("")
	if args.Namespace != nil {
		namespace = args.Namespace.ToOutput(ctx.Context())
	}

	if args.PostgresURI == nil {
		return nil, ErrPostgresURIRequired
	}
	postgresURI := pulumix.ApplyErr(args.PostgresURI, func(postgresURI string) (string, error) {
		if postgresURI == "" {
			return "", ErrPostgresURIRequired
		}

		return postgresURI, nil
	})

	debug := pulumix.Val(true)
	if args.Debug != nil {
		debug = args.Debug.ToOutput(ctx.Context())
	}

	experimentalFeatures := pulumix.Val(true)
	if args.ExperimentalFeatures != nil {
		experimentalFeatures = args.ExperimentalFeatures.ToOutput(ctx.Context())
	}

	gracePeriod := pulumix.Val("0s")
	if args.GracePeriod != nil {
		gracePeriod = args.GracePeriod.ToOutput(ctx.Context())
	}

	tag := pulumix.Val("latest")
	if args.Tag != nil {
		tag = pulumix.Apply(args.Tag, func(tag string) string {
			if tag == "" {
				return "latest"
			}
			return tag
		})
	}
	ledgerImage := pulumi.Sprintf("ghcr.io/formancehq/ledger:%s", tag)

	autoUpgrade := pulumix.Val(true)
	if args.AutoUpgrade != nil {
		autoUpgrade = args.AutoUpgrade.ToOutput(ctx.Context())
	}

	waitUpgrade := pulumix.Val(true)
	if args.WaitUpgrade != nil {
		waitUpgrade = args.WaitUpgrade.ToOutput(ctx.Context())
	}

	imagePullPolicy := pulumix.Val("")
	if args.ImagePullPolicy != nil {
		imagePullPolicy = args.ImagePullPolicy.ToOutput(ctx.Context())
	}

	deployment, err := appsv1.NewDeployment(ctx, "ledger", &appsv1.DeploymentArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: namespace.Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger"),
			},
		},
		Spec: appsv1.DeploymentSpecArgs{
			Replicas: pulumi.Int(1),
			Selector: &metav1.LabelSelectorArgs{
				MatchLabels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger"),
				},
			},
			Template: &corev1.PodTemplateSpecArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Labels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger"),
					},
				},
				Spec: corev1.PodSpecArgs{
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name:            pulumi.String("ledger"),
							Image:           ledgerImage,
							ImagePullPolicy: imagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
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
							},
							ReadinessProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
							},
							Env: corev1.EnvVarArray{
								corev1.EnvVarArgs{
									Name:  pulumi.String("POSTGRES_URI"),
									Value: postgresURI.Untyped().(pulumi.StringOutput),
								},
								corev1.EnvVarArgs{
									Name:  pulumi.String("BIND"),
									Value: pulumi.String(":8080"),
								},
								corev1.EnvVarArgs{
									Name:  pulumi.String("DEBUG"),
									Value: boolToString(debug).Untyped().(pulumi.StringOutput),
								},
								corev1.EnvVarArgs{
									Name:  pulumi.String("EXPERIMENTAL_FEATURES"),
									Value: boolToString(experimentalFeatures).Untyped().(pulumi.StringOutput),
								},
								// https://freecontent.manning.com/handling-client-requests-properly-with-kubernetes/
								corev1.EnvVarArgs{
									Name:  pulumi.String("GRACE_PERIOD"),
									Value: gracePeriod.Untyped().(pulumi.StringOutput),
								},
								corev1.EnvVarArgs{
									Name: pulumi.String("AUTO_UPGRADE"),
									Value: boolToString(pulumix.Apply2Err(autoUpgrade, waitUpgrade, func(autoUpgrade, waitUpgrade bool) (bool, error) {
										if waitUpgrade && !autoUpgrade {
											return false, fmt.Errorf("waitUpgrade requires autoUpgrade to be true")
										}
										return true, nil
									})).Untyped().(pulumi.StringOutput),
								},
							},
						},
					},
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	cmp.Migrations = pulumix.ApplyErr(waitUpgrade, func(waitUpgrade bool) (*batchv1.Job, error) {
		if !waitUpgrade {
			return nil, nil
		}
		return batchv1.NewJob(ctx, "wait-migration-completion", &batchv1.JobArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Namespace: namespace.Untyped().(pulumi.StringOutput),
			},
			Spec: batchv1.JobSpecArgs{
				Template: corev1.PodTemplateSpecArgs{
					Spec: corev1.PodSpecArgs{
						RestartPolicy: pulumi.String("OnFailure"),
						Containers: corev1.ContainerArray{
							corev1.ContainerArgs{
								Name: pulumi.String("check"),
								Args: pulumi.StringArray{
									pulumi.String("migrate"),
								},
								Image:           ledgerImage,
								ImagePullPolicy: imagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
								Env: corev1.EnvVarArray{
									corev1.EnvVarArgs{
										Name:  pulumi.String("POSTGRES_URI"),
										Value: postgresURI.Untyped().(pulumi.StringOutput),
									},
									corev1.EnvVarArgs{
										Name:  pulumi.String("DEBUG"),
										Value: boolToString(debug).Untyped().(pulumi.StringOutput),
									},
								},
							},
						},
					},
				},
			},
		}, pulumi.Parent(cmp))
	})

	service, err := corev1.NewService(ctx, "ledger", &corev1.ServiceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: namespace.Untyped().(pulumi.StringOutput),
		},
		Spec: &corev1.ServiceSpecArgs{
			Selector: deployment.Spec.Selector().MatchLabels(),
			Type:     pulumi.String("ClusterIP"),
			Ports: corev1.ServicePortArray{
				corev1.ServicePortArgs{
					Port:       pulumi.Int(8080),
					TargetPort: pulumi.Int(8080),
					Protocol:   pulumi.String("TCP"),
					Name:       pulumi.String("http"),
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	cmp.ServiceName = pulumix.Apply(service.Metadata.Name().ToStringPtrOutput(), func(name *string) string {
		if name == nil {
			return ""
		}
		return *name
	})
	cmp.ServiceNamespace = pulumix.Apply(service.Metadata.Namespace().ToStringPtrOutput(), func(namespace *string) string {
		if namespace == nil {
			return ""
		}
		return *namespace
	})
	cmp.ServicePort = pulumix.Val(8080)
	cmp.ServiceInternalURL = pulumix.Apply(pulumi.Sprintf(
		"http://%s.%s.svc.cluster.local:%d",
		cmp.ServiceName,
		cmp.ServiceNamespace,
		cmp.ServicePort,
	), func(url string) string {
		return url
	})

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"service-name":         cmp.ServiceName,
		"service-namespace":    cmp.ServiceNamespace,
		"service-port":         cmp.ServicePort,
		"service-internal-url": cmp.ServiceInternalURL,
	}); err != nil {
		return nil, fmt.Errorf("registering resource outputs: %w", err)
	}

	return cmp, nil
}

func boolToString(output pulumix.Input[bool]) pulumix.Output[string] {
	return pulumix.Apply(output, func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	})
}
