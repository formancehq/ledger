package pulumi_ledger

import (
	"fmt"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	v3 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	v2 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type LedgerComponent struct {
	pulumi.ResourceState

	ServiceName        pulumix.Output[string]
	ServiceNamespace   pulumix.Output[string]
	ServicePort        pulumix.Output[int]
	ServiceInternalURL pulumix.Output[string]
}

type LedgerComponentArgs struct {
	Namespace            pulumix.Input[string]
	Timeout              pulumix.Input[int]
	Tag                  pulumix.Input[string]
	ImagePullPolicy      pulumix.Input[string]
	PostgresURI          pulumix.Input[string]
	Debug                pulumix.Input[bool]
	ReplicaCount         pulumix.Input[int]
	ExperimentalFeatures pulumix.Input[bool]
	GracePeriod          pulumix.Input[string]
}

func NewLedgerComponent(ctx *pulumi.Context, name string, args *LedgerComponentArgs, opts ...pulumi.ResourceOption) (*LedgerComponent, error) {
	cmp := &LedgerComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	namespace := pulumix.Val[string]("")
	if args.Namespace != nil {
		namespace = args.Namespace.ToOutput(ctx.Context())
	}

	postgresURI := pulumix.ApplyErr(args.PostgresURI, func(postgresURI string) (string, error) {
		if postgresURI == "" {
			return "", fmt.Errorf("postgresURI is required")
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

	deployment, err := v1.NewDeployment(ctx, "ledger", &v1.DeploymentArgs{
		Metadata: &v2.ObjectMetaArgs{
			Namespace: namespace.Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger"),
			},
		},
		Spec: v1.DeploymentSpecArgs{
			Replicas: pulumi.Int(1),
			Selector: &v2.LabelSelectorArgs{
				MatchLabels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger"),
				},
			},
			Template: &v3.PodTemplateSpecArgs{
				Metadata: &v2.ObjectMetaArgs{
					Labels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger"),
					},
				},
				Spec: v3.PodSpecArgs{
					Containers: v3.ContainerArray{
						v3.ContainerArgs{
							Name:  pulumi.String("ledger"),
							Image: pulumi.Sprintf("ghcr.io/formancehq/ledger:%s", tag),
							Args: pulumi.StringArray{
								pulumi.String("serve"),
							},
							Ports: v3.ContainerPortArray{
								v3.ContainerPortArgs{
									ContainerPort: pulumi.Int(8080),
									Name:          pulumi.String("http"),
									Protocol:      pulumi.String("TCP"),
								},
							},
							LivenessProbe: v3.ProbeArgs{
								HttpGet: v3.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
							},
							ReadinessProbe: v3.ProbeArgs{
								HttpGet: v3.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
							},
							Env: v3.EnvVarArray{
								v3.EnvVarArgs{
									Name:  pulumi.String("POSTGRES_URI"),
									Value: postgresURI.Untyped().(pulumi.StringOutput),
								},
								v3.EnvVarArgs{
									Name:  pulumi.String("BIND"),
									Value: pulumi.String(":8080"),
								},
								v3.EnvVarArgs{
									Name: pulumi.String("DEBUG"),
									Value: pulumix.Apply(debug, func(debug bool) string {
										if debug {
											return "true"
										}
										return "false"
									}).Untyped().(pulumi.StringOutput),
								},
								v3.EnvVarArgs{
									Name: pulumi.String("EXPERIMENTAL_FEATURES"),
									Value: pulumix.Apply(experimentalFeatures, func(experimentalFeatures bool) string {
										if experimentalFeatures {
											return "true"
										}
										return "false"
									}).Untyped().(pulumi.StringOutput),
								},
								// https://freecontent.manning.com/handling-client-requests-properly-with-kubernetes/
								v3.EnvVarArgs{
									Name:  pulumi.String("GRACE_PERIOD"),
									Value: gracePeriod.Untyped().(pulumi.StringOutput),
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

	service, err := v3.NewService(ctx, "ledger", &v3.ServiceArgs{
		Metadata: &v2.ObjectMetaArgs{
			Namespace: namespace.Untyped().(pulumi.StringOutput),
		},
		Spec: &v3.ServiceSpecArgs{
			Selector: deployment.Spec.Selector().MatchLabels(),
			Type:     pulumi.String("ClusterIP"),
			Ports: v3.ServicePortArray{
				v3.ServicePortArgs{
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

	cmp.ServiceName = pulumix.Apply(service.Metadata.Name().ToStringPtrOutput(), func(a1 *string) string {
		return *a1
	})
	cmp.ServiceNamespace = pulumix.Apply(service.Metadata.Namespace().ToStringPtrOutput(), func(a1 *string) string {
		return *a1
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
