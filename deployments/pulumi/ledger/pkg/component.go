package pulumi_ledger

import (
	"fmt"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
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
}

func NewLedgerComponent(ctx *pulumi.Context, name string, args *LedgerComponentArgs, opts ...pulumi.ResourceOption) (*LedgerComponent, error) {
	cmp := &LedgerComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	rel, err := helm.NewRelease(ctx, "ledger", &helm.ReleaseArgs{
		Chart:           pulumi.String("../../../helm"),
		Namespace:       pulumix.Cast[pulumi.StringOutput](args.Namespace),
		CreateNamespace: pulumi.BoolPtr(true),
		Timeout:         pulumix.Cast[pulumi.IntOutput](args.Timeout),
		Values: pulumi.Map(map[string]pulumi.Input{
			"image": pulumi.Map{
				"repository": pulumi.String("ghcr.io/formancehq/ledger"),
				"tag":        args.Tag,
				"pullPolicy": args.ImagePullPolicy,
			},
			"postgres": pulumi.Map{
				"uri": args.PostgresURI,
			},
			"debug":                args.Debug,
			"replicaCount":         args.ReplicaCount,
			"experimentalFeatures": args.ExperimentalFeatures,
		}),
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("installing release: %w", err)
	}

	cmp.ServiceName = pulumix.Apply(rel.Status.Name().ToStringPtrOutput(), func(a1 *string) string {
		return *a1
	})
	cmp.ServiceNamespace = pulumix.Apply(rel.Status.Namespace().ToStringPtrOutput(), func(a1 *string) string {
		return *a1
	})
	cmp.ServicePort = pulumix.Val(8080)
	cmp.ServiceInternalURL = pulumix.Apply(pulumi.Sprintf(
		"http://%s.%s.svc.cluster.local:%d",
		cmp.ServiceName,
		cmp.ServiceNamespace,
		cmp.ServicePort,
	), func(a1 string) string {
		return a1
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
