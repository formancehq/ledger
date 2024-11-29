package pulumi_ledger

import (
	"fmt"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type LedgerComponent struct {
	pulumi.ResourceState

	ServiceName        pulumi.StringPtrOutput
	ServiceNamespace   pulumi.StringPtrOutput
	ServicePort        pulumi.IntPtrOutput
	ServiceInternalURL pulumi.StringOutput
}

type LedgerComponentArgs struct {
	Namespace            pulumi.StringInput
	Timeout              pulumi.IntInput
	Tag                  pulumi.StringInput
	ImagePullPolicy      pulumi.StringInput
	PostgresURI          pulumi.StringInput
	Debug                pulumi.BoolInput
	ReplicaCount         pulumi.IntInput
	ExperimentalFeatures pulumi.BoolInput
}

func NewLedgerComponent(ctx *pulumi.Context, name string, args *LedgerComponentArgs, opts ...pulumi.ResourceOption) (*LedgerComponent, error) {
	cmp := &LedgerComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Deployment", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	rel, err := helm.NewRelease(ctx, "ledger", &helm.ReleaseArgs{
		Chart:           pulumi.String("../../../helm"),
		Namespace:       args.Namespace,
		CreateNamespace: pulumi.BoolPtr(true),
		Timeout:         args.Timeout,
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

	cmp.ServiceName = rel.Status.Name()
	cmp.ServiceNamespace = rel.Status.Namespace()
	cmp.ServicePort = pulumi.IntPtr(8080).ToIntPtrOutput()
	cmp.ServiceInternalURL = pulumi.Sprintf(
		"http://%s.%s.svc.cluster.local:%d",
		cmp.ServiceName.Elem(),
		cmp.ServiceNamespace.Elem(),
		cmp.ServicePort.Elem(),
	)

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
