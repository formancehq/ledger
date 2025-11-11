package worker

import (
	"fmt"

	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"

	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
)

type Args struct {
	TerminationGracePeriodSeconds pulumix.Input[*int]
}

func (args *Args) SetDefaults() {
	if args.TerminationGracePeriodSeconds == nil {
		args.TerminationGracePeriodSeconds = pulumix.Val((*int)(nil))
	}
}

type Component struct {
	pulumi.ResourceState

	Deployment *appsv1.Deployment
	Service    *corev1.Service
}

type ComponentArgs struct {
	common.CommonArgs
	Args
	Database *storage.Component
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Worker", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Deployment, err = createDeployment(ctx, args, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating deployment: %w", err)
	}

	cmp.Service, err = createService(ctx, args, cmp.Deployment, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating service: %w", err)
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
