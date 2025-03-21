package ledger

import (
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/devbox"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/generator"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/provision"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type ComponentArgs struct {
	common.CommonArgs
	Timeout       pulumix.Input[int]
	InstallDevBox pulumix.Input[bool]
	Storage       storage.Args
	Ingress       *api.IngressArgs
	API           api.Args
	Provision     provision.Args
	Generator     *generator.Args
}

func (args *ComponentArgs) SetDefaults() {
	args.Storage.SetDefaults()
	args.CommonArgs.SetDefaults()
	args.API.SetDefaults()
	if args.Generator != nil {
		args.Generator.SetDefaults()
	}
}

type Component struct {
	pulumi.ResourceState

	API        *api.Component
	Storage    *storage.Component
	Namespace  *corev1.Namespace
	Devbox     *devbox.Component
	Provision  *provision.Component
	Generator  *generator.Component
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	args.SetDefaults()

	options := []pulumi.ResourceOption{
		pulumi.Parent(cmp),
	}

	cmp.Namespace, err = corev1.NewNamespace(ctx, "namespace", &corev1.NamespaceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Name: args.Namespace.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
		},
	}, options...)
	if err != nil {
		return nil, err
	}

	options = append(options, pulumi.DependsOn([]pulumi.Resource{
		cmp.Namespace,
	}))

	cmp.Storage, err = storage.NewComponent(ctx, "storage", storage.ComponentArgs{
		CommonArgs: args.CommonArgs,
		Args:       args.Storage,
	}, options...)
	if err != nil {
		return nil, err
	}

	options = append(options, pulumi.DependsOn([]pulumi.Resource{
		// don't depend on storage since it includes migrations
		// we just need the database to be up, migrations will be run in background
		// we also need to have credentials ready for the API and Worker
		cmp.Storage.DatabaseComponent,
		cmp.Storage.Credentials,
		cmp.Storage.Service,
	}))

	cmp.API, err = api.NewComponent(ctx, "api", api.ComponentArgs{
		CommonArgs: args.CommonArgs,
		Args:       args.API,
		Storage:    cmp.Storage,
		Ingress:    args.Ingress,
	}, options...)
	if err != nil {
		return nil, err
	}

	if len(args.Provision.Ledgers) > 0 {
		cmp.Provision, err = provision.NewComponent(ctx, "provisioner", provision.ComponentArgs{
			CommonArgs: args.CommonArgs,
			API:        cmp.API,
			Args:       args.Provision,
		}, options...)
		if err != nil {
			return nil, err
		}
	}

	if args.Generator != nil {
		cmp.Generator, err = generator.NewComponent(ctx, "generator", generator.ComponentArgs{
			CommonArgs: args.CommonArgs,
			API:        cmp.API,
			Args:       *args.Generator,
		}, append(options, pulumi.DependsOn([]pulumi.Resource{
			cmp.Provision,
		}))...)
		if err != nil {
			return nil, err
		}
	}

	installDevBox, err := internals.UnsafeAwaitOutput(ctx.Context(), args.InstallDevBox.ToOutput(ctx.Context()))
	if err != nil {
		return nil, err
	}
	if installDevBox.Value != nil && installDevBox.Value.(bool) {
		cmp.Devbox, err = devbox.NewComponent(ctx, "devbox", devbox.ComponentArgs{
			CommonArgs: args.CommonArgs,
			Storage:    cmp.Storage,
			API:        cmp.API,
		}, options...)
		if err != nil {
			return nil, err
		}
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"deployment-name": cmp.API.Deployment.Metadata.Name(),
	}); err != nil {
		return nil, fmt.Errorf("registering resource outputs: %w", err)
	}

	return cmp, nil
}
