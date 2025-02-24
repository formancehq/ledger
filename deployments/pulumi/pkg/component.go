package pulumi_ledger

import (
	"fmt"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
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
	ServerDeployment   *appsv1.Deployment
	WorkerDeployment   *appsv1.Deployment
}

func NewComponent(ctx *pulumi.Context, name string, args *ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	if args == nil {
		args = &ComponentArgs{}
	}
	args.setDefaults()

	cmp.ServerDeployment, err = createAPIDeployment(ctx, cmp, args)
	if err != nil {
		return nil, err
	}

	cmp.WorkerDeployment, err = createWorkerDeployment(ctx, cmp, args)
	if err != nil {
		return nil, err
	}

	_, err = newMigrationJob(ctx, cmp, args)
	if err != nil {
		return nil, err
	}

	service, err := installService(ctx, cmp, *args)
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

	if args.API.Ingress != nil {
		if _, err := installIngress(ctx, cmp, args); err != nil {
			return nil, err
		}
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"deployment-name":      cmp.ServerDeployment.Metadata.Name(),
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
