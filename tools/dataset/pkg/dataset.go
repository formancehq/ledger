package pulumi_dataset_init_stack

import (
	"context"
	"fmt"
	pulumi_ledger "github.com/formancehq/ledger/deployments/pulumi/ledger/pkg"
	pulumi_generator "github.com/formancehq/ledger/tools/generator/pulumi/pkg"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type DatasetComponent struct {
	pulumi.ResourceState
	RDS       *RDSComponent
	Ledger    *pulumi_ledger.LedgerComponent
	Generator pulumix.Output[*pulumi_generator.GeneratorComponent]
	Snapshot  pulumix.Output[*rds.ClusterSnapshot]
}

type DatasetComponentArgs struct {
	Namespace        pulumix.Input[string]
	RDS              RDSComponentArgs
	LedgerVersion    pulumix.Input[string]
	GeneratorVersion pulumix.Input[string]
	UntilLogID       pulumix.Input[int]
	CreateSnapshot   pulumix.Input[bool]
	Script           pulumix.Input[string]
}

func NewDatasetComponent(ctx *pulumi.Context, name string, args *DatasetComponentArgs, opts ...pulumi.ResourceOption) (*DatasetComponent, error) {
	cmp := &DatasetComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Dataset", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.RDS, err = NewRDSComponent(ctx, "rds", &RDSComponentArgs{
		InstanceClass:     args.RDS.InstanceClass,
		DBSubnetGroupName: args.RDS.DBSubnetGroupName,
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating RDS component: %w", err)
	}

	cmp.Ledger, err = pulumi_ledger.NewLedgerComponent(ctx, "ledger", &pulumi_ledger.LedgerComponentArgs{
		Timeout:         pulumi.Int(30),
		Tag:             args.LedgerVersion,
		ImagePullPolicy: pulumi.String("Always"),
		PostgresURI: pulumi.Sprintf(
			"postgres://%s:%s@%s:%d/postgres?sslmode=disable",
			cmp.RDS.MasterUsername,
			cmp.RDS.MasterPassword,
			cmp.RDS.Endpoint,
			cmp.RDS.Port,
		),
		ExperimentalFeatures: pulumi.Bool(true),
		Namespace:            args.Namespace,
	}, pulumi.Transforms([]pulumi.ResourceTransform{
		// Update relative location of the helm chart
		func(context context.Context, args *pulumi.ResourceTransformArgs) *pulumi.ResourceTransformResult {
			if args.Type == "kubernetes:helm.sh/v3:Release" {
				args.Props["chart"] = pulumi.String("../../deployments/helm")
			}

			return &pulumi.ResourceTransformResult{
				Props: args.Props,
			}
		},
	}), pulumi.Parent(cmp))

	// todo: check actual log on the ledger to avoid running the generator if not necessary

	cmp.Generator = pulumix.ApplyErr(args.UntilLogID, func(untilLogID int) (*pulumi_generator.GeneratorComponent, error) {
		if untilLogID == 0 {
			return nil, nil
		}

		return pulumi_generator.NewGeneratorComponent(ctx, "generator", &pulumi_generator.GeneratorComponentArgs{
			LedgerURL:  cmp.Ledger.ServiceInternalURL,
			Version:    args.GeneratorVersion,
			UntilLogID: pulumi.Int(untilLogID),
			Namespace:  args.Namespace,
			Script:     args.Script,
		}, pulumi.Parent(cmp))
	})

	cmp.Snapshot = pulumix.Apply2Err(args.CreateSnapshot, cmp.Generator, func(createSnapshot bool, generator *pulumi_generator.GeneratorComponent) (*rds.ClusterSnapshot, error) {
		if !createSnapshot {
			return nil, nil
		}

		resourceOptions := []pulumi.ResourceOption{
			pulumi.Parent(cmp),
			pulumi.RetainOnDelete(true),
		}

		if generator != nil {
			resourceOptions = append(resourceOptions, pulumi.DependsOn([]pulumi.Resource{generator}))
		}

		return rds.NewClusterSnapshot(ctx, fmt.Sprintf("snapshot-%d", args.UntilLogID), &rds.ClusterSnapshotArgs{
			DbClusterIdentifier:         cmp.RDS.ClusterIdentifier.Untyped().(pulumi.StringOutput),
			DbClusterSnapshotIdentifier: pulumi.Sprintf("%s-%d", cmp.RDS.ClusterIdentifier, args.UntilLogID),
		},
			resourceOptions...,
		)
	})

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"snapshot-identifier": pulumix.Apply(cmp.Snapshot, func(s *rds.ClusterSnapshot) pulumi.IDPtrOutput {
			if s == nil {
				return pulumi.IDPtrOutput{}
			}
			return s.ID().ToIDPtrOutput()
		}),
	}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
