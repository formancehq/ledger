package main

import (
	"github.com/formancehq/ledger/tools/dataset/pkg"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cmp, err := pulumi_dataset_init_stack.NewDatasetComponent(ctx, ctx.Stack(), &pulumi_dataset_init_stack.DatasetComponentArgs{
			Namespace: pulumi.String(config.Get(ctx, "namespace")),
			RDS: pulumi_dataset_init_stack.RDSComponentArgs{
				InstanceClass:          pulumi.String(config.Get(ctx, "rds-instance-class")),
				DBSubnetGroupName:      pulumi.String(config.Require(ctx, "rds-db-subnet-group-name")),
				InitializationSnapshot: pulumi.String(config.Get(ctx, "rds-initialization-snapshot")),
			},
			GeneratorVersion: pulumi.String(config.Get(ctx, "generator-version")),
			UntilLogID:       pulumi.Int(config.GetInt(ctx, "until-log-id")),
			CreateSnapshot:   pulumi.Bool(config.GetBool(ctx, "create-snapshot")),
			Script:           pulumi.String(config.Require(ctx, "script")),
		})

		ctx.Export("ledger-url", cmp.Ledger.ServiceInternalURL)
		ctx.Export("rds-cluster-identifier", cmp.RDS.Cluster.ClusterIdentifier)
		ctx.Export("snapshot-identifier", pulumix.Apply(cmp.Snapshot, func(snapshot *rds.ClusterSnapshot) pulumi.IDPtrOutput {
			if snapshot != nil {
				return snapshot.ID().ToIDPtrOutput()
			}
			return pulumi.IDPtrOutput{}
		}))

		return err
	})
}
