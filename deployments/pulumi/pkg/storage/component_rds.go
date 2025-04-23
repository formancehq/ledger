package storage

import (
	"errors"
	"fmt"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"strings"
)

type RDSComponentArgs struct {
	CreateCluster   *RDSClusterCreateArgs
	Database        pulumix.Input[string]
}

type RDSDatabaseComponent struct {
	pulumi.ResourceState

	Cluster        *rds.Cluster
	Instance       *rds.ClusterInstance
	Database       pulumix.Input[string]
	MasterUsername pulumix.Input[string]
	MasterPassword pulumix.Input[string]
}

func (r *RDSDatabaseComponent) GetOptions() pulumix.Input[map[string]string] {
	return pulumix.Val(map[string]string{})
}

func (r *RDSDatabaseComponent) GetEndpoint() pulumix.Input[string] {
	return r.Cluster.Endpoint
}

func (r *RDSDatabaseComponent) GetUsername() pulumix.Input[string] {
	return r.MasterUsername
}

func (r *RDSDatabaseComponent) GetPassword() pulumix.Input[string] {
	return r.MasterPassword
}

func (r *RDSDatabaseComponent) GetDatabase() pulumix.Input[string] {
	return r.Database
}

func (r *RDSDatabaseComponent) GetPort() pulumix.Input[int] {
	return r.Cluster.Port
}

func newRDSDatabaseComponent(ctx *pulumi.Context, args *RDSComponentArgs, opts ...pulumi.ResourceOption) (*RDSDatabaseComponent, error) {
	cmp := &RDSDatabaseComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:RDS", "storage", cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Database = args.Database
	cmp.MasterUsername = args.CreateCluster.MasterUsername
	cmp.MasterPassword = args.CreateCluster.MasterPassword
	cmp.Cluster, err = rds.NewCluster(
		ctx,
		"cluster",
		&rds.ClusterArgs{
			DbSubnetGroupName: pulumix.ApplyErr(args.CreateCluster.UseSubnetGroupName, func(v string) (string, error) {
				if v == "" {
					return "", errors.New("dbSubnetGroupName is required")
				}
				return v, nil
			}).
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
			Engine: args.CreateCluster.Engine.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
			EngineVersion: args.CreateCluster.EngineVersion.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
			SkipFinalSnapshot: pulumi.Bool(true),
			SnapshotIdentifier: args.CreateCluster.SnapshotIdentifier.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringPtrOutput),
			MasterUsername: args.CreateCluster.MasterUsername.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
			MasterPassword: args.CreateCluster.MasterPassword.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.StringOutput),
			IamDatabaseAuthenticationEnabled: pulumi.BoolPtr(false),
			ClusterIdentifier: pulumi.Sprintf(
				"%s-%s-%s",
				ctx.Organization(),
				ctx.Project(),
				strings.Replace(ctx.Stack(), ".", "-", -1),
			),
			PerformanceInsightsEnabled: args.CreateCluster.PerformanceInsightsEnabled.
				ToOutput(ctx.Context()).
				Untyped().(pulumi.BoolOutput),
		},
		pulumi.Parent(cmp),
		// Ignore these changes to avoid recreating the cluster on stack rename
		pulumi.IgnoreChanges([]string{
			"snapshotIdentifier",
			"clusterIdentifier",
			"masterPassword",
			"masterUsername",
		}),
		pulumi.RetainOnDelete(args.CreateCluster.RetainsOnDelete),
	)
	if err != nil {
		return nil, err
	}

	_, err = rds.NewClusterInstance(ctx, "primary", &rds.ClusterInstanceArgs{
		ClusterIdentifier: cmp.Cluster.ClusterIdentifier,
		InstanceClass: pulumix.Apply(args.CreateCluster.InstanceClass, func(instanceType rds.InstanceType) string {
			return string(instanceType)
		}).
			ToOutput(ctx.Context()).
			Untyped().(pulumi.StringOutput),
		Engine:           pulumi.String("aurora-postgresql"),
		ApplyImmediately: pulumi.BoolPtr(true),
		Tags: pulumi.StringMap{
			"Name": pulumi.String("primary"),
		},
		PerformanceInsightsEnabled: args.CreateCluster.PerformanceInsightsEnabled.
			ToOutput(ctx.Context()).
			Untyped().(pulumi.BoolOutput),
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating RDS instance: %w", err)
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}

var _ databaseComponent = (*RDSDatabaseComponent)(nil)
