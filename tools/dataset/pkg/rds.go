package pulumi_dataset_init_stack

import (
	"errors"
	"fmt"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"strings"
)

type RDSComponent struct {
	pulumi.ResourceState

	MasterUsername  pulumix.Output[string]
	MasterPassword  pulumix.Output[string]
	Cluster         *rds.Cluster
	PrimaryInstance *rds.ClusterInstance
}

type RDSComponentArgs struct {
	InstanceClass          pulumix.Input[string]
	DBSubnetGroupName      pulumix.Input[string]
	InitializationSnapshot pulumix.Input[string]
}

func NewRDSComponent(ctx *pulumi.Context, name string, args *RDSComponentArgs, opts ...pulumi.ResourceOption) (*RDSComponent, error) {
	cmp := &RDSComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:RDS", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	masterUsername := pulumix.Val("root")
	masterPassword := pulumix.Val("password")

	initializationSnapshot := pulumix.Val[*string](nil)
	if args.InitializationSnapshot != nil {
		initializationSnapshot = pulumix.Apply(args.InitializationSnapshot, func(initializationSnapshot string) *string {
			if initializationSnapshot == "" {
				return nil
			}

			return &initializationSnapshot
		})
	}

	cmp.Cluster, err = rds.NewCluster(ctx, "default", &rds.ClusterArgs{
		DbSubnetGroupName: pulumix.ApplyErr(args.DBSubnetGroupName, func(v string) (string, error) {
			if v == "" {
				return "", errors.New("dbSubnetGroupName is required")
			}
			return v, nil
		}).Untyped().(pulumi.StringOutput),
		Engine:             pulumi.String("aurora-postgresql"),
		EngineVersion:      pulumi.String("16"),
		SkipFinalSnapshot:  pulumi.Bool(true),
		SnapshotIdentifier: initializationSnapshot.Untyped().(pulumi.StringPtrOutput),
		MasterUsername: masterUsername.ApplyT(func(v string) string {
			return v
		}).(pulumi.StringOutput),
		MasterPassword: masterPassword.ApplyT(func(v string) string {
			return v
		}).(pulumi.StringOutput),
		ClusterIdentifier: pulumi.String(ctx.Project() + "-" + strings.Replace(ctx.Stack(), ".", "-", -1)),
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating RDS cluster: %w", err)
	}

	cmp.PrimaryInstance, err = rds.NewClusterInstance(ctx, "primary", &rds.ClusterInstanceArgs{
		ClusterIdentifier: cmp.Cluster.ClusterIdentifier,
		InstanceClass: pulumix.Apply(args.InstanceClass, func(v string) string {
			if v == "" {
				return string(rds.InstanceType_T3_Medium)
			}
			return v
		}).Untyped().(pulumi.StringOutput),
		Engine:           pulumi.String("aurora-postgresql"),
		ApplyImmediately: pulumi.BoolPtr(true),
		Tags: pulumi.StringMap{
			"Name": pulumi.String("primary"),
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, fmt.Errorf("creating RDS instance: %w", err)
	}

	cmp.MasterUsername = masterUsername
	cmp.MasterPassword = masterPassword

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"masterUsername": masterUsername,
		"masterPassword": masterPassword,
	}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
