package storage

import (
	"errors"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type RDSClusterCreateArgs struct {
	UseSubnetGroupName         pulumix.Input[string]
	MasterUsername             pulumix.Input[string]
	MasterPassword             pulumix.Input[string]
	SnapshotIdentifier         pulumix.Input[*string]
	PerformanceInsightsEnabled pulumix.Input[bool]
	InstanceClass              pulumix.Input[rds.InstanceType]
	Engine                     pulumix.Input[string]
	EngineVersion              pulumix.Input[string]
}

func (a *RDSClusterCreateArgs) SetDefaults() {
	if a.Engine == nil {
		a.Engine = pulumix.Val("")
	}
	a.Engine = pulumix.Apply(a.Engine, func(engine string) string {
		if engine == "" {
			return "aurora-postgresql"
		}
		return engine
	})
	if a.EngineVersion == nil {
		a.EngineVersion = pulumix.Val("")
	}
	a.EngineVersion = pulumix.Apply(a.EngineVersion, func(engineVersion string) string {
		if engineVersion == "" {
			return "16"
		}
		return engineVersion
	})

	if a.MasterUsername == nil {
		a.MasterUsername = pulumix.Val("")
	}
	a.MasterUsername = pulumix.Apply(a.MasterUsername, func(username string) string {
		if username == "" {
			return "root"
		}
		return username
	})
	if a.MasterPassword == nil {
		a.MasterPassword = pulumix.Val("")
	}
	a.MasterPassword = pulumix.Apply(a.MasterPassword, func(password string) string {
		if password == "" {
			return "password"
		}
		return password
	})
	if a.PerformanceInsightsEnabled == nil {
		a.PerformanceInsightsEnabled = pulumix.Val(false)
	}
	if a.SnapshotIdentifier == nil {
		a.SnapshotIdentifier = pulumix.Val[*string](nil)
	}
	if a.InstanceClass == nil {
		a.InstanceClass = pulumix.Val(rds.InstanceType_T3_Medium)
	}
	a.InstanceClass = pulumix.Apply(a.InstanceClass, func(instanceClass rds.InstanceType) rds.InstanceType {
		if instanceClass == "" {
			return rds.InstanceType_T3_Medium
		}
		return instanceClass
	})
}

type RDSUseExistingClusterArgs struct {
	ClusterName    pulumix.Input[string]
	MasterPassword pulumix.Input[string]
}

func (a *RDSUseExistingClusterArgs) SetDefaults() {
	if a.MasterPassword == nil {
		a.MasterPassword = pulumix.Val("")
	}
	if a.ClusterName == nil {
		a.ClusterName = pulumix.Val("")
	}
}

type RDSDatabaseArgs struct {
	UseCluster    *RDSUseExistingClusterArgs
	CreateCluster *RDSClusterCreateArgs
}

func (a *RDSDatabaseArgs) SetDefaults() {
	if a.CreateCluster != nil {
		a.CreateCluster.SetDefaults()
	}
	if a.UseCluster != nil {
		a.UseCluster.SetDefaults()
	}
}

func (a RDSDatabaseArgs) setup(ctx *pulumi.Context, _ pulumix.Input[string], options ...pulumi.ResourceOption) (DatabaseComponent, error) {

	switch {
	case a.UseCluster == nil && a.CreateCluster == nil:
		return nil, errors.New("rds cluster not defined")
	case a.UseCluster != nil && a.CreateCluster != nil:
		return nil, errors.New("either UseCluster or CreateCluster config must be provided, not both")
	case a.UseCluster != nil:
		cluster := pulumix.ApplyErr(a.UseCluster.ClusterName, func(clusterName string) (*rds.LookupClusterResult, error) {
			if clusterName == "" {
				return nil, errors.New("rds cluster not defined")
			}
			return rds.LookupCluster(ctx, &rds.LookupClusterArgs{
				ClusterIdentifier: clusterName,
			})
		})

		return newExternalDatabaseComponent(ctx, "rds", ExternalDatabaseComponentArgs{
			Endpoint: pulumix.Apply(cluster, func(cluster *rds.LookupClusterResult) string {
				return cluster.Endpoint
			}),
			Username: pulumix.Apply(cluster, func(cluster *rds.LookupClusterResult) string {
				return cluster.MasterUsername
			}),
			Port: pulumix.Apply(cluster, func(cluster *rds.LookupClusterResult) int {
				return cluster.Port
			}),
			Options: pulumix.Apply(cluster, func(cluster *rds.LookupClusterResult) map[string]string {
				return map[string]string{}
			}),
			Password: a.UseCluster.MasterPassword,
		})
	case a.CreateCluster != nil:
		return newRDSDatabaseComponent(ctx, &RDSComponentArgs{
			CreateCluster: a.CreateCluster,
		}, options...)
	}

	panic("unreachable")
}
