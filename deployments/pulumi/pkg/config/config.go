package config

import (
	"errors"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	pulumi_ledger "github.com/formancehq/ledger/deployments/pulumi/pkg"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/provision"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"time"
)

type Ingress struct {
	// Host is the hostname for the ingress
	Host string `json:"host"`

	// Secret is the secret name for the ingress
	Secret string `json:"secret"`
}

func (i Ingress) toInput() *api.IngressArgs {
	if i.Host == "" {
		return nil
	}

	return &api.IngressArgs{
		Host:   pulumix.Val(i.Host),
		Secret: pulumix.Val(&i.Secret),
	}
}

type RDSUseExistingCluster struct {
	// ClusterName is the name of the existing RDS cluster to use
	ClusterName string `json:"cluster-name" yaml:"cluster-name"`

	// MasterPassword is the master password for the existing RDS cluster
	MasterPassword string `json:"master-password" yaml:"master-password"`
}

func (a *RDSUseExistingCluster) toInput() *storage.RDSUseExistingClusterArgs {
	if a == nil {
		return nil
	}
	return &storage.RDSUseExistingClusterArgs{
		ClusterName:    pulumi.String(a.ClusterName),
		MasterPassword: pulumi.String(a.MasterPassword),
	}
}

type RDSPostMigrateSnapshot struct {
	// SnapshotIdentifier is the snapshot identifier to create after migrations
	SnapshotIdentifier string `json:"snapshot-identifier" yaml:"snapshot-identifier"`
}

func (a *RDSPostMigrateSnapshot) toInput() *storage.RDSPostMigrateSnapshotArgs {
	if a == nil {
		return nil
	}
	return &storage.RDSPostMigrateSnapshotArgs{
		SnapshotIdentifier: pulumi.String(a.SnapshotIdentifier),
	}
}

type RDSDatabase struct {
	// UseCluster is the configuration to use an existing RDS cluster
	UseCluster *RDSUseExistingCluster `json:"use-cluster" yaml:"use-cluster" jsonschema:"oneof_required=use-cluster"`

	// CreateCluster is the configuration to create a new RDS cluster
	CreateCluster *RDSClusterCreate `json:"create-cluster" yaml:"create-cluster" jsonschema:"oneof_required=create-cluster"`

	// PostMigrateSnapshot is the configuration for a snapshot to create after migrations
	PostMigrateSnapshot *RDSPostMigrateSnapshot `json:"post-migrate-snapshot" yaml:"post-migrate-snapshot"`
}

func (a *RDSDatabase) toInput() *storage.RDSDatabaseArgs {
	if a == nil {
		return nil
	}

	return &storage.RDSDatabaseArgs{
		CreateCluster:       a.CreateCluster.toInput(),
		UseCluster:          a.UseCluster.toInput(),
		PostMigrateSnapshot: a.PostMigrateSnapshot.toInput(),
	}
}

type RDSClusterCreate struct {
	// UseSubnetGroupName is the name of the subnet group to use for the RDS cluster
	UseSubnetGroupName string `json:"use-subnet-group-name" yaml:"use-subnet-group-name"`

	// MasterUsername is the master username for the RDS cluster
	MasterUsername string `json:"master-username" yaml:"master-username"`

	// MasterPassword is the master password for the RDS cluster
	MasterPassword string `json:"master-password" yaml:"master-password"`

	// SnapshotIdentifier is the snapshot identifier to use for the RDS cluster
	SnapshotIdentifier string `json:"initialization-snapshot-identifier" yaml:"initialization-snapshot-identifier"`

	// PerformanceInsightsEnabled is whether performance insights is enabled for the RDS cluster
	PerformanceInsightsEnabled bool `json:"performance-insights-enabled" yaml:"performance-insights-enabled"`

	// InstanceClass is the instance class for the RDS cluster
	InstanceClass string `json:"instance-class" yaml:"instance-class"`

	// Engine is the engine for the RDS cluster
	Engine string `json:"engine" yaml:"engine"`

	// EngineVersion is the engine version for the RDS cluster
	EngineVersion string `json:"engine-version" yaml:"engine-version"`
}

func (a RDSClusterCreate) toInput() *storage.RDSClusterCreateArgs {
	return &storage.RDSClusterCreateArgs{
		UseSubnetGroupName:         pulumi.String(a.UseSubnetGroupName),
		MasterUsername:             pulumi.String(a.MasterUsername),
		MasterPassword:             pulumi.String(a.MasterPassword),
		SnapshotIdentifier:         pulumix.Val(&a.SnapshotIdentifier),
		PerformanceInsightsEnabled: pulumi.Bool(a.PerformanceInsightsEnabled),
		InstanceClass:              pulumix.Val(rds.InstanceType(a.InstanceClass)),
		Engine:                     pulumi.String(a.Engine),
		EngineVersion:              pulumi.String(a.EngineVersion),
	}
}

type PostgresDatabase struct {
	// URI is the URI for the Postgres database
	URI string `json:"uri" yaml:"uri"`

	// Install is whether to install the Postgres database
	Install bool `json:"install" yaml:"install"`
}

func (a *PostgresDatabase) toInput() *storage.PostgresDatabaseArgs {
	if a == nil {
		return nil
	}
	if a.URI != "" {
		return &storage.PostgresDatabaseArgs{
			URI: pulumi.String(a.URI),
		}
	}

	if !a.Install {
		panic("uri must be provided if install is false")
	}

	return &storage.PostgresDatabaseArgs{
		Install: pulumi.Bool(a.Install),
	}
}

type ConnectivityDatabase struct {
	// AWSEnableIAM is whether to enable IAM for the database
	AWSEnableIAM bool `json:"aws-enable-iam" yaml:"aws-enable-iam"`

	// MaxIdleConns is the maximum number of idle connections for the database
	MaxIdleConns *int `json:"max-idle-conns" yaml:"max-idle-conns"`

	// MaxOpenConns is the maximum number of open connections for the database
	MaxOpenConns *int `json:"max-open-conns" yaml:"max-open-conns"`

	// ConnMaxIdleTime is the maximum idle time for a connection
	ConnMaxIdleTime *time.Duration `json:"conn-max-idle-time" yaml:"conn-max-idle-time"`
}

func (d ConnectivityDatabase) toInput() storage.ConnectivityDatabaseArgs {
	return storage.ConnectivityDatabaseArgs{
		AWSEnableIAM:    pulumi.Bool(d.AWSEnableIAM),
		MaxIdleConns:    pulumix.Val(d.MaxIdleConns),
		MaxOpenConns:    pulumix.Val(d.MaxOpenConns),
		ConnMaxIdleTime: pulumix.Val(d.ConnMaxIdleTime),
	}
}

type StorageSource struct {
	// RDS is the RDS configuration for the database
	RDS *RDSDatabase `json:"rds" yaml:"rds" jsonschema:"oneof_required=rds"`

	// Postgres is the Postgres configuration for the database
	Postgres *PostgresDatabase `json:"postgres" yaml:"postgres" jsonschema:"oneof_required=postgres"`
}

type Storage struct {
	StorageSource

	// Connectivity is the connectivity configuration for the database
	Connectivity ConnectivityDatabase `json:"connectivity" yaml:"connectivity"`

	// DisableUpgrade is whether to disable upgrades for the database
	DisableUpgrade bool `json:"disable-upgrade" yaml:"disable-upgrade"`
}

func (s Storage) toInput() storage.Args {
	return storage.Args{
		Postgres:                 s.Postgres.toInput(),
		RDS:                      s.RDS.toInput(),
		ConnectivityDatabaseArgs: s.Connectivity.toInput(),
		DisableUpgrade:           pulumix.Val(s.DisableUpgrade),
	}
}

type API struct {
	// ReplicaCount is the number of replicas for the API
	ReplicaCount *int `json:"replica-count" yaml:"replica-count"`

	// GracePeriod is the grace period for the API
	GracePeriod time.Duration `json:"grace-period" yaml:"grace-period"`

	// BallastSizeInBytes is the ballast size in bytes for the API
	BallastSizeInBytes int `json:"ballast-size-in-bytes" yaml:"ballast-size-in-bytes"`

	// NumscriptCacheMaxCount is the maximum number of scripts to cache
	NumscriptCacheMaxCount int `json:"numscript-cache-max-count" yaml:"numscript-cache-max-count"`

	// BulkMaxSize is the maximum size for bulk requests
	BulkMaxSize int `json:"bulk-max-size" yaml:"bulk-max-size"`

	// BulkParallel is the number of parallel bulk requests
	BulkParallel int `json:"bulk-parallel" yaml:"bulk-parallel"`

	// TerminationGracePeriodSeconds is the termination grace period in seconds
	TerminationGracePeriodSeconds *int `json:"termination-grace-period-seconds" yaml:"termination-grace-period-seconds"`

	// ExperimentalFeatures is whether to enable experimental features
	ExperimentalFeatures bool `json:"experimental-features" yaml:"experimental-features"`

	// ExperimentalNumscriptInterpreter is whether to enable the experimental numscript interpreter
	ExperimentalNumscriptInterpreter bool `json:"experimental-numscript-interpreter" yaml:"experimental-numscript-interpreter"`
}

func (d API) toInput() api.Args {
	return api.Args{
		ReplicaCount:                     pulumix.Val(d.ReplicaCount),
		GracePeriod:                      pulumix.Val(d.GracePeriod),
		BallastSizeInBytes:               pulumix.Val(d.BallastSizeInBytes),
		NumscriptCacheMaxCount:           pulumix.Val(d.NumscriptCacheMaxCount),
		BulkMaxSize:                      pulumix.Val(d.BulkMaxSize),
		BulkParallel:                     pulumix.Val(d.BulkParallel),
		TerminationGracePeriodSeconds:    pulumix.Val(d.TerminationGracePeriodSeconds),
		ExperimentalFeatures:             pulumix.Val(d.ExperimentalFeatures),
		ExperimentalNumscriptInterpreter: pulumix.Val(d.ExperimentalNumscriptInterpreter),
	}
}

type Monitoring struct {
	// ResourceAttributes is the resource attributes for OpenTelemetry
	ResourceAttributes map[string]string `json:"resource-attributes" yaml:"resource-attributes"`

	// ServiceName is the service name for OpenTelemetry
	ServiceName string `json:"service-name" yaml:"service-name"`

	// Traces is the traces configuration for OpenTelemetry
	Traces *OtelTraces `json:"traces" yaml:"traces"`

	// Metrics is the metrics configuration for OpenTelemetry
	Metrics *OtelMetrics `json:"metrics" yaml:"metrics"`
}

func (o *Monitoring) ToInput() *utils.OtelArgs {
	if o == nil {
		return nil
	}
	return &utils.OtelArgs{
		ResourceAttributes: pulumix.Val(o.ResourceAttributes),
		ServiceName:        pulumix.Val(o.ServiceName),
		Traces:             o.Traces.toInput(),
		Metrics:            o.Metrics.toInput(),
	}
}

type OtelTracesJaeger struct {
	// Endpoint is the endpoint for the Jaeger exporter
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// User is the user for the Jaeger exporter
	User string `json:"user" yaml:"user"`

	// Password is the password for the Jaeger exporter
	Password string `json:"password" yaml:"password"`
}

type OtelTracesOTLP struct {
	// Mode is the mode for the OTLP exporter
	Mode string `json:"mode" yaml:"mode"`

	// Endpoint is the endpoint for the OTLP exporter
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// Insecure is whether the OTLP exporter is insecure
	Insecure bool `json:"insecure" yaml:"insecure"`
}

type OtelTraces struct {
	// Batch is whether to batch traces
	Batch bool `json:"batch" yaml:"batch"`

	// ExporterFlag is the exporter flag for traces
	ExporterFlag string `json:"exporter-flag" yaml:"exporter-flag"`

	// Jaeger is the Jaeger configuration for traces
	Jaeger *OtelTracesJaeger `json:"jaeger" yaml:"jaeger"`

	// OTLP is the OTLP configuration for traces
	OTLP *OtelTracesOTLP `json:"otlp" yaml:"otlp"`
}

func (t *OtelTraces) toInput() *utils.OtelTracesArgs {
	if t == nil {
		return nil
	}
	return &utils.OtelTracesArgs{
		OtelTracesBatch:                  pulumix.Val(t.Batch),
		OtelTracesExporterFlag:           pulumix.Val(t.ExporterFlag),
		OtelTracesExporterJaegerEndpoint: pulumix.Val(t.Jaeger.Endpoint),
		OtelTracesExporterJaegerUser:     pulumix.Val(t.Jaeger.User),
		OtelTracesExporterJaegerPassword: pulumix.Val(t.Jaeger.Password),
		OtelTracesExporterOTLPMode:       pulumix.Val(t.OTLP.Mode),
		OtelTracesExporterOTLPEndpoint:   pulumix.Val(t.OTLP.Endpoint),
		OtelTracesExporterOTLPInsecure:   pulumix.Val(t.OTLP.Insecure),
	}
}

type OtelMetricsOTLP struct {
	// Mode is the mode for the OTLP metrics exporter
	Mode string `json:"mode" yaml:"mode"`

	// Endpoint is the endpoint for the OTLP metrics exporter
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// Insecure is whether the OTLP metrics exporter is insecure
	Insecure bool `json:"insecure" yaml:"insecure"`
}

type OtelMetrics struct {
	// PushInterval is the push interval for the metrics exporter
	PushInterval *time.Duration `json:"push-interval" yaml:"push-interval"`

	// Runtime is whether to enable runtime metrics
	Runtime bool `json:"runtime" yaml:"runtime"`

	// RuntimeMinimumReadMemStatsInterval is the minimum read memory stats interval for runtime metrics
	RuntimeMinimumReadMemStatsInterval *time.Duration `json:"runtime-minimum-read-mem-stats-interval" yaml:"runtime-minimum-read-mem-stats-interval"`

	// Exporter is the exporter for metrics
	Exporter string `json:"exporter" yaml:"exporter"`

	// KeepInMemory is whether to keep metrics in memory
	KeepInMemory bool `json:"keep-in-memory" yaml:"keep-in-memory"`

	// OTLP is the OTLP configuration for metrics
	OtelMetricsOTLP *OtelMetricsOTLP `json:"otlp" yaml:"otlp"`
}

func (m *OtelMetrics) toInput() *utils.OtelMetricsArgs {
	if m == nil {
		return nil
	}
	return &utils.OtelMetricsArgs{
		OtelMetricsExporterPushInterval:               pulumix.Val(m.PushInterval),
		OtelMetricsRuntime:                            pulumix.Val(m.Runtime),
		OtelMetricsRuntimeMinimumReadMemStatsInterval: pulumix.Val(m.RuntimeMinimumReadMemStatsInterval),
		OtelMetricsExporter:                           pulumix.Val(m.Exporter),
		OtelMetricsKeepInMemory:                       pulumix.Val(m.KeepInMemory),
		OtelMetricsExporterOTLPMode:                   pulumix.Val(m.OtelMetricsOTLP.Mode),
		OtelMetricsExporterOTLPEndpoint:               pulumix.Val(m.OtelMetricsOTLP.Endpoint),
		OtelMetricsExporterOTLPInsecure:               pulumix.Val(m.OtelMetricsOTLP.Insecure),
	}
}

type Common struct {
	// Namespace is the namespace for the ledger
	Namespace string `json:"namespace" yaml:"namespace"`

	// Monitoring is the monitoring configuration for the ledger
	Monitoring *Monitoring `json:"monitoring" yaml:"monitoring"`

	// Tag is the version tag for the ledger
	Tag string `json:"version" yaml:"version"`

	// ImagePullPolicy is the image pull policy for the ledger
	ImagePullPolicy string `json:"image-pull-policy" yaml:"image-pull-policy"`

	// Debug is whether to enable debug mode
	Debug bool `json:"debug" yaml:"debug"`
}

func (c Common) toInput() utils.CommonArgs {
	return utils.CommonArgs{
		Namespace:       pulumix.Val(c.Namespace),
		Otel:            c.Monitoring.ToInput(),
		Tag:             pulumix.Val(c.Tag),
		ImagePullPolicy: pulumix.Val(c.ImagePullPolicy),
		Debug:           pulumix.Val(c.Debug),
	}
}

type LedgerConfig struct {
	// Bucket is the bucket for the ledger
	Bucket string `json:"bucket" yaml:"bucket"`

	// Metadata is the metadata for the ledger
	Metadata map[string]string `json:"metadata" yaml:"metadata"`

	// Features is the features for the ledger
	Features map[string]string `json:"features" yaml:"features"`
}

func (c LedgerConfig) toInput() provision.LedgerConfigArgs {
	return provision.LedgerConfigArgs{
		Bucket:   c.Bucket,
		Metadata: c.Metadata,
		Features: c.Features,
	}
}

type ProvisionConfig struct {
	// Ledgers are the ledgers to auto create
	Ledgers map[string]LedgerConfig `json:"ledgers" yaml:"ledgers"`
}

func (c ProvisionConfig) toInput() provision.ConfigArgs {
	if c.Ledgers == nil {
		return provision.ConfigArgs{}
	}
	return provision.ConfigArgs{
		Ledgers: ConvertMap(c.Ledgers, LedgerConfig.toInput),
	}
}

type Provision struct {
	// ProvisionerVersion is the version of the provisioner (default to the ledger version if not specified)
	ProvisionerVersion string `json:"provisioner-version" yaml:"provisioner-version"`

	// Config is the configuration for the provisioner
	Config ProvisionConfig `json:"config" yaml:"config"`
}

func (i Provision) toInput() provision.Args {
	return provision.Args{
		ProvisionerVersion: pulumi.String(i.ProvisionerVersion),
		Config:             i.Config.toInput(),
	}
}

type Config struct {
	Common

	// Storage is the storage configuration for the ledger
	Storage *Storage `json:"storage" yaml:"storage"`

	// API is the API configuration for the ledger
	API *API `json:"api" yaml:"api"`

	// Ingress is the ingress configuration for the ledger
	Ingress *Ingress `json:"ingress" yaml:"ingress"`

	// Provision is the initialization configuration for the ledger
	Provision *Provision `json:"provision" yaml:"provision"`

	// Timeout is the timeout for the ledger
	Timeout int `json:"timeout" yaml:"timeout"`

	// InstallDevBox is whether to install the dev box
	InstallDevBox bool `json:"install-dev-box" yaml:"install-dev-box"`
}

func (cfg Config) ToInput() pulumi_ledger.ComponentArgs {
	return pulumi_ledger.ComponentArgs{
		CommonArgs:    cfg.Common.toInput(),
		Storage:       cfg.Storage.toInput(),
		API:           cfg.API.toInput(),
		Timeout:       pulumix.Val(cfg.Timeout),
		Ingress:       cfg.Ingress.toInput(),
		InstallDevBox: pulumix.Val(cfg.InstallDevBox),
		Provision:     cfg.Provision.toInput(),
	}
}

func Load(ctx *pulumi.Context) (*Config, error) {
	cfg := config.New(ctx, "")

	ingress := &Ingress{}
	if err := cfg.TryObject("ingress", ingress); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, err
		}
	}

	timeout, err := config.TryInt(ctx, "timeout")
	if err != nil {
		if errors.Is(err, config.ErrMissingVar) {
			timeout = 60
		} else {
			return nil, fmt.Errorf("error reading timeout: %w", err)
		}
	}

	storage := &Storage{}
	if err := config.GetObject(ctx, "storage", storage); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, err
		}
		return nil, errors.New("storage not defined")
	}

	api := &API{}
	if err := config.GetObject(ctx, "api", api); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, err
		}
	}

	otel := &Monitoring{}
	if err := config.GetObject(ctx, "monitoring", otel); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, err
		}
	}

	provision := &Provision{}
	if err := cfg.TryObject("provision", provision); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, err
		}
	}

	namespace := config.Get(ctx, "namespace")
	if namespace == "" {
		namespace = ctx.Stack()
	}

	return &Config{
		Timeout: timeout,
		Common: Common{
			Debug:      config.GetBool(ctx, "debug"),
			Namespace:  namespace,
			Tag:        config.Get(ctx, "version"),
			Monitoring: otel,
		},
		InstallDevBox: config.GetBool(ctx, "install-dev-box"),
		Storage:       storage,
		API:           api,
		Ingress:       ingress,
		Provision:     provision,
	}, nil
}
