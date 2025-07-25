package config

import (
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/formancehq/go-libs/v3/collectionutils"
	pulumi_ledger "github.com/formancehq/ledger/deployments/pulumi/pkg"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/common"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/exporters"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/generator"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/monitoring"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/provision"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/storage"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/worker"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"gopkg.in/yaml.v3"
	"reflect"
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

	// RetainsOnDelete is whether to retain the RDS cluster on delete (instances will be deleted)
	RetainsOnDelete bool `json:"retains-on-delete" yaml:"retains-on-delete"`
}

func (a *RDSPostMigrateSnapshot) toInput() *storage.RDSPostMigrateSnapshotArgs {
	if a == nil {
		return nil
	}
	return &storage.RDSPostMigrateSnapshotArgs{
		SnapshotIdentifier: pulumi.String(a.SnapshotIdentifier),
		RetainsOnDelete:    a.RetainsOnDelete,
	}
}

type RDSDatabase struct {
	// UseCluster is the configuration to use an existing RDS cluster
	UseCluster *RDSUseExistingCluster `json:"use-cluster" yaml:"use-cluster" jsonschema:"oneof_required=use-cluster"`

	// CreateCluster is the configuration to create a new RDS cluster
	CreateCluster *RDSClusterCreate `json:"create-cluster" yaml:"create-cluster" jsonschema:"oneof_required=create-cluster"`

	// PostMigrateSnapshot is the configuration for a snapshot to create after migrations
	PostMigrateSnapshot *RDSPostMigrateSnapshot `json:"post-migrate-snapshot" yaml:"post-migrate-snapshot"`

	// UseDBName is the name of the database to use
	UseDBName string `json:"use-db-name" yaml:"use-db-name"`
}

func (a *RDSDatabase) toInput() *storage.RDSDatabaseArgs {
	if a == nil {
		return nil
	}

	return &storage.RDSDatabaseArgs{
		CreateCluster:       a.CreateCluster.toInput(),
		UseCluster:          a.UseCluster.toInput(),
		PostMigrateSnapshot: a.PostMigrateSnapshot.toInput(),
		UseDBName:           pulumi.String(a.UseDBName),
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

	// RetainsOnDelete is whether to retain the RDS cluster on delete (instances will be deleted)
	RetainsOnDelete bool `json:"retains-on-delete" yaml:"retains-on-delete"`
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
		RetainsOnDelete:            a.RetainsOnDelete,
	}
}

type PostgresInstall struct {
	// Username is the username for the Postgres database
	Username string `json:"username" yaml:"username"`

	// Password is the password for the Postgres database
	Password string `json:"password" yaml:"password"`
}

type PostgresDatabase struct {
	// URI is the URI for the Postgres database
	URI string `json:"uri" yaml:"uri"`

	// Install is whether to install the Postgres database
	Install *PostgresInstall `json:"install" yaml:"install"`
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

	if a.Install == nil {
		panic("uri must be provided if install is false")
	}

	return &storage.PostgresDatabaseArgs{
		Install: &storage.PostgresInstallArgs{
			Username: pulumix.Val(a.Install.Username),
			Password: pulumix.Val(a.Install.Password),
		},
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
	ConnMaxIdleTime *Duration `json:"conn-max-idle-time" yaml:"conn-max-idle-time"`

	// Options is the options for the Postgres database to pass on the dsn
	Options map[string]string `json:"options" yaml:"options"`
}

func (d ConnectivityDatabase) toInput() storage.ConnectivityDatabaseArgs {
	return storage.ConnectivityDatabaseArgs{
		AWSEnableIAM:    pulumi.Bool(d.AWSEnableIAM),
		MaxIdleConns:    pulumix.Val(d.MaxIdleConns),
		MaxOpenConns:    pulumix.Val(d.MaxOpenConns),
		ConnMaxIdleTime: pulumix.Val((*time.Duration)(d.ConnMaxIdleTime)),
		Options:         pulumix.Val(d.Options),
	}
}

type StorageSource struct {
	// RDS is the RDS configuration for the database
	RDS *RDSDatabase `json:"rds" yaml:"rds" jsonschema:"oneof_required=rds"`

	// Postgres is the Postgres configuration for the database
	Postgres *PostgresDatabase `json:"postgres" yaml:"postgres" jsonschema:"oneof_required=postgres"`
}

type StorageService struct {
	// Annotations is the annotations for the service
	Annotations map[string]string `json:"annotations" yaml:"annotations"`
}

func (s StorageService) toInput() storage.Service {
	return storage.Service{
		Annotations: pulumix.Val(s.Annotations),
	}
}

type Storage struct {
	StorageSource `yaml:",inline"`

	// Connectivity is the connectivity configuration for the database
	Connectivity ConnectivityDatabase `json:"connectivity" yaml:"connectivity"`

	// DisableUpgrade is whether to disable upgrades for the database
	DisableUpgrade bool `json:"disable-upgrade" yaml:"disable-upgrade"`

	// Service is the service configuration for the database
	Service StorageService `json:"service" yaml:"service"`
}

func (s Storage) toInput() storage.Args {
	return storage.Args{
		Postgres:                 s.Postgres.toInput(),
		RDS:                      s.RDS.toInput(),
		ConnectivityDatabaseArgs: s.Connectivity.toInput(),
		DisableUpgrade:           pulumix.Val(s.DisableUpgrade),
		Service:                  s.Service.toInput(),
	}
}

type Duration time.Duration

var _ json.Marshaler = Duration(0)
var _ json.Unmarshaler = (*Duration)(nil)
var _ yaml.Marshaler = Duration(0)
var _ yaml.Unmarshaler = (*Duration)(nil)

func (d *Duration) UnmarshalJSON(data []byte) error {

	fmt.Println(string(data))

	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	duration, err := time.ParseDuration(str)
	if err != nil {
		return err
	}
	*d = Duration(duration)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	duration := time.Duration(d)
	return json.Marshal(duration.String())
}

func (d Duration) MarshalYAML() (interface{}, error) {
	duration := time.Duration(d)
	return duration.String(), nil
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	v, err := time.ParseDuration(value.Value)
	if err != nil {
		return err
	}
	*d = Duration(v)

	return nil
}

type API struct {
	// ReplicaCount is the number of replicas for the API
	ReplicaCount *int `json:"replica-count" yaml:"replica-count"`

	// GracePeriod is the grace period for the API
	GracePeriod Duration `json:"grace-period" yaml:"grace-period"`

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

	// ExperimentalExporters is whether to enable experimental exporter
	ExperimentalExporters bool `json:"experimental-exporters" yaml:"experimental-exporters"`
}

func (d API) toInput() api.Args {
	return api.Args{
		ReplicaCount:                     pulumix.Val(d.ReplicaCount),
		GracePeriod:                      pulumix.Val(time.Duration(d.GracePeriod)),
		BallastSizeInBytes:               pulumix.Val(d.BallastSizeInBytes),
		NumscriptCacheMaxCount:           pulumix.Val(d.NumscriptCacheMaxCount),
		BulkMaxSize:                      pulumix.Val(d.BulkMaxSize),
		BulkParallel:                     pulumix.Val(d.BulkParallel),
		TerminationGracePeriodSeconds:    pulumix.Val(d.TerminationGracePeriodSeconds),
		ExperimentalFeatures:             pulumix.Val(d.ExperimentalFeatures),
		ExperimentalNumscriptInterpreter: pulumix.Val(d.ExperimentalNumscriptInterpreter),
		ExperimentalExporters:            pulumix.Val(d.ExperimentalExporters),
	}
}

type Exporter struct {
	// Driver is the driver for the exporter
	Driver string `json:"driver" yaml:"driver"`

	// Config is the configuration for the exporter
	Config any `json:"config" yaml:"config"`
}

func (c Exporter) toInput() exporters.ExporterArgs {
	return exporters.ExporterArgs{
		Driver: c.Driver,
		Config: c.Config,
	}
}

type Exporters map[string]Exporter

func (c *Exporters) UnmarshalJSON(data []byte) error {
	asMap := make(map[string]json.RawMessage, 0)
	if err := json.Unmarshal(data, &asMap); err != nil {
		return fmt.Errorf("error unmarshalling exporters into an array: %w", err)
	}

	*c = make(map[string]Exporter)
	for id, elem := range asMap {
		type def struct {
			Driver string `json:"driver" yaml:"driver"`
		}
		d := def{}
		if err := json.Unmarshal(elem, &d); err != nil {
			return fmt.Errorf("error unmarshalling exporter definition %s: %w", id, err)
		}

		cfg, err := exporters.GetExporterConfig(d.Driver)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(elem, cfg); err != nil {
			return fmt.Errorf("error unmarshalling exporter config %s: %w", id, err)
		}

		(*c)[id] = Exporter{
			Driver: d.Driver,
			Config: reflect.ValueOf(cfg).Elem().Interface(),
		}
	}

	return nil
}

func (c *Exporters) toInput() exporters.Args {
	return exporters.Args{
		Exporters: ConvertMap(*c, Exporter.toInput),
	}
}

type Worker struct{}

func (w Worker) toInput() worker.Args {
	return worker.Args{}
}

type Monitoring struct {
	// ResourceAttributes is the resource attributes for OpenTelemetry
	ResourceAttributes map[string]string `json:"resource-attributes" yaml:"resource-attributes"`

	// ServiceName is the service name for OpenTelemetry
	ServiceName string `json:"service-name" yaml:"service-name"`

	// Traces is the traces configuration for OpenTelemetry
	Traces *MonitoringTraces `json:"traces" yaml:"traces"`

	// Metrics is the metrics configuration for OpenTelemetry
	Metrics *MonitoringMetrics `json:"metrics" yaml:"metrics"`
}

func (o *Monitoring) ToInput() *monitoring.Args {
	if o == nil {
		return nil
	}
	return &monitoring.Args{
		ResourceAttributes: pulumix.Val(o.ResourceAttributes),
		ServiceName:        pulumix.Val(o.ServiceName),
		Traces:             o.Traces.toInput(),
		Metrics:            o.Metrics.toInput(),
	}
}

type MonitoringTracesJaeger struct {
	// Endpoint is the endpoint for the Jaeger exporter
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// User is the user for the Jaeger exporter
	User string `json:"user" yaml:"user"`

	// Password is the password for the Jaeger exporter
	Password string `json:"password" yaml:"password"`
}

func (j *MonitoringTracesJaeger) toInput() *monitoring.JaegerExporterArgs {
	if j == nil {
		return nil
	}
	return &monitoring.JaegerExporterArgs{
		Endpoint: pulumi.String(j.Endpoint),
		User:     pulumi.String(j.User),
		Password: pulumi.String(j.Password),
	}
}

type MonitoringTracesOTLP struct {
	// Mode is the mode for the OTLP exporter
	Mode string `json:"mode" yaml:"mode"`

	// Endpoint is the endpoint for the OTLP exporter
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// Insecure is whether the OTLP exporter is insecure
	Insecure bool `json:"insecure" yaml:"insecure"`
}

func (o *MonitoringTracesOTLP) toInput() *monitoring.EndpointArgs {
	if o == nil {
		return nil
	}
	return &monitoring.EndpointArgs{
		Mode:     pulumi.String(o.Mode),
		Endpoint: pulumi.String(o.Endpoint),
		Insecure: pulumi.Bool(o.Insecure),
	}
}

type MonitoringTraces struct {
	// Batch is whether to batch traces
	Batch bool `json:"batch" yaml:"batch"`

	// Exporter is the exporter flag for traces
	Exporter string `json:"exporter" yaml:"exporter"`

	// Jaeger is the Jaeger configuration for traces
	Jaeger *MonitoringTracesJaeger `json:"jaeger" yaml:"jaeger"`

	// OTLP is the OTLP configuration for traces
	OTLP *MonitoringTracesOTLP `json:"otlp" yaml:"otlp"`
}

func (t *MonitoringTraces) toInput() *monitoring.TracesArgs {
	if t == nil {
		return nil
	}
	return &monitoring.TracesArgs{
		Batch:    pulumix.Val(t.Batch),
		Exporter: pulumix.Val(t.Exporter),
		OTLP:     t.OTLP.toInput(),
		Jaeger:   t.Jaeger.toInput(),
	}
}

type MonitoringMetricsOTLP struct {
	// Mode is the mode for the OTLP metrics exporter
	Mode string `json:"mode" yaml:"mode"`

	// Endpoint is the endpoint for the OTLP metrics exporter
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// Insecure is whether the OTLP metrics exporter is insecure
	Insecure bool `json:"insecure" yaml:"insecure"`
}

func (o *MonitoringMetricsOTLP) toInput() *monitoring.EndpointArgs {
	if o == nil {
		return nil
	}
	return &monitoring.EndpointArgs{
		Mode:     pulumi.String(o.Mode),
		Endpoint: pulumi.String(o.Endpoint),
		Insecure: pulumi.Bool(o.Insecure),
	}
}

type MonitoringMetrics struct {
	// PushInterval is the push interval for the metrics exporter
	PushInterval *Duration `json:"push-interval" yaml:"push-interval"`

	// Runtime is whether to enable runtime metrics
	Runtime bool `json:"runtime" yaml:"runtime"`

	// RuntimeMinimumReadMemStatsInterval is the minimum read memory stats interval for runtime metrics
	RuntimeMinimumReadMemStatsInterval *Duration `json:"runtime-minimum-read-mem-stats-interval" yaml:"runtime-minimum-read-mem-stats-interval"`

	// Exporter is the exporter for metrics
	Exporter string `json:"exporter" yaml:"exporter"`

	// KeepInMemory is whether to keep metrics in memory
	KeepInMemory bool `json:"keep-in-memory" yaml:"keep-in-memory"`

	// OTLP is the OTLP configuration for metrics
	MonitoringMetricsOTLP *MonitoringMetricsOTLP `json:"otlp" yaml:"otlp"`
}

func (m *MonitoringMetrics) toInput() *monitoring.MetricsArgs {
	if m == nil {
		return nil
	}
	return &monitoring.MetricsArgs{
		PushInterval:                pulumix.Val((*time.Duration)(m.PushInterval)),
		Runtime:                     pulumix.Val(m.Runtime),
		MinimumReadMemStatsInterval: pulumix.Val((*time.Duration)(m.RuntimeMinimumReadMemStatsInterval)),
		Exporter:                    pulumix.Val(m.Exporter),
		KeepInMemory:                pulumix.Val(m.KeepInMemory),
		OTLP:                        m.MonitoringMetricsOTLP.toInput(),
	}
}

type Common struct {
	// Namespace is the namespace for the ledger
	Namespace string `json:"namespace" yaml:"namespace"`

	// Monitoring is the monitoring configuration for the ledger
	Monitoring *Monitoring `json:"monitoring,omitempty" yaml:"monitoring,omitempty"`

	// Tag is the version tag for the ledger
	Tag string `json:"version,omitempty" yaml:"version,omitempty"`

	// ImagePullPolicy is the image pull policy for the ledger
	ImagePullPolicy string `json:"image-pull-policy,omitempty" yaml:"image-pull-policy,omitempty"`

	// Debug is whether to enable debug mode
	Debug bool `json:"debug,omitempty" yaml:"debug,omitempty"`
}

func (c Common) toInput() common.CommonArgs {
	return common.CommonArgs{
		Namespace:       pulumix.Val(c.Namespace),
		Monitoring:      c.Monitoring.ToInput(),
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

	// Exporters are the exporter to bound to this ledger
	Exporters []string `json:"exporters" yaml:"exporters"`
}

func (c LedgerConfig) toInput() provision.LedgerConfigArgs {
	return provision.LedgerConfigArgs{
		Bucket:    c.Bucket,
		Metadata:  c.Metadata,
		Features:  c.Features,
		Exporters: c.Exporters,
	}
}

type Provision struct {
	// ProvisionerVersion is the version of the provisioner (default to the ledger version if not specified)
	ProvisionerVersion string `json:"provisioner-version" yaml:"provisioner-version"`

	// Ledgers are the ledgers to auto create
	Ledgers map[string]LedgerConfig `json:"ledgers" yaml:"ledgers"`
}

func (i Provision) toInput() provision.Args {
	return provision.Args{
		ProvisionerVersion: pulumi.String(i.ProvisionerVersion),
		Ledgers:            ConvertMap(i.Ledgers, LedgerConfig.toInput),
	}
}

type GeneratorLedgerConfiguration struct {
	// UntilLogID is the log ID to run the generator until
	UntilLogID uint `json:"until-log-id" yaml:"until-log-id"`

	// Script is the script to run
	Script string `json:"script" yaml:"script"`

	// ScriptFromFile is the script to run from a file (related to the root directory)
	ScriptFromFile string `json:"script-from-file" yaml:"script-from-file"`

	// VUs is the number of virtual users to run
	VUs int `json:"vus" yaml:"vus"`

	// HTTPClientTimeout is the http client timeout for the generator
	HTTPClientTimeout Duration `json:"http-client-timeout" yaml:"http-client-timeout"`

	// SkipAwait is whether to skip the await for the generator
	SkipAwait bool `json:"skip-await" yaml:"skip-await"`
}

func (g GeneratorLedgerConfiguration) toInput() generator.LedgerConfiguration {
	return generator.LedgerConfiguration{
		UntilLogID:        pulumix.Val(g.UntilLogID),
		Script:            pulumix.Val(g.Script),
		ScriptFromFile:    pulumix.Val(g.ScriptFromFile),
		VUs:               pulumix.Val(g.VUs),
		HTTPClientTimeout: pulumix.Val((time.Duration)(g.HTTPClientTimeout)),
		SkipAwait:         pulumix.Val(g.SkipAwait),
	}
}

type Generator struct {
	// GeneratorVersion is the version of the generator
	GeneratorVersion string `json:"generator-version" yaml:"generator-version"`

	// Ledgers are the ledgers to run the generator against
	Ledgers map[string]GeneratorLedgerConfiguration `json:"ledgers" yaml:"ledgers"`
}

func (g *Generator) toInput() *generator.Args {
	if g == nil {
		return nil
	}

	return &generator.Args{
		GeneratorVersion: pulumix.Val(g.GeneratorVersion),
		Ledgers:          ConvertMap(g.Ledgers, GeneratorLedgerConfiguration.toInput),
	}
}

type Config struct {
	Common `yaml:",inline"`

	// Storage is the storage configuration for the ledger
	Storage *Storage `json:"storage,omitempty" yaml:"storage,omitempty"`

	// API is the API configuration for the ledger
	API *API `json:"api,omitempty" yaml:"api,omitempty"`

	// Worker is the worker configuration for the ledger
	Worker *Worker `json:"worker,omitempty" yaml:"worker,omitempty"`

	// Exporters is the exporters configuration for the ledger
	Exporters Exporters `json:"exporters" yaml:"exporters"`

	// Ingress is the ingress configuration for the ledger
	Ingress *Ingress `json:"ingress,omitempty" yaml:"ingress,omitempty"`

	// Provision is the initialization configuration for the ledger
	Provision *Provision `json:"provision,omitempty" yaml:"provision,omitempty"`

	// Timeout is the timeout for the ledger
	Timeout int `json:"timeout,omitempty" yaml:"timeout,omitempty"`

	// InstallDevBox is whether to install the dev box
	InstallDevBox bool `json:"install-dev-box,omitempty" yaml:"install-dev-box,omitempty"`

	// Generator is the generator configuration for the ledger
	Generator *Generator `json:"generator,omitempty" yaml:"generator,omitempty"`
}

func (cfg Config) ToInput() pulumi_ledger.ComponentArgs {
	return pulumi_ledger.ComponentArgs{
		CommonArgs:    cfg.toInput(),
		Storage:       cfg.Storage.toInput(),
		API:           cfg.API.toInput(),
		Worker:        cfg.Worker.toInput(),
		Timeout:       pulumix.Val(cfg.Timeout),
		Ingress:       cfg.Ingress.toInput(),
		InstallDevBox: pulumix.Val(cfg.InstallDevBox),
		Provision:     cfg.Provision.toInput(),
		Exporters:     cfg.Exporters.toInput(),
		Generator:     cfg.Generator.toInput(),
	}
}

func Load(ctx *pulumi.Context) (*Config, error) {
	cfg := config.New(ctx, "")

	ingress := &Ingress{}
	if err := cfg.TryObject("ingress", ingress); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading ingress config: %w", err)
		}
	}

	timeout, err := cfg.TryInt("timeout")
	if err != nil {
		if errors.Is(err, config.ErrMissingVar) {
			timeout = 60
		} else {
			return nil, fmt.Errorf("error reading timeout: %w", err)
		}
	}

	storage := &Storage{}
	if err := cfg.GetObject("storage", storage); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading storage config: %w", err)
		}
		return nil, errors.New("storage not defined")
	}

	api := &API{}
	if err := cfg.GetObject("api", api); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading api config: %w", err)
		}
	}

	worker := &Worker{}
	if err := cfg.GetObject("worker", worker); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading worker config: %w", err)
		}
	}

	exporters := Exporters{}
	if err := config.GetObject(ctx, "exporters", &exporters); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, err
		}
	}

	monitoring := &Monitoring{}
	if err := cfg.GetObject("monitoring", monitoring); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading monitoring config: %w", err)
		}
	}

	provision := &Provision{}
	if err := cfg.TryObject("provision", provision); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading provision config: %w", err)
		}
	}

	generator := &Generator{}
	if err := cfg.TryObject("generator", generator); err != nil {
		if !errors.Is(err, config.ErrMissingVar) {
			return nil, fmt.Errorf("error reading generator config: %w", err)
		}
		generator = nil
	}

	namespace := cfg.Get("namespace")
	if namespace == "" {
		namespace = ctx.Stack()
	}

	return &Config{
		Timeout: timeout,
		Common: Common{
			Debug:           cfg.GetBool("debug"),
			Namespace:       namespace,
			Tag:             cfg.Get("version"),
			Monitoring:      monitoring,
			ImagePullPolicy: cfg.Get("image-pull-policy"),
		},
		InstallDevBox: cfg.GetBool("install-dev-box"),
		Storage:       storage,
		API:           api,
		Worker:        worker,
		Ingress:       ingress,
		Exporters:     exporters,
		Provision:     provision,
		Generator:     generator,
	}, nil
}
