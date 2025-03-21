# jsonschema-markdown

JSON Schema missing a description, provide it using the `description` key in the root of the JSON document.

### Type: `object(?)`


---

# Definitions

## API

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| replica-count | `integer` |  | integer | ReplicaCount is the number of replicas for the API |
| grace-period | `integer` |  | integer | GracePeriod is the grace period for the API |
| ballast-size-in-bytes | `integer` |  | integer | BallastSizeInBytes is the ballast size in bytes for the API |
| numscript-cache-max-count | `integer` |  | integer | NumscriptCacheMaxCount is the maximum number of scripts to cache |
| bulk-max-size | `integer` |  | integer | BulkMaxSize is the maximum size for bulk requests |
| bulk-parallel | `integer` |  | integer | BulkParallel is the number of parallel bulk requests |
| termination-grace-period-seconds | `integer` |  | integer | TerminationGracePeriodSeconds is the termination grace period in seconds |
| experimental-features | `boolean` |  | boolean | ExperimentalFeatures is whether to enable experimental features |
| experimental-numscript-interpreter | `boolean` |  | boolean | ExperimentalNumscriptInterpreter is whether to enable the experimental numscript interpreter |

## Config

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| namespace | `string` |  | string | Namespace is the namespace for the ledger |
| monitoring | `object` |  | [Monitoring](#monitoring) | Monitoring is the monitoring configuration for the ledger |
| version | `string` |  | string | Tag is the version tag for the ledger |
| image-pull-policy | `string` |  | string | ImagePullPolicy is the image pull policy for the ledger |
| debug | `boolean` |  | boolean | Debug is whether to enable debug mode |
| storage | `object` |  | [Storage](#storage) | Storage is the storage configuration for the ledger |
| api | `object` |  | [API](#api) | API is the API configuration for the ledger |
| worker | `object` |  | [Worker](#worker) | Worker is the worker configuration for the ledger |
| ingress | `object` |  | [Ingress](#ingress) | Ingress is the ingress configuration for the ledger |
| provision | `object` |  | [Provision](#provision) | Provision is the initialization configuration for the ledger |
| timeout | `integer` |  | integer | Timeout is the timeout for the ledger |
| install-dev-box | `boolean` |  | boolean | InstallDevBox is whether to install the dev box |
| generator | `object` |  | [Generator](#generator) | Generator is the generator configuration for the ledger |

## ConnectivityDatabase

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| aws-enable-iam | `boolean` |  | boolean | AWSEnableIAM is whether to enable IAM for the database |
| max-idle-conns | `integer` |  | integer | MaxIdleConns is the maximum number of idle connections for the database |
| max-open-conns | `integer` |  | integer | MaxOpenConns is the maximum number of open connections for the database |
| conn-max-idle-time | `integer` |  | integer | ConnMaxIdleTime is the maximum idle time for a connection |
| options | `object` |  | object | Options is the options for the Postgres database to pass on the dsn |

## Generator

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| generator-version | `string` |  | string | GeneratorVersion is the version of the generator |
| until-log-id | `integer` |  | integer | UntilLogID is the log ID to run the generator until |
| script | `string` |  | string | Script is the script to run |
| script-from-file | `string` |  | string | ScriptFromFile is the script to run from a file (related to the root directory) |
| vus | `integer` |  | integer | VUs is the number of virtual users to run |
| ledger | `string` |  | string | Ledger is the ledger to run the generator against |
| http-client-timeout | `integer` |  | integer | HTTPClientTimeout is the http client timeout for the generator |

## Ingress

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| host | `string` |  | string | Host is the hostname for the ingress |
| secret | `string` |  | string | Secret is the secret name for the ingress |

## LedgerConfig

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| bucket | `string` |  | string | Bucket is the bucket for the ledger |
| metadata | `object` |  | object | Metadata is the metadata for the ledger |
| features | `object` |  | object | Features is the features for the ledger |

## Monitoring

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| resource-attributes | `object` |  | object | ResourceAttributes is the resource attributes for OpenTelemetry |
| service-name | `string` |  | string | ServiceName is the service name for OpenTelemetry |
| traces | `object` |  | [MonitoringTraces](#monitoringtraces) | Traces is the traces configuration for OpenTelemetry |
| metrics | `object` |  | [MonitoringMetrics](#monitoringmetrics) | Metrics is the metrics configuration for OpenTelemetry |

## MonitoringMetrics

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| push-interval | `integer` |  | integer | PushInterval is the push interval for the metrics exporter |
| runtime | `boolean` |  | boolean | Runtime is whether to enable runtime metrics |
| runtime-minimum-read-mem-stats-interval | `integer` |  | integer | RuntimeMinimumReadMemStatsInterval is the minimum read memory stats interval for runtime metrics |
| exporter | `string` |  | string | Exporter is the exporter for metrics |
| keep-in-memory | `boolean` |  | boolean | KeepInMemory is whether to keep metrics in memory |
| otlp | `object` |  | [MonitoringMetricsOTLP](#monitoringmetricsotlp) | OTLP is the OTLP configuration for metrics |

## MonitoringMetricsOTLP

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| mode | `string` |  | string | Mode is the mode for the OTLP metrics exporter |
| endpoint | `string` |  | string | Endpoint is the endpoint for the OTLP metrics exporter |
| insecure | `boolean` |  | boolean | Insecure is whether the OTLP metrics exporter is insecure |

## MonitoringTraces

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| batch | `boolean` |  | boolean | Batch is whether to batch traces |
| exporter | `string` |  | string | Exporter is the exporter flag for traces |
| jaeger | `object` |  | [MonitoringTracesJaeger](#monitoringtracesjaeger) | Jaeger is the Jaeger configuration for traces |
| otlp | `object` |  | [MonitoringTracesOTLP](#monitoringtracesotlp) | OTLP is the OTLP configuration for traces |

## MonitoringTracesJaeger

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| endpoint | `string` |  | string | Endpoint is the endpoint for the Jaeger exporter |
| user | `string` |  | string | User is the user for the Jaeger exporter |
| password | `string` |  | string | Password is the password for the Jaeger exporter |

## MonitoringTracesOTLP

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| mode | `string` |  | string | Mode is the mode for the OTLP exporter |
| endpoint | `string` |  | string | Endpoint is the endpoint for the OTLP exporter |
| insecure | `boolean` |  | boolean | Insecure is whether the OTLP exporter is insecure |

## PostgresDatabase

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| uri | `string` |  | string | URI is the URI for the Postgres database |
| install | `object` |  | [PostgresInstall](#postgresinstall) | Install is whether to install the Postgres database |

## PostgresInstall

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| username | `string` |  | string | Username is the username for the Postgres database |
| password | `string` |  | string | Password is the password for the Postgres database |

## Provision

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| provisioner-version | `string` |  | string | ProvisionerVersion is the version of the provisioner (default to the ledger version if not specified) |
| ledgers | `object` |  | [LedgerConfig](#ledgerconfig) | Ledgers are the ledgers to auto create |

## RDSClusterCreate

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| use-subnet-group-name | `string` |  | string | UseSubnetGroupName is the name of the subnet group to use for the RDS cluster |
| master-username | `string` |  | string | MasterUsername is the master username for the RDS cluster |
| master-password | `string` |  | string | MasterPassword is the master password for the RDS cluster |
| initialization-snapshot-identifier | `string` |  | string | SnapshotIdentifier is the snapshot identifier to use for the RDS cluster |
| performance-insights-enabled | `boolean` |  | boolean | PerformanceInsightsEnabled is whether performance insights is enabled for the RDS cluster |
| instance-class | `string` |  | string | InstanceClass is the instance class for the RDS cluster |
| engine | `string` |  | string | Engine is the engine for the RDS cluster |
| engine-version | `string` |  | string | EngineVersion is the engine version for the RDS cluster |

## RDSDatabase

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| use-cluster | `object` |  | [RDSUseExistingCluster](#rdsuseexistingcluster) | UseCluster is the configuration to use an existing RDS cluster |
| create-cluster | `object` |  | [RDSClusterCreate](#rdsclustercreate) | CreateCluster is the configuration to create a new RDS cluster |
| post-migrate-snapshot | `object` |  | [RDSPostMigrateSnapshot](#rdspostmigratesnapshot) | PostMigrateSnapshot is the configuration for a snapshot to create after migrations |
| use-db-name | `string` |  | string | UseDBName is the name of the database to use |

## RDSPostMigrateSnapshot

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| snapshot-identifier | `string` |  | string | SnapshotIdentifier is the snapshot identifier to create after migrations |

## RDSUseExistingCluster

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| cluster-name | `string` |  | string | ClusterName is the name of the existing RDS cluster to use |
| master-password | `string` |  | string | MasterPassword is the master password for the existing RDS cluster |

## Storage

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| rds | `object` |  | [RDSDatabase](#rdsdatabase) | RDS is the RDS configuration for the database |
| postgres | `object` |  | [PostgresDatabase](#postgresdatabase) | Postgres is the Postgres configuration for the database |
| connectivity | `object` |  | [ConnectivityDatabase](#connectivitydatabase) | Connectivity is the connectivity configuration for the database |
| disable-upgrade | `boolean` |  | boolean | DisableUpgrade is whether to disable upgrades for the database |

## Worker

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.


---

Markdown generated with [jsonschema-markdown](https://github.com/elisiariocouto/jsonschema-markdown).
