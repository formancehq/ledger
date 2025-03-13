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
| ingress | `object` |  | [Ingress](#ingress) | Ingress is the ingress configuration for the ledger |
| provision | `object` |  | [Provision](#provision) | Provision is the initialization configuration for the ledger |
| timeout | `integer` |  | integer | Timeout is the timeout for the ledger |
| install-dev-box | `boolean` |  | boolean | InstallDevBox is whether to install the dev box |

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
| traces | `object` |  | [OtelTraces](#oteltraces) | Traces is the traces configuration for OpenTelemetry |
| metrics | `object` |  | [OtelMetrics](#otelmetrics) | Metrics is the metrics configuration for OpenTelemetry |

## OtelMetrics

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
| otlp | `object` |  | [OtelMetricsOTLP](#otelmetricsotlp) | OTLP is the OTLP configuration for metrics |

## OtelMetricsOTLP

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| mode | `string` |  | string | Mode is the mode for the OTLP metrics exporter |
| endpoint | `string` |  | string | Endpoint is the endpoint for the OTLP metrics exporter |
| insecure | `boolean` |  | boolean | Insecure is whether the OTLP metrics exporter is insecure |

## OtelTraces

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| batch | `boolean` |  | boolean | Batch is whether to batch traces |
| exporter-flag | `string` |  | string | ExporterFlag is the exporter flag for traces |
| jaeger | `object` |  | [OtelTracesJaeger](#oteltracesjaeger) | Jaeger is the Jaeger configuration for traces |
| otlp | `object` |  | [OtelTracesOTLP](#oteltracesotlp) | OTLP is the OTLP configuration for traces |

## OtelTracesJaeger

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| endpoint | `string` |  | string | Endpoint is the endpoint for the Jaeger exporter |
| user | `string` |  | string | User is the user for the Jaeger exporter |
| password | `string` |  | string | Password is the password for the Jaeger exporter |

## OtelTracesOTLP

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
| install | `boolean` |  | boolean | Install is whether to install the Postgres database |

## Provision

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
| provisioner-version | `string` |  | string | ProvisionerVersion is the version of the provisioner (default to the ledger version if not specified) |
| config | `object` |  | [ProvisionConfig](#provisionconfig) | Config is the configuration for the provisioner |

## ProvisionConfig

No description provided for this model.

#### Type: `object`

> ⚠️ Additional properties are not allowed.

| Property | Type | Required | Possible values | Description |
| -------- | ---- | -------- | --------------- | ----------- |
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


---

Markdown generated with [jsonschema-markdown](https://github.com/elisiariocouto/jsonschema-markdown).
