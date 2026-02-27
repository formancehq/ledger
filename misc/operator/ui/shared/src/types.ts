// TypeScript types derived from Go CRD structs in:
//   misc/operator/api/v1alpha1/ledger_types.go
//   misc/operator/api/v1alpha1/ledgerdefaults_types.go

// --- Kubernetes metadata (simplified) ---

export interface ObjectMeta {
  name: string;
  namespace?: string;
  creationTimestamp?: string;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  generation?: number;
  resourceVersion?: string;
  uid?: string;
}

// --- Image ---

export interface ImageSpec {
  repository?: string;
  tag?: string;
  pullPolicy?: string;
}

// --- Service Account ---

export interface ServiceAccountSpec {
  create?: boolean;
  annotations?: Record<string, string>;
  name?: string;
}

// --- Pebble ---

export interface PebbleConfig {
  memTableSize?: number;
  memTableStopWritesThreshold?: number;
  l0CompactionThreshold?: number;
  l0StopWritesThreshold?: number;
  lBaseMaxBytes?: number;
  cacheSize?: number;
  targetFileSize?: number;
  bytesPerSync?: number;
  walBytesPerSync?: number;
  maxConcurrentCompactions?: number;
  walMinSyncInterval?: string;
  disableWAL?: boolean;
}

// --- Raft ---

export interface RaftTransportConfig {
  receptionQueues?: number[];
  sendQueues?: number[];
}

export interface RaftConfig {
  snapshotThreshold?: number;
  compactionMargin?: number;
  snapshotInterval?: string;
  electionTick?: number;
  heartbeatTick?: number;
  maxSizePerMsg?: number;
  maxInflightMsgs?: number;
  tickInterval?: string;
  proposeQueueCapacity?: number;
  learnerPromotionThreshold?: number;
  transport?: RaftTransportConfig;
}

// --- Cache ---

export interface CacheConfig {
  rotationThreshold?: number;
}

// --- Health ---

export interface HealthConfig {
  interval?: string;
  walThreshold?: string;
  dataThreshold?: string;
  clockSkewThreshold?: string;
}

// --- Audit ---

export interface AuditConfig {
  enabled?: boolean;
}

// --- Cold Storage ---

export interface S3Config {
  bucket?: string;
  region?: string;
  endpoint?: string;
}

export interface ColdStorageConfig {
  driver?: string;
  path?: string;
  bucketId?: string;
  s3?: S3Config;
}

// --- TLS ---

export interface TLSConfig {
  enabled?: boolean;
  secretName?: string;
  caSecretKey?: string;
}

// --- Response Signing ---

export interface ResponseSigningConfig {
  enabled?: boolean;
  secretName?: string;
  secretKey?: string;
}

// --- Monitoring ---

export interface TraceSamplingConfig {
  enabled?: boolean;
  successRatio?: string;
}

export interface TracesConfig {
  enabled?: boolean;
  exporter?: string;
  endpoint?: string;
  port?: string;
  insecure?: string;
  mode?: string;
  batch?: string;
  sampling?: TraceSamplingConfig;
}

export interface MetricsConfig {
  enabled?: boolean;
  exporter?: string;
  endpoint?: string;
  port?: string;
  insecure?: string;
  mode?: string;
  keepInMemory?: boolean;
  exporterPushInterval?: string;
  runtime?: boolean;
  runtimeMinimumReadMemStatsInterval?: string;
}

export interface LogsConfig {
  enabled?: boolean;
  level?: string;
  exporter?: string;
  endpoint?: string;
  port?: string;
  insecure?: string;
  mode?: string;
}

export interface PyroscopeConfig {
  enabled?: boolean;
  serverAddress?: string;
  applicationName?: string;
  authToken?: string;
  tenantId?: string;
  basicAuthUser?: string;
  basicAuthPassword?: string;
  uploadRate?: string;
  tags?: string;
  profileTypes?: string;
  mutexProfileFraction?: number;
  blockProfileRate?: number;
  disableGCRuns?: boolean;
}

export interface MonitoringConfig {
  serviceName?: string;
  traces?: TracesConfig;
  metrics?: MetricsConfig;
  logs?: LogsConfig;
  attributes?: string;
  pyroscope?: PyroscopeConfig;
}

// --- Service ---

export interface ServiceSpec {
  type?: string;
  httpPort?: number;
  grpcPort?: number;
  raftPort?: number;
  annotations?: Record<string, string>;
}

export interface HeadlessServiceSpec {
  enabled?: boolean;
  annotations?: Record<string, string>;
}

// --- Ingress ---

export interface IngressPath {
  path?: string;
  pathType?: string;
}

export interface IngressHost {
  host: string;
  paths?: IngressPath[];
}

export interface IngressTLS {
  hosts?: string[];
  secretName?: string;
}

export interface IngressSpec {
  enabled?: boolean;
  className?: string;
  annotations?: Record<string, string>;
  hosts?: IngressHost[];
  tls?: IngressTLS[];
}

export interface TargetGroupBindingSpec {
  enabled?: boolean;
  targetGroupARN?: string;
  targetType?: string;
  networking?: unknown;
}

export interface IngressGrpcSpec {
  enabled?: boolean;
  className?: string;
  annotations?: Record<string, string>;
  hosts?: IngressHost[];
  tls?: IngressTLS[];
  targetGroupBinding?: TargetGroupBindingSpec;
}

// --- Persistence ---

export interface VolumeSpec {
  storageClass?: string;
  accessMode?: string;
  size?: string;
}

export interface RetentionPolicySpec {
  whenScaled?: string;
  whenDeleted?: string;
}

export interface PersistenceSpec {
  wal?: VolumeSpec;
  data?: VolumeSpec;
  retentionPolicy?: RetentionPolicySpec;
}

// --- Pod Scheduling ---

export interface PodAntiAffinitySpec {
  enabled?: boolean;
  type?: string;
  weight?: number;
  topologyKey?: string;
}

export interface PodDisruptionBudgetSpec {
  enabled?: boolean;
  minAvailable?: number;
  maxUnavailable?: number;
}

// --- ServiceMonitor ---

export interface ServiceMonitorSpec {
  enabled?: boolean;
  interval?: string;
  scrapeTimeout?: string;
  labels?: Record<string, string>;
  relabelings?: unknown[];
  metricRelabelings?: unknown[];
}

// --- Resources ---

export interface ResourceRequirements {
  requests?: Record<string, string>;
  limits?: Record<string, string>;
}

// --- LedgerService Config ---

export interface LedgerServiceConfig {
  clusterID?: string;
  bindAddr?: string;
  grpcPort?: number;
  httpPort?: number;
  walDir?: string;
  dataDir?: string;
  debug?: boolean;
  restore?: boolean;
  pebble?: PebbleConfig;
  raft?: RaftConfig;
  cache?: CacheConfig;
  health?: HealthConfig;
  audit?: AuditConfig;
  admissionMetrics?: boolean;
  coldStorage?: ColdStorageConfig;
  tls?: TLSConfig;
  responseSigning?: ResponseSigningConfig;
  monitoring?: MonitoringConfig;
}

// --- Condition ---

export interface Condition {
  type: string;
  status: string;
  lastTransitionTime?: string;
  reason?: string;
  message?: string;
  observedGeneration?: number;
}

// --- LedgerService Status ---

export interface LedgerServiceStatus {
  phase?: string;
  readyReplicas?: number;
  observedGeneration?: number;
  conditions?: Condition[];
}

// --- LedgerService Spec ---

export interface LedgerServiceSpec {
  defaultsRef?: string;
  image?: ImageSpec;
  imagePullSecrets?: Array<{ name: string }>;
  replicas?: number;
  serviceAccount?: ServiceAccountSpec;
  podSecurityContext?: unknown;
  securityContext?: unknown;
  config?: LedgerServiceConfig;
  service?: ServiceSpec;
  headlessService?: HeadlessServiceSpec;
  ingress?: IngressSpec;
  ingressGrpc?: IngressGrpcSpec;
  persistence?: PersistenceSpec;
  resources?: ResourceRequirements;
  livenessProbe?: unknown;
  readinessProbe?: unknown;
  podAnnotations?: Record<string, string>;
  nodeSelector?: Record<string, string>;
  tolerations?: unknown[];
  affinity?: unknown;
  podAntiAffinity?: PodAntiAffinitySpec;
  podDisruptionBudget?: PodDisruptionBudgetSpec;
  serviceMonitor?: ServiceMonitorSpec;
}

// --- LedgerService ---

export interface LedgerService {
  apiVersion?: string;
  kind?: string;
  metadata: ObjectMeta;
  spec: LedgerServiceSpec;
  status?: LedgerServiceStatus;
}

// --- LedgerDefaults Config ---

export interface LedgerDefaultsConfig {
  pebble?: PebbleConfig;
  raft?: RaftConfig;
  health?: HealthConfig;
  coldStorage?: ColdStorageConfig;
  tls?: TLSConfig;
  responseSigning?: ResponseSigningConfig;
  monitoring?: MonitoringConfig;
}

// --- LedgerDefaults Spec ---

export interface LedgerDefaultsSpec {
  image?: ImageSpec;
  imagePullSecrets?: Array<{ name: string }>;
  serviceAccount?: ServiceAccountSpec;
  config?: LedgerDefaultsConfig;
  resources?: ResourceRequirements;
  livenessProbe?: unknown;
  readinessProbe?: unknown;
  podSecurityContext?: unknown;
  securityContext?: unknown;
  nodeSelector?: Record<string, string>;
  tolerations?: unknown[];
  affinity?: unknown;
  podAntiAffinity?: PodAntiAffinitySpec;
  podDisruptionBudget?: PodDisruptionBudgetSpec;
  serviceMonitor?: ServiceMonitorSpec;
}

// --- LedgerDefaults ---

export interface LedgerDefaults {
  apiVersion?: string;
  kind?: string;
  metadata: ObjectMeta;
  spec: LedgerDefaultsSpec;
}

// --- API response types ---

export interface PodSummary {
  name: string;
  ready: string;
  status: string;
  reason?: string;
  message?: string;
  restarts: number;
  age?: string;
  node?: string;
  containers: string[];
}

export interface PvcSummary {
  name: string;
  status: string;
  capacity?: string;
  storageClass?: string;
  age?: string;
}

export interface ServiceSummary {
  name: string;
  type: string;
  clusterIP?: string;
  ports?: string;
  age?: string;
}

export interface EventSummary {
  type: string;
  reason: string;
  message: string;
  count: number;
  source?: string;
  involvedObject: string;
  firstTimestamp?: string;
  lastTimestamp?: string;
}

export interface LedgerServiceDetail {
  ledgerService: LedgerService;
  pods: PodSummary[];
  pvcs: PvcSummary[];
  services: ServiceSummary[];
  events: EventSummary[];
}

export interface LedgerDefaultsDetail {
  ledgerDefaults: LedgerDefaults;
  referencedBy: Array<{ name: string; namespace: string }>;
}

export interface LedgerDefaultsListItem {
  ledgerDefaults: LedgerDefaults;
  referencedByCount: number;
}
