# Ledger v3 POC Helm Chart

This Helm chart deploys the Ledger v3 POC application with Raft consensus cluster on Kubernetes.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- PersistentVolume provisioner support in the underlying infrastructure
- Access to Formance Helm repository (for core chart dependency)

## Adding the Formance Helm Repository

The chart depends on the `core` chart from Formance. Add the repository first:

```bash
helm repo add formance https://formancehq.github.io/helm
helm repo update
```

**Note**: The `core` chart may not be publicly available. If you encounter an error when running `helm dependency update`, you have two options:

### Option 1: Use a Local Dependency

If you have access to the `formancehq/helm` repository:

```bash
# Clone the repository
git clone https://github.com/formancehq/helm.git /tmp/formance-helm

# Package the core chart
cd /tmp/formance-helm/charts/core
helm package .

# Update Chart.yaml to use local dependency
# Change repository to: "file://../../formance-helm/charts/core"
```

Then update `Chart.yaml`:
```yaml
dependencies:
  - name: core
    version: ">=0.0.0"
    repository: "file://../../formance-helm/charts/core"
```

### Option 2: Use the Monitoring Helpers Directly

The chart includes local monitoring helpers (`_monitoring.tpl`) that are compatible with the core chart. If the core chart dependency fails, the local helpers will be used automatically.

## Installation

### Basic Installation

```bash
helm install ledger-v3-poc ./deployments/chart
```

### Installation with Custom Values

```bash
helm install ledger-v3-poc ./deployments/chart -f my-values.yaml
```

### Installation in a Specific Namespace

```bash
helm install ledger-v3-poc ./deployments/chart --namespace ledger --create-namespace
```

### Installation with Ingress Enabled

```bash
helm install ledger-v3-poc ./deployments/chart \
  --set ingress.enabled=true \
  --set ingress.className=nginx \
  --set ingress.hosts[0].host=ledger.example.com \
  --set ingress.tls[0].secretName=ledger-tls \
  --set ingress.tls[0].hosts[0]=ledger.example.com
```

Or using a values file:

```yaml
ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
  hosts:
    - host: ledger.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: ledger-tls
      hosts:
        - ledger.example.com
```

### Installation with Per-Pod Ingress (Traefik)

To access each pod individually from outside the cluster using Traefik, the chart automatically reuses the configuration from `ingress.hosts[0]`:

```bash
helm install ledger-v3-poc ./deployments/chart \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=ledger.example.com \
  --set ingress.perPod.enabled=true \
  --set ingress.className=traefik \
  --set ingress.perPod.annotations."traefik\.ingress\.kubernetes\.io/router\.entrypoints"=web,websecure \
  --set ingress.perPod.annotations."traefik\.ingress\.kubernetes\.io/router\.tls"=true
```

This automatically creates:
- `pod-0.ledger.example.com` → pod 0
- `pod-1.ledger.example.com` → pod 1
- `pod-2.ledger.example.com` → pod 2

Or using a values file:

```yaml
ingress:
  enabled: true
  className: traefik
  hosts:
    - host: ledger.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: ledger-tls
      hosts:
        - ledger.example.com
  perPod:
    enabled: true
    # Automatically uses ingress.hosts[0].host as base (pod-%d.{baseHost})
    # Automatically reuses ingress.hosts[0].paths and ingress.tls
    annotations:
      traefik.ingress.kubernetes.io/router.tls: "true"
      traefik.ingress.kubernetes.io/router.middlewares: default-headers@kubernetescrd
```

**Note**: When `ingress.enabled=true` and `ingress.perPod.enabled=true`, the chart:
- Creates a dedicated Service for each pod (selecting the pod by its StatefulSet pod name)
- Creates an Ingress resource for each pod pointing to its dedicated service
- Automatically reuses `ingress.hosts[0]` configuration to create pod hostnames (`pod-{index}.{baseHost}`)
- Reuses `ingress.hosts[0].paths` and `ingress.tls` configuration automatically
- Each pod becomes accessible via its own hostname/URL (e.g., `pod-0.ledger.example.com`, `pod-1.ledger.example.com`, etc.)

## Configuration

The following table lists the configurable parameters and their default values:

### Image Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Container image repository | `ghcr.io/formancehq/ledger-v3-poc` |
| `image.tag` | Container image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` (automatically `Always` if tag is `latest`) |
| `imagePullSecrets` | List of image pull secrets for private registries | `[]` |

### Replica Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of Raft nodes | `3` |

### Application Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.clusterID` | Cluster ID for inter-node communication validation (all nodes must share the same ID) | `default` |
| `config.bindAddr` | Raft transport bind address (internal inter-node communication) | `0.0.0.0:7777` |
| `config.grpcPort` | gRPC service port (external client-facing API) | `8888` |
| `config.httpPort` | HTTP server port | `9000` |
| `config.dataDir` | Data directory for Raft | `/data/raft` |
| `config.raft.snapshotThreshold` | Number of logs before triggering a snapshot | `100` |
| `config.raft.snapshotInterval` | Minimum interval between snapshots | `30s` |
| `config.raft.electionTick` | Election timeout in ticks | `10` |
| `config.raft.heartbeatTick` | Heartbeat interval in ticks | `1` |
| `config.raft.tickInterval` | Interval between Raft ticks (e.g., "100ms") | `100ms` |
| `config.raft.maxSizePerMsg` | Maximum size per message in bytes | `1048576` (1MB) |
| `config.raft.maxInflightMsgs` | Maximum number of in-flight messages | `256` |
| `config.raft.proposeQueueCapacity` | Capacity of the propose queue | `256` |
| `config.raft.transport.receptionQueues` | Reception queue capacities per priority | `[10, 512, 512, 512, 128]` |
| `config.raft.transport.sendQueues` | Send queue capacities per priority | `[10, 512, 512, 512, 128]` |
| `config.debug` | Enable debug logging | `false` |

### Monitoring Configuration

The chart uses the `core` chart's monitoring helpers. Configuration can be set at `config.monitoring.*` or `global.monitoring.*` (global takes precedence).

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.monitoring.serviceName` | Service name for monitoring | `ledger-v3-poc` |
| `config.monitoring.traces.enabled` | Enable traces | `true` |
| `config.monitoring.traces.exporter` | Traces exporter type | `otlp` |
| `config.monitoring.traces.endpoint` | OTLP endpoint URL | `` |
| `config.monitoring.traces.port` | OTLP port | `` |
| `config.monitoring.traces.insecure` | Use insecure connection | `false` |
| `config.monitoring.traces.mode` | Transport mode | `grpc` |
| `config.monitoring.metrics.enabled` | Enable metrics | `false` |
| `config.monitoring.logs.enabled` | Enable structured logging | `true` |
| `config.monitoring.logs.level` | Log level | `info` |
| `config.monitoring.logs.format` | Log format | `json` |

### Service Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.httpPort` | HTTP service port | `9000` |
| `service.grpcPort` | gRPC service port (external client-facing API) | `8888` |
| `service.raftPort` | Raft transport port (internal inter-node communication) | `7777` |
| `headlessService.enabled` | Enable headless service for peer discovery | `true` |

#### Accessing Individual Pods

With a StatefulSet and headless service, each pod gets a stable DNS name that allows direct access:

**Format**: `{pod-name}-{index}.{headless-service-name}.{namespace}.svc.cluster.local`

**Example** (with 3 replicas in `default` namespace):
- Pod 0: `ledger-v3-poc-0.ledger-v3-poc-headless.default.svc.cluster.local`
- Pod 1: `ledger-v3-poc-1.ledger-v3-poc-headless.default.svc.cluster.local`
- Pod 2: `ledger-v3-poc-2.ledger-v3-poc-headless.default.svc.cluster.local`

**Ports**:
- HTTP: `{pod-dns-name}:9000`
- gRPC Service: `{pod-dns-name}:8888`
- Raft Transport: `{pod-dns-name}:7777` (internal)

**Usage examples**:
```bash
# Access HTTP endpoint on pod 0
curl http://ledger-v3-poc-0.ledger-v3-poc-headless.default.svc.cluster.local:9000/health

# Access gRPC endpoint on pod 1
grpcurl ledger-v3-poc-1.ledger-v3-poc-headless.default.svc.cluster.local:8888 list

# From within the cluster, you can use short names:
curl http://ledger-v3-poc-0.ledger-v3-poc-headless:9000/health
```

**Note**: These DNS names are stable and persist across pod restarts, making them ideal for Raft peer discovery and direct pod access.

### Ingress Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.enabled` | Enable Ingress resource | `false` |
| `ingress.className` | Ingress class name (e.g., "nginx", "traefik") | `` |
| `ingress.annotations` | Ingress annotations | `{}` |
| `ingress.hosts` | List of host configurations | `[{host: ledger-v3-poc.local, paths: [{path: /, pathType: Prefix}]}]` |
| `ingress.tls` | TLS configuration | `[]` |

### Per-Pod Ingress Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.perPod.enabled` | Enable individual ingress for each pod (requires `ingress.enabled=true`) | `false` |
| `ingress.perPod.annotations` | Ingress annotations (applied to all pod ingresses) | `{}` |

### Persistence Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.enabled` | Enable persistent volumes | `true` |
| `persistence.storageClass` | Storage class name (empty = default) | `` |
| `persistence.accessMode` | Access mode | `ReadWriteOnce` |
| `persistence.size` | Volume size | `10Gi` |

### Resource Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `1000m` |
| `resources.limits.memory` | Memory limit | `512Mi` |
| `resources.requests.cpu` | CPU request | `100m` |
| `resources.requests.memory` | Memory request | `128Mi` |

### Health Checks

| Parameter | Description | Default |
|-----------|-------------|---------|
| `livenessProbe.initialDelaySeconds` | Liveness probe initial delay | `30` |
| `livenessProbe.periodSeconds` | Liveness probe period | `10` |
| `readinessProbe.initialDelaySeconds` | Readiness probe initial delay | `10` |
| `readinessProbe.periodSeconds` | Readiness probe period | `5` |

### ServiceMonitor (Prometheus Operator)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceMonitor.enabled` | Enable ServiceMonitor | `false` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |
| `serviceMonitor.labels` | Additional labels | `{}` |

## Architecture

The chart deploys a StatefulSet with multiple replicas, where each replica represents a Raft node:

- **StatefulSet**: Ensures ordered deployment and stable network identities
- **Headless Service**: Enables DNS-based peer discovery
- **Service**: Exposes HTTP and gRPC ports
- **Persistent Volumes**: Stores Raft data and snapshots

### Node Configuration

Each pod automatically:
- Extracts its node ID from the pod index (pod-0 → node ID 1, pod-1 → node ID 2, etc.)
- Generates its advertise address using the headless service DNS name
- Builds the peers list dynamically from all other pods
- Initializes its storage with the cluster configuration when starting with empty storage

## Examples

### Example: Custom Storage Class

```yaml
persistence:
  enabled: true
  storageClass: "fast-ssd"
  size: 50Gi
```

### Example: Production Configuration

```yaml
replicaCount: 5
image:
  tag: "v1.0.0"
resources:
  limits:
    cpu: 2000m
    memory: 1Gi
  requests:
    cpu: 500m
    memory: 512Mi
persistence:
  size: 100Gi
config:
  snapshotThreshold: 1000
  snapshotInterval: "60s"
```

### Example: Development Configuration

```yaml
replicaCount: 3
image:
  tag: "dev"
config:
  debug: true
persistence:
  enabled: false
resources:
  limits:
    cpu: 500m
    memory: 256Mi
  requests:
    cpu: 100m
    memory: 128Mi
```

### Example: Monitoring with Global Configuration

```yaml
global:
  monitoring:
    traces:
      enabled: true
      endpoint: "otel-collector"
      port: 4317
      exporter: "otlp"
      insecure: false
      mode: "grpc"
    logs:
      enabled: true
      level: "info"
      format: "json"
    metrics:
      enabled: true
      exporter: "otlp"
      endpoint: "otel-collector"
      port: 4317
```

### Example: Private Registry with Image Pull Secrets

```yaml
image:
  repository: ghcr.io/formancehq/ledger-v3-poc
  tag: "v1.0.0"
imagePullSecrets:
  - name: regcred
```

To create the image pull secret:
```bash
kubectl create secret docker-registry regcred \
  --docker-server=ghcr.io \
  --docker-username=<username> \
  --docker-password=<token> \
  --docker-email=<email>
```

### Example: ServiceMonitor for Prometheus

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
  scrapeTimeout: 10s
  labels:
    release: prometheus
```

## Upgrading

### Upgrade the Chart

```bash
helm upgrade ledger-v3-poc ./deployments/chart
```

### Upgrade with New Values

```bash
helm upgrade ledger-v3-poc ./deployments/chart -f new-values.yaml
```

### Rolling Upgrade

The StatefulSet ensures that pods are upgraded one at a time, maintaining cluster availability:

```bash
helm upgrade ledger-v3-poc ./deployments/chart
```

## Scaling

### Scale Up

```bash
kubectl scale statefulset ledger-v3-poc --replicas=5
```

**Note**: When scaling up, new nodes will automatically discover existing peers through the headless service. However, you may need to manually add them to the Raft cluster configuration.

### Scale Down

```bash
kubectl scale statefulset ledger-v3-poc --replicas=3
```

**Warning**: Scaling down removes nodes from the cluster. Ensure you have enough nodes remaining to maintain quorum (typically at least 3 nodes for a 5-node cluster).

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -l app.kubernetes.io/name=ledger-v3-poc
```

### View Pod Logs

```bash
kubectl logs ledger-v3-poc-0
```

### Check Service Endpoints

```bash
kubectl get endpoints ledger-v3-poc
```

### Check Headless Service DNS

To verify that each pod has a dedicated DNS address:

```bash
# List all pods
kubectl get pods -l app.kubernetes.io/name=ledger-v3-poc

# Check DNS resolution from within a pod
kubectl exec -it ledger-v3-poc-0 -- nslookup ledger-v3-poc-0.ledger-v3-poc-headless.default.svc.cluster.local
kubectl exec -it ledger-v3-poc-0 -- nslookup ledger-v3-poc-1.ledger-v3-poc-headless.default.svc.cluster.local
kubectl exec -it ledger-v3-poc-0 -- nslookup ledger-v3-poc-2.ledger-v3-poc-headless.default.svc.cluster.local

# Or use dig if available
kubectl exec -it ledger-v3-poc-0 -- dig ledger-v3-poc-headless.default.svc.cluster.local SRV
```

### Test Direct Pod Access

```bash
# Test HTTP access to a specific pod
kubectl run -it --rm debug --image=curlimages/curl --restart=Never -- \
  curl http://ledger-v3-poc-0.ledger-v3-poc-headless.default.svc.cluster.local:9000/health

# Test from within the cluster
kubectl exec -it ledger-v3-poc-1 -- wget -q -O- http://ledger-v3-poc-0.ledger-v3-poc-headless:9000/health
```

### Test External Access to Individual Pods

When `ingress.enabled=true` and `ingress.perPod.enabled=true`, you can access each pod from outside the cluster:

```bash
# Test access to pod 0 (replace with your actual hostname)
curl http://pod-0.ledger.example.com/health

# Test access to pod 1
curl http://pod-1.ledger.example.com/health

# Test access to pod 2
curl http://pod-2.ledger.example.com/health

# With HTTPS (if TLS is configured)
curl https://pod-0.ledger.example.com/health
```

**Verify ingress resources**:
```bash
# List all pod-specific ingresses
kubectl get ingress -l app.kubernetes.io/component=pod-ingress

# Check a specific pod ingress
kubectl describe ingress ledger-v3-poc-pod-0

# List pod-specific services
kubectl get svc -l app.kubernetes.io/component=pod-service
```

### Access Pod Shell

```bash
kubectl exec -it ledger-v3-poc-0 -- /bin/sh
```

### Check Persistent Volumes

```bash
kubectl get pvc -l app.kubernetes.io/name=ledger-v3-poc
```

## Uninstallation

```bash
helm uninstall ledger-v3-poc
```

**Note**: This will delete the StatefulSet and Services, but PersistentVolumeClaims will remain. To delete them:

```bash
kubectl delete pvc -l app.kubernetes.io/name=ledger-v3-poc
```

## Dependencies

This chart depends on the `core` chart from Formance for monitoring helpers. The core chart provides:
- Monitoring configuration helpers (`core.monitoring`)
- Value resolution helpers (`resolveGlobalOrServiceValue`)
- Common Kubernetes resource templates

See [Formance Helm Charts](https://github.com/formancehq/helm) for more information.

## License

[Add your license information here]
