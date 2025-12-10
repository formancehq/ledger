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
| `config.bindAddr` | Raft/gRPC bind address (same port for both) | `0.0.0.0:8888` |
| `config.httpPort` | HTTP server port | `9000` |
| `config.dataDir` | Data directory for Raft | `/data/raft` |
| `config.snapshotThreshold` | Number of logs before triggering a snapshot | `100` |
| `config.snapshotInterval` | Minimum interval between snapshots | `30s` |
| `config.bootstrap` | Bootstrap the cluster (first node only) | `true` |
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
| `service.grpcPort` | gRPC service port (same as bindAddr port) | `8888` |
| `headlessService.enabled` | Enable headless service for peer discovery | `true` |

### Ingress Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.enabled` | Enable Ingress resource | `false` |
| `ingress.className` | Ingress class name (e.g., "nginx", "traefik") | `` |
| `ingress.annotations` | Ingress annotations | `{}` |
| `ingress.hosts` | List of host configurations | `[{host: ledger-v3-poc.local, paths: [{path: /, pathType: Prefix}]}]` |
| `ingress.tls` | TLS configuration | `[]` |

### Persistence Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.enabled` | Enable persistent volumes | `true` |
| `persistence.storageClass` | Storage class name (empty = default) | `` |
| `persistence.accessMode` | Access mode | `ReadWriteOnce` |
| `persistence.size` | Volume size | `10Gi` |

### Extra Data Volume Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `extraData.enabled` | Enable extra data volume | `false` |
| `extraData.mountPath` | Mount path for extra data volume | `/data/extra` |
| `extraData.storageClass` | Storage class name (empty = default) | `` |
| `extraData.accessMode` | Access mode | `ReadWriteOnce` |
| `extraData.size` | Volume size | `10Gi` |
| `extraData.annotations` | Volume annotations | `{}` |

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
- Bootstraps only the first pod (pod-0)

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
