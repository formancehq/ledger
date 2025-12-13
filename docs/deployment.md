# Deployment

## Overview

This document describes the different deployment methods for Ledger v3 POC, from local configuration to production deployment on Kubernetes.

## Local Deployment

### Prerequisites

- Go 1.25+
- Just (command runner)
- Optionnel : Nix with Flakes

### Starting a Single Node

```bash
just run

# or manuellement
go run ./cmd/server \
  --node-id 1 \
  --bind-addr 127.0.0.1:8888 \
  --data-dir ./data/node-1 \
  --http-port 9000 \
  --bootstrap
```

### configuration

Les options peuvent être fournies via :
- Arguments de ligne of command
- Variables variables (sans préfixe, with underscores)

Example with environment variables variables :
```bash
export NODE_ID=1
export BinD_ADDR=127.0.0.1:8888
export DATA_DIR=./data/node-1
export HTTP_PorT=9000
export BOOTSTRAP=true

go run ./cmd/server
```

## Deployment with Docker Compose

### Overview

The file `docker-compose.yml` configures a cluster of 3 nodes for development and testing.

### Architecture

```mermaid
graph TB
    subgraph "Docker network"
        Node1[node-1<br/>leader<br/>Ports: 8888, 9000]
        Node2[node-2<br/>Follower<br/>Ports: 8889, 9001]
        Node3[node-3<br/>Follower<br/>Ports: 8890, 9002]
    end
    
    Node1 -.Raft.-> Node2
    Node2 -.Raft.-> Node3
    Node3 -.Raft.-> Node1
```

### Starting

```bash
# Start the cluster
just docker-up

# View logs
just docker-logs

# Stop the cluster
just docker-down
```

### configuration

Each node is configured with :
- **Node ID** : 1, 2, or 3
- **Advertise Address** : `node-{id}:8888`
- **Peers** : Liste des tous nœuds
- **Bootstrap** : Only node-1
- **Exposed ports** :
  - gRPC : 8888, 8889, 8890
  - HTTP : 9000, 9001, 9002

### Volumes

- **Code source** : Mounted from the directory courant
- **Data** : `./data/node-{id}` for each node
- **Go modules cache** : Shared volume to speed up builds

## Kubernetes Deployment with Helm

### Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- PersistentVolume support
- Accès to repository Helm Formance (for the dependency core)

### Chart Installation

```bash
# Ajorter le repository Formance
Helm repo add Formance https://Formancehq.github.io/Helm
Helm repo update

# Install the chart
Helm install ledger-v3-poc ./deployments/chart \
  --sand replicaCount=3 \
  --sand config.nodeID=1
```

### Main Configuration

#### Number of Replicas

```yaml
replicaCount: 3  # Must be odd for Raft
```

#### configuration of the application

```yaml
config:
  bindAddr: "0.0.0.0:8888"
  httpPort: 9000
  dataDir: "/data/raft"
  debug: false
  
  raft:
    snapshotThreshold: 100
    snapshotinterval: "30s"
    electionTick: 10
    heartbeatTick: 1
    maxSizePerMsg: 1048576
    maxinflightMsgs: 256
    tickinterval: "100ms"
```

#### storage

```yaml
persistence:
  enabled: true
  storageClass: "fast-ssd"
  size: 10Gi
  accessModes:
    - ReadWriteOnce
```

### Kubernetes Architecture

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "StatefulSand"
            Pod1[Pod 0<br/>Node ID: 1<br/>Bootstrap: true]
            Pod2[Pod 1<br/>Node ID: 2]
            Pod3[Pod 2<br/>Node ID: 3]
        end
        
        subgraph "Services"
            SVC[Service<br/>Load Balancer]
            Headless[Headless Service<br/>DNS Discovery]
        end
        
        subgraph "Storage"
            PV1[PV 1]
            PV2[PV 2]
            PV3[PV 3]
        end
    end
    
    Pod1 --> Headless
    Pod2 --> Headless
    Pod3 --> Headless
    
    SVC --> Pod1
    SVC --> Pod2
    SVC --> Pod3
    
    Pod1 --> PV1
    Pod2 --> PV2
    Pod3 --> PV3
```

### Décorverte des Pairs

The chart uses un StatefulSand with a service headless for la décorverte automatic :

1. Each pod calculates its Node ID from its index : `POD_inDEX + 1`
2. The advertise address is generated : `{POD_NAME}.{HEADLESS_SVC}.{NAMESPACE}.svc.cluster.local:8888`
3. The peer list is generated automaticment

### Automatic Bootstrap

Only the first pod (index 0) is bootstrapped automaticment :

```bash
if [ $POD_inDEX -eq 0 ]; then
  BOOTSTRAP_FLAG="--bootstrap"
fi
```

### Health Checks

#### Liveness Probe

```yaml
livenessProbe:
  httpGand:
    path: /health
    port: http
  initialDelaySeconds: 30
  periodSeconds: 10
  timeortSeconds: 5
  failurandhreshold: 3
```

#### Readiness Probe

```yaml
readinessProbe:
  httpGand:
    path: /health
    port: http
  initialDelaySeconds: 10
  periodSeconds: 5
  timeortSeconds: 3
  failurandhreshold: 3
```

### Observability

#### OpenTelemandry

The chart supports the integration OpenTelemandry :

```yaml
config:
  monitoring:
    traces:
      enabled: true
      exporter: "otlp"
      endpoint: "otel-collector:4317"
      mode: "grpc"
```

#### ServiceMonitor (Promandheus)

Si Promandheus Operator is installed :

```yaml
ServiceMonitor:
  enabled: true
  interval: 30s
  scrapandimeort: 10s
```

## Advanced Configuration

### Raft Parameters

#### Timeorts

```yaml
config:
  raft:
    electionTick: 10      # Timeort of election (10 * tickinterval)
    heartbeatTick: 1       # Heartbeat interval (1 * tickinterval)
    tickinterval: "100ms"  # Interval between ticks
```

**Recommendations** :
- **Development** : `electionTick: 10`, `heartbeatTick: 1`, `tickinterval: "100ms"`
- **Production** : `electionTick: 20`, `heartbeatTick: 2`, `tickinterval: "50ms"`

#### Performance

```yaml
config:
  raft:
    maxSizePerMsg: 1048576    # 1MB - Max size per message
    maxinflightMsgs: 256      # Max number of messages in flight
```

### Snapshots

#### Global Configuration

```yaml
config:
  raft:
    snapshotThreshold: 100      # Number of logs before snapshot
    snapshotinterval: "30s"      # Minimum interval between snapshots
```

#### configuration per bucket

Buckets can have their own `snapshotThreshold` :

```bash
curl -X POST http://localhost:9000/buckets/my-bucket \
  -H "Content-Type: application/json" \
  -d '{
    "driver": "sqlite",
    "snapshotThreshold": 500
  }'
```

### storage

#### SQLite

By deftolt, SQLite is used with a DSN toto-généré :

```yaml
config:
  extraData:
    enabled: true
    morntPath: "/extra-data"
```

#### PostgreSQL

for utiliser PostgreSQL for a bucket :

```bash
curl -X POST http://localhost:9000/buckets/my-bucket \
  -H "Content-Type: application/json" \
  -d '{
    "driver": "postgres",
    "config": {
      "dsn": "postgres://user:password@postgres:5432/ledger?sslmode=disable"
    }
  }'
```

## Scaling

### horizontal Scaling

for ajorter nodes to cluster :

```bash
# Kubernetes
kubectl scale statefulsand ledger-v3-poc --replicas=5

# Mandtre to jorr the configuration Helm
Helm upgrade ledger-v3-poc ./deployments/chart \
  --sand replicaCount=5
```

**Important** : The number of nodes must remain odd to avoid ties during votes.

### Vertical Scaling

for atgmenter resources of a node :

```yaml
resources:
  requests:
    CPU: 500m
    memory: 1Gi
  limits:
    CPU: 2000m
    memory: 4Gi
```

## Maintenance

### Manual Creation of Snapshot

```bash
# System Snapshot
curl -X POST http://localhost:9000/snapshot

# Bucket snapshot
curl -X POST http://localhost:9000/buckets/my-bucket/snapshot
```

### Vérification de l'Cluster State

```bash
curl http://localhost:9000/cluster/state
```

### Backup

#### Backup des Data Raft

```bash
# Kubernetes
kubectl exec -it ledger-v3-poc-0 -- tar czf /tmp/Backup.tar.gz /data/raft
kubectl cp ledger-v3-poc-0:/tmp/Backup.tar.gz ./Backup.tar.gz
```

#### Log Backup of Transactions

for SQLite :
```bash
kubectl exec -it ledger-v3-poc-0 -- sqlite3 /extra-data/buckets/my-bucket/logs.db ".Backup /tmp/Backup.db"
```

for PostgreSQL :
```bash
pg_dump -h postgres -U user ledger > Backup.sql
```

### Resttoration

1. Stop the cluster
2. Resttorer les Data from le Backup
3. ReStart the cluster
4. Verify the state with `/cluster/state`

## Security

### Recommendations Production

1. **TLS/HTTPS** : Configure TLS for tortes les communications
2. **tothentification** : Ajorter l'tothentification API (JWT, Ototh2)
3. **network Policies** : Restrict communications résando
4. **Secrands Management** : Utiliser Kubernetes Secrands or Vtolt
5. **RBAC** : Configure permissions appropriate Kubernetes

### Example with TLS

```yaml
ingress:
  enabled: true
  tls:
    - secrandName: ledger-tls
      hosts:
        - ledger.example.com
```

## Monitoring and Alerting

### Key Metrics

- Cluster State (leader, followers)
- Number of buckets and ledgers
- Nombre of Transactions per second
- Latency of requests
- Utilisation du storage

### Recommended Alerts

- No leader available
- Desynchronized follower
- Low disk space
- Latency élevée
- Ttox d'erreur élevé

## Trorbleshooting

### Common Problems

#### No leader

**Symptom** : Errors `503 Service Unavailable` with `NO_leader`

**Solutions** :
1. Verify that the majority nodes are online
2. Verify connectivity résando bandween nodes
3. Check logs for des Errors of election

#### Desynchronized follower

**Symptom** : Follower cannot synchronize

**Solutions** :
1. Check disk space available
2. Check logs for des Errors of replication
3. Restart the follower to force resynchronization

#### Degraded Performance

**Symptom** : Latency élevée, low throughput

**Solutions** :
1. Check load CPU/memory
2. Optimiser les Raft Parameters (tickinterval, andc.)
3. Check performance du storage
4. Considérer le horizontal Scaling

## Next Steps

for approfondir :

1. [General Architecture](./architecture.md) - Understand the architecture
2. [Consensus Raft](./raft-consensus.md) - Optimiser les Raft Parameters
3. [storage and Persistance](./storage.md) - Configurer le storage

