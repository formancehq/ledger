# Docker Compose Configuration

This directory contains all configuration files related to the `docker-compose.yml` setup.

## Structure

```
deployments/docker-compose/
├── otel-collector-config.yml  # OpenTelemetry Collector configuration
├── grafana/                    # Grafana configuration
│   ├── provisioning/
│   │   ├── datasources/       # Data source definitions
│   │   └── dashboards/        # Dashboard definitions
│   └── README.md              # Grafana documentation
└── README.md                  # This file
```

## Files

### `otel-collector-config.yml`

Configuration for the OpenTelemetry Collector that:
- Receives OTLP metrics from ledger nodes (gRPC and HTTP)
- Processes metrics (batching, memory limiting)
- Exports metrics to VictoriaMetrics via OTLP HTTP

### `grafana/`

Grafana configuration directory containing:
- **Data sources**: VictoriaMetrics connection configuration
- **Dashboards**: Pre-configured dashboards for visualizing metrics

See `grafana/README.md` for more details.

## Usage

All paths in `docker-compose.yml` are relative to the project root. The configuration files in this directory are automatically mounted into the containers.

## Services

The docker-compose setup includes:
- **otel-collector**: OpenTelemetry Collector for metrics aggregation
- **victoria-metrics**: Time-series database for metrics storage
- **grafana**: Visualization and dashboarding tool
- **node-1, node-2, node-3**: Ledger Raft cluster nodes
