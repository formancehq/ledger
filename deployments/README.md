# Deployment Configuration Files

This directory contains all configuration files used by the Docker Compose setup.

## Files

### `default-config.yml`
Configuration file for the ledger server nodes. This file is mounted into all node containers and contains:
- Network configuration (bind address, data directory)
- Server configuration (gRPC and HTTP ports)
- Storage configuration (SQLite or file-based)
- Raft snapshot configuration

Node-specific values (node ID, advertise address, peers, bootstrap flag) are set via environment variables in `docker-compose.yml`.

### `otel-collector-config.yml`
Configuration for the OpenTelemetry Collector. This collector:
- Receives traces via OTLP (gRPC on port 4317, HTTP on port 4318)
- Processes traces using batch processor
- Exports traces to the SigNoz OpenTelemetry Collector

### `signoz-otel-collector-config.yaml`
Configuration for the SigNoz OpenTelemetry Collector. This collector:
- Receives traces from the main OpenTelemetry Collector
- Processes traces, metrics, and logs using batch and memory limiter processors
- Exports data to ClickHouse databases:
  - `signoz_traces` for traces
  - `signoz_metrics` for metrics
  - `signoz_logs` for logs

## Usage

These files are automatically mounted into their respective containers by `docker-compose.yml`. No manual configuration is required when using Docker Compose.

To modify the configuration:
1. Edit the relevant file in this directory
2. Restart the affected service: `docker-compose restart <service-name>`

