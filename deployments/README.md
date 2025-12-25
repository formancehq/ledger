# Deployment Configuration Files

This directory contains configuration files used for deploying the Ledger v3 POC application, primarily with Docker Compose.

## Files:

- `otel-collector-config.yml`:
  - **Purpose**: Configuration for the OpenTelemetry Collector that receives traces, metrics, and logs from the Ledger v3 POC application.
  - **Details**: Defines OTLP gRPC and HTTP receivers (ports 4317, 4318) and exports metrics to Prometheus. Includes batch and memory limiter processors for efficient data handling.

- `prometheus.yml`:
  - **Purpose**: Configuration for Prometheus to scrape metrics from the OpenTelemetry Collector.
  - **Details**: Scrapes metrics from the collector's Prometheus exporter endpoint.

## Usage with Docker Compose:

The `docker-compose.yml` file in the root directory references these configuration files. When starting the Docker Compose environment, these files are mounted into their respective containers, ensuring that the services are configured correctly for operation and observability.

**Note**: The Ledger v3 POC application nodes are configured via command-line flags and environment variables directly in `docker-compose.yml`, not via configuration files.

## Accessing Services:

- **Prometheus**: http://localhost:9090
- **OpenTelemetry Collector**: Receives data on ports 4317 (gRPC) and 4318 (HTTP)
