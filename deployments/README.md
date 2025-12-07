# Deployment Configuration Files

This directory contains configuration files used for deploying the Ledger v3 POC application, primarily with Docker Compose.

## Files:

- `otel-collector-config.yml`:
  - **Purpose**: Configuration for the primary OpenTelemetry Collector. This collector receives traces from the Ledger v3 POC application and forwards them to the SigNoz OpenTelemetry Collector.
  - **Details**: Defines OTLP gRPC and HTTP receivers (ports 4317, 4318) and an OTLP exporter configured to send data to `signoz-otel-collector:4317`. It includes a batch processor for efficient trace handling.

- `signoz-otel-collector-config.yaml`:
  - **Purpose**: Configuration for the SigNoz-specific OpenTelemetry Collector. This collector is responsible for processing traces, metrics, and logs and exporting them to the ClickHouse database, which is then used by the SigNoz UI.
  - **Details**: Defines an OTLP gRPC receiver (port 4317), memory limiter, and batch processors. It configures ClickHouse exporters for traces, metrics, and logs, specifying the ClickHouse endpoint, database names, and retry policies.

## Usage with Docker Compose:

The `docker-compose.yml` file in the root directory references these configuration files. When starting the Docker Compose environment, these files are mounted into their respective containers, ensuring that the services are configured correctly for operation and observability.

**Note**: The Ledger v3 POC application nodes are configured via command-line flags and environment variables directly in `docker-compose.yml`, not via configuration files.
