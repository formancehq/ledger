# Deployment Configuration Files

This directory contains configuration files used for deploying the Ledger v3 POC application on Kubernetes.

## Structure:

- **`chart/`**: Helm chart for deploying the Ledger v3 POC application
- **`k8s/`**: Kubernetes manifests and configuration files for various services:
  - **`grafana/`**: Grafana configuration including dashboards and datasources
  - **`victoriametrics/`**: VictoriaMetrics configuration
  - **`otlp/`**: OpenTelemetry Collector configuration
  - **`tempo/`**: Tempo (tracing) configuration
  - **`loki/`**: Loki (logging) configuration
  - **`promtail/`**: Promtail (log collection) configuration
