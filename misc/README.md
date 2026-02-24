# Deployment Configuration Files

> **WARNING: The operator and devenv tooling in this directory are intended for development and testing purposes only.**
> **The official method for deploying Formance in production is the [Formance Stack Operator](https://github.com/formancehq/operator).**

This directory contains configuration files used for deploying the Ledger v3 POC application on Kubernetes.

## Structure:

- **`operator/`**: Kubernetes operator and its Helm chart for deploying and managing Ledger clusters
- **`devenv/`**: Pulumi application for deploying the observability stack and Ledger v3 POC
