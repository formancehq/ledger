# Ledger-Exp Dev Environment URLs

This page lists the useful URLs and endpoints for the `ledger-exp` development environment.

## External URLs

- Ledger HTTP API: `https://ledger-exp.v2.formance.dev`
- Grafana: `https://grafana.ledger-exp.v2.formance.dev`

## In-Cluster Endpoints

- Ledger HTTP (ClusterIP): `http://ledger-exp.ledger.svc.cluster.local:9000`
- Ledger gRPC/Raft (ClusterIP): `ledger-exp.ledger.svc.cluster.local:8888`
- VictoriaMetrics: `http://vm-victoria-metrics-single-server.monitoring.svc.cluster.local:8428`
- OpenTelemetry Collector (gRPC): `otel-opentelemetry-collector.monitoring.svc.cluster.local:4317`

## Discovering Service URLs

If the ingress hosts change, you can confirm them with:

```bash
kubectl get ingress -A
```

To check service endpoints inside the cluster:

```bash
kubectl get svc -n ledger
kubectl get svc -n monitoring
```
