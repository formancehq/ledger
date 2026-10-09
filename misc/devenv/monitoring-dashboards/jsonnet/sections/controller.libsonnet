// Controller section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Controller', 170, [
  panels.timeseries(
    'Apply Duration (p50, p95, p99)',
    { h: 8, w: 24, x: 0, y: 107 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(ctrl.apply.duration_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (le, k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id))', legendFormat: 'p50 - {{k8s.namespace.name}}/{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(ctrl.apply.duration_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (le, k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id))', legendFormat: 'p95 - {{k8s.namespace.name}}/{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(ctrl.apply.duration_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (le, k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id))', legendFormat: 'p99 - {{k8s.namespace.name}}/{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='s',
    description='End-to-end duration of a batch Apply call through the controller, including admission, preload, marshal, propose, and FSM wait.',
  ),

  panels.timeseries(
    'gRPC Apply Duration (p50, p95, p99)',
    { h: 8, w: 24, x: 0, y: 108 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(grpc.apply.duration_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (le, k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id))', legendFormat: 'p50 - {{k8s.namespace.name}}/{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(grpc.apply.duration_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (le, k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id))', legendFormat: 'p95 - {{k8s.namespace.name}}/{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(grpc.apply.duration_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (le, k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id))', legendFormat: 'p99 - {{k8s.namespace.name}}/{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='s',
    description='Total duration of the gRPC Apply handler, including auth, ctrl.Apply, and response signing.',
  ),
])
