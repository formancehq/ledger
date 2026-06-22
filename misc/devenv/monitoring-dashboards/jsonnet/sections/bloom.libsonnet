// Bloom Filter section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Bloom Filter', 168, [
  panels.timeseries(
    'Bloom Lookup Rate by Type',
    { h: 8, w: 12, x: 0, y: 1 },
    [
      { expr: 'sum(rate(bloom.lookups{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id)', legendFormat: '{{type}} (Node {{service.node_id}})' },
    ], unit='ops',
    description=|||
      Rate of bloom filter checks per second, broken down by attribute type.
      
      Shows how heavily each bloom filter is being queried during transaction processing.
   |||,
  ),

  panels.timeseries(
    'Bloom Filter Efficiency (% Negatives)',
    { h: 8, w: 12, x: 12, y: 1 },
    [
      { expr: 'sum(rate(bloom.negatives{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id) / sum(rate(bloom.lookups{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id) * 100', legendFormat: '{{type}} (Node {{service.node_id}})' },
    ], unit='percent',
    description=|||
      Percentage of bloom filter lookups that returned definitely-not-present, by type.
      
      Higher values mean the bloom filter is effectively avoiding Pebble Gets. A value of 80% means 80% of lookups were short-circuited without hitting storage.
   |||,
  ),

  panels.timeseries(
    'Bloom Negatives Rate (Pebble Gets Avoided)',
    { h: 8, w: 12, x: 0, y: 9 },
    [
      { expr: 'sum(rate(bloom.negatives{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id)', legendFormat: '{{type}} (Node {{service.node_id}})' },
    ], unit='ops',
    description=|||
      Rate of bloom filter checks that returned definitely-not-present, by type.
      
      Each negative result represents a Pebble Get that was avoided, directly saving I/O.
   |||,
  ),

  panels.timeseries(
    'Bloom Adds Rate',
    { h: 8, w: 12, x: 12, y: 9 },
    [
      { expr: 'sum(rate(bloom.adds{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id)', legendFormat: '{{type}} (Node {{service.node_id}})' },
    ], unit='ops',
    description=|||
      Rate of keys being added to bloom filters, by type.
      
      Shows how fast bloom filters are growing. High add rates may indicate the filter is approaching its configured capacity.
   |||,
  ),

  panels.timeseries(
    'Bloom Ready',
    { h: 8, w: 12, x: 0, y: 17 },
    [
      { expr: 'bloom.ready{service.cluster=~"$cluster", service.node_id=~"$node"}', legendFormat: 'Node {{service.node_id}}' },
    ], unit='bool',
    description=|||
      Bloom filter readiness state (1 = ready, 0 = populating).
      
      During startup, bloom filters must be populated from the WAL before they can serve lookups. Until ready, all lookups fall through to Pebble.
   |||,
  ),

  panels.timeseries(
    'Bloom False Positive Rate',
    { h: 8, w: 12, x: 0, y: 26 },
    [
      { expr: 'sum(rate(bloom.false_positives{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id) / sum(rate(bloom.lookups{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval]) - rate(bloom.negatives{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (type, service.node_id)', legendFormat: '{{type}} (Node {{service.node_id}})' },
    ], unit='percentunit',
    description=|||
      Rate of bloom filter false positives per second, by type.
      
      A false positive occurs when MayContain returns 'maybe present' but the subsequent Pebble Get finds nothing. These represent wasted I/O. The ratio false_positives / (lookups - negatives) gives the empirical false positive rate.
   |||,
  ),
])
