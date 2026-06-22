// Storage Disk Usage section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Storage Disk Usage', 172, [
  panels.timeseries(
    'Disk Usage by Volume',
    { h: 8, w: 12, x: 0, y: 91 },
    [
      { expr: 'storage.disk.volume.bytes{service.cluster=~"$cluster", service.node_id=~"$node", volume="wal"}', legendFormat: 'WAL Volume (Node {{service.node_id}})' },
      { expr: 'storage.disk.volume.bytes{service.cluster=~"$cluster", service.node_id=~"$node", volume="data"}', legendFormat: 'Data Volume (Node {{service.node_id}})' },
    ], unit='bytes',
    description='Disk space used by each storage volume (WAL volume, data volume). Updated every 10 seconds by a background collector.',
  ),

  panels.timeseries(
    'Disk Usage by Volume',
    { h: 8, w: 12, x: 12, y: 91 },
    [
      { expr: 'storage.disk.volume.bytes{service.cluster=~"$cluster", service.node_id=~"$node", volume="wal"}', legendFormat: 'WAL (Node {{service.node_id}})' },
      { expr: 'storage.disk.volume.bytes{service.cluster=~"$cluster", service.node_id=~"$node", volume="data"}', legendFormat: 'Data (Node {{service.node_id}})' },
    ], unit='bytes',
    description='Disk space used by each storage volume (WAL volume, data volume). Updated every 10 seconds by a background collector.',
  ),
])
