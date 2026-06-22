// Generator entry point for the Ledger Grafana dashboards.
//
// Pure-Jsonnet composition: every panel is constructed from
// lib/panels.libsonnet, grouped into sections under
// sections/<name>.libsonnet, and wrapped by lib/dashboard.libsonnet
// into a Grafana dashboard object. The result is emitted in both the
// OTel dot-notation form and the Prometheus underscore form via
// lib/transform.libsonnet, which walks the assembled tree and
// applies the naming policy declared in lib/naming.libsonnet.

local panels = import 'lib/panels.libsonnet';
local dashboard = import 'lib/dashboard.libsonnet';
local naming = import 'lib/naming.libsonnet';
local transform = import 'lib/transform.libsonnet';

local rows = panels.withIDs([
  import 'sections/system.libsonnet',
  import 'sections/transport.libsonnet',
  import 'sections/ready_loop.libsonnet',
  import 'sections/applier.libsonnet',
  import 'sections/pebble.libsonnet',
  import 'sections/read_index.libsonnet',
  import 'sections/caching.libsonnet',
  import 'sections/bloom.libsonnet',
  import 'sections/admission.libsonnet',
  import 'sections/controller.libsonnet',
  import 'sections/mirror.libsonnet',
  import 'sections/storage_disk.libsonnet',
  import 'sections/index_builder.libsonnet',
  import 'sections/pyroscope.libsonnet',
]);

local base = dashboard.dashboard('Ledger Metrics Dashboard', 'ledger-metrics', rows);

{
  ['ledger-metrics-' + mode + '.json']: transform.dashboard(base, mode)
  for mode in naming.modes
}
