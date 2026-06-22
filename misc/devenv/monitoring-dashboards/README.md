# Monitoring dashboards

This directory holds the Grafana dashboards that ship with the
Ledger v3 deployment. Dashboards are generated from Jsonnet sources
and applied to a Grafana instance via a Pulumi program.

## Layout

```
misc/devenv/monitoring-dashboards/
├── jsonnet/
│   ├── main.jsonnet                  # generator entry point
│   ├── jsonnetfile.json              # grafonnet dependency manifest
│   ├── lib/
│   │   ├── naming.libsonnet          # otel ↔ prom rename policy
│   │   ├── metrics.libsonnet         # registry of applicative metric names
│   │   ├── panels.libsonnet          # panel constructors (timeseries, heatmap, …)
│   │   ├── queries.libsonnet         # PromQL helpers (rate, p50/p95, …)
│   │   ├── dashboard.libsonnet       # top-level dashboard envelope
│   │   └── transform.libsonnet       # naming-aware walker (otel → prom)
│   └── sections/                     # one file per dashboard row
│       ├── system.libsonnet
│       ├── transport.libsonnet
│       ├── ready_loop.libsonnet
│       ├── applier.libsonnet
│       ├── pebble.libsonnet
│       ├── read_index.libsonnet
│       ├── caching.libsonnet
│       ├── bloom.libsonnet
│       ├── admission.libsonnet
│       ├── controller.libsonnet
│       ├── mirror.libsonnet
│       ├── storage_disk.libsonnet
│       ├── index_builder.libsonnet
│       └── pyroscope.libsonnet
├── config/
│   └── dashboards/                   # generated artifacts (committed)
│       ├── ledger-metrics-otel.json  # OTel dot-notation variant
│       └── ledger-metrics-prom.json  # Prometheus underscore variant
├── main.go                           # Pulumi program — reads config/dashboards/
└── README.md                         # this file
```

The dashboard is defined entirely in Jsonnet — there is no longer
any Grafana JSON export feeding the generator. Every panel is built
from one of the constructors in `lib/panels.libsonnet`
(`timeseries`, `heatmap`, `gauge`, `stat`, `histogram`,
`flamegraph`), and `sections/*.libsonnet` is the source of truth for
panel layout, titles, queries and descriptions.

## Generating the dashboards

```bash
just generate-dashboards
```

The recipe runs `jb install` once to vendor `grafonnet`, then
`jsonnet -m config/dashboards jsonnet/main.jsonnet`. The output is two
JSON files under `config/dashboards/`. Both are committed so Pulumi
can deploy without invoking Jsonnet.

`generate-dashboards` is part of `just pre-commit`; CI fails if the
generated files have drifted from the source.

## Seven variants

The OTLP→Prometheus collector deployed in front of Grafana converts
dots to underscores in metric and label names — a property of every
official OTel collector. Whether that conversion happens, whether
unit suffixes and `_total` are appended, and whether histograms are
stored classic or native are all collector / Prometheus settings
that vary by environment. The generator emits one variant per
combination so the same source produces a dashboard that works for
each.

Pick the file matching your *(server flag, collector behaviour,
histogram representation)* combination — the "Server" column is
what the ledger emits, the "Collector" column is what the OTel→Prom
translator on top of it does (`pkg.translator.prometheus.NormalizeName`
in the contrib collector, or `otlp.translation_strategy` in
Prometheus 3.x's built-in OTLP receiver).

| File | Server | Collector | Histograms | Examples |
| ---- | ------ | --------- | ---------- | -------- |
| `ledger-metrics-otel.json`                            | `--metrics-naming=otel` | preserves dots                       | classic | `raft.fsm.logs_appended`, `service.cluster` |
| `ledger-metrics-prom.json`                            | `--metrics-naming=prom` | de-dots only (`NormalizeName=false`) | classic | `ledger_raft_fsm_logs_appended`, `ledger_admission_command_duration_bucket` |
| `ledger-metrics-prom-normalized.json`                 | `--metrics-naming=prom` | full normalisation (default)         | classic | `ledger_raft_fsm_logs_appended_total`, `ledger_admission_command_duration_microseconds_bucket` |
| `ledger-metrics-prom-normalized-native.json`          | `--metrics-naming=prom` | full normalisation (default)         | native  | `ledger_admission_command_duration_microseconds`, queried via `histogram_quantile(0.95, rate(metric[5m]))` directly |
| `ledger-metrics-prom-noprefix.json`                   | `--metrics-naming=otel` | de-dots only (`NormalizeName=false`) | classic | `raft_fsm_logs_appended`, `admission_command_duration_bucket` |
| `ledger-metrics-prom-noprefix-normalized.json`        | `--metrics-naming=otel` | full normalisation (default)         | classic | `raft_fsm_logs_appended_total`, `admission_command_duration_microseconds_bucket` |
| `ledger-metrics-prom-noprefix-normalized-native.json` | `--metrics-naming=otel` | full normalisation (default)         | native  | `admission_command_duration_microseconds`, queried via `histogram_quantile(0.95, rate(metric[5m]))` directly |

The `-native` variants target Prometheus 3.x with the OTLP receiver
in its default mode (or an OTel collector with
`prometheusremotewrite.convertHistogramsToNHCB=true`): OTel
histograms are stored as **native histograms** — a single time
series carrying the bucket data, with no `_bucket` / `_count` /
`_sum` split and no `le` label. In those variants the dashboard:

- drops the `_bucket` suffix from metric references inside
  `histogram_quantile` calls;
- removes the `le` label from `by (...)` grouping clauses.

Histogram-mean panels are written in section files using the
`queries.histogramAvg(metric, by=…, selector=…)` helper instead of
the raw `_sum / _count` ratio. The transform expands the helper
differently per histogram mode:

  classic — `sum(rate(<metric>_sum[…])) by (…) / sum(rate(<metric>_count[…])) by (…)`
  native  — `histogram_avg(sum(rate(<metric>[…])) by (…))`

This is necessary because `histogram_avg` is undefined on classic
histograms and the `_sum` / `_count` series do not exist on native
histograms. The helper picks the right form at generation time so
every variant has working ratio panels.

The "**normalised**" variants assume the standard OTel→Prom
transformation: dots → underscores, the UCUM unit suffix (`By` →
`_bytes`, `us` → `_microseconds`, `s` → `_seconds`), `_total`
appended to monotonic counters, and `_ratio` appended to gauges
with the dimensionless unit `1`. Most cloud Prometheus / Thanos /
VictoriaMetrics with `OTLP_NORMALIZE=true` deployments fall in
this category.

Every metric the server emits — `admission.*`, `cache.*`, `wal.*`,
`raft.*` (our instrumentation of etcd-raft), `pebble.*` (our
instrumentation of Pebble) — gains a `ledger_` prefix in the `prom`
variant. See [../../../docs/ops/monitoring.md](../../../docs/ops/monitoring.md)
for the rationale. OTel semantic-convention auto-instrumentation
(`go.*`, `process.*`, `system.*`, `http.*`) is emitted via the
global MeterProvider in the server and bypasses the renaming policy
entirely; in the `prom` variant those names are merely de-dotted by
the collector, never prefixed. Attribute names (`service.cluster`,
`network.io.direction`, …) are de-dotted too but never prefixed
either.

The naming policy is implemented twice: in Go (the runtime
factory in `internal/infra/monitoring/metrics`) and in Jsonnet
(`lib/naming.libsonnet`, used to generate the dashboards). The
Go-side test `registry_test.go` cross-checks **name coverage**
only: every metric listed in `lib/metrics.libsonnet` must be
emitted by a call site, and every emitted name must be listed.
It does **not** assert that `transformName(name, NamingProm)` in
Go and `transformMetric(name, 'prom')` in Jsonnet produce
identical output — keeping the two transformations algorithmically
in sync is a contributor responsibility, and any divergence in the
prefix or the unit table will silently break dashboards.

## Workflow when adding a metric

1. Create the instrument in Go on any meter obtained from the
   injected `metric.MeterProvider`. There is no per-meter allowlist:
   the naming factory rewrites every instrument it sees in `prom`
   mode. If the new metric uses an OTel semantic-convention prefix
   (`go.*`, `process.*`, `system.*`, `http.*`, …) add that prefix to
   `naming.libsonnet#semconvPrefixes` so the dashboard transform
   skips it.
2. Add the metric name to `metrics.libsonnet` under the matching
   meter section. Run `go test ./internal/infra/monitoring/metrics/`
   to confirm the registry is in sync.
3. Add a panel in the relevant `sections/<name>.libsonnet` (or a
   new section file) using one of the panel constructors. Author
   queries in OTel dot-notation; `transform.libsonnet` rewrites them
   for the prom variants at generation time.
4. Run `just generate-dashboards` and commit the regenerated artifacts.

## Workflow when adding a section

1. Create `sections/<name>.libsonnet` exporting a `panels.row(title, y, [panel, …])`.
2. Add the import to `main.jsonnet`'s `rows = panels.withIDs([...])`.
3. Regenerate.
