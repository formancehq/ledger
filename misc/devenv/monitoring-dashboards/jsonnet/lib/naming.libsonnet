// Naming policy applied to metric and label names when generating
// dashboards for environments whose OTLP→Prometheus collector
// sanitises dots into underscores.
//
// This file is the dashboard-side implementation of the transform
// between the OpenTelemetry dot-notation our code emits and the
// form Prometheus actually stores after the collector. The Go-side
// runtime implementation lives in `internal/infra/monitoring/metrics`.
// The two implementations are NOT cross-checked by any automated
// test: `registry_test.go` only verifies metric-name coverage (every
// name listed in `lib/metrics.libsonnet` has a Go call site and
// vice versa). Algorithmic equivalence between
// `transformName(name, NamingProm)` in Go and
// `transformMetric(name, 'prom')` here is a contributor
// responsibility — diverging on the prefix or the unit map breaks
// dashboards silently.
//
// Seven modes are emitted (one JSON file per mode) covering the
// cross product of three orthogonal collector behaviours:
//
//   * otel — preserves dots end-to-end (Prom 3.x with
//     `otlp.translation_strategy: NoTranslation`).
//   * prom[-noprefix] — underscores; `ledger_` prefix when the
//     server runs in `--metrics-naming=prom`. No unit suffix and no
//     automatic `_total` (collector with `NormalizeName=false`).
//   * prom[-noprefix]-normalized — same plus the OTel→Prometheus
//     unit suffix (`us` → `microseconds`, `By` → `bytes`, …),
//     `_total` for monotonic counters, and `_ratio` for
//     dimensionless gauges. This is what the default contrib
//     collector and the Prometheus 3.x OTLP receiver produce.
//   * prom[-noprefix]-normalized-native — same as -normalized but
//     histograms are stored as native (single time series, no
//     `_bucket` / `_count` / `_sum` split, no `le` label).
//
// Scope of the policy: every metric our code emits is subject to
// the rename. OpenTelemetry semantic-convention auto-instrumentation
// (`go.*`, `process.*`, `system.*`, `http.*`) targets the *global*
// MeterProvider in the Go side and bypasses our factory entirely —
// in `prom-normalized` mode we still apply the unit/total
// transformation to those names because the collector does the same
// thing to them on its way to Prometheus.

local metadata = import 'metric_metadata.libsonnet';

{
  // prefix is prepended (with an underscore separator in `prom`
  // modes) to every metric our code emits.
  prefix:: 'ledger',

  // semconvPrefixes are the metric-name prefixes that come from
  // OpenTelemetry semantic-convention auto-instrumentation (the SDK
  // runtime instrumentation, otelhttp, host metrics). Those
  // instruments are emitted via the global MeterProvider — the
  // ledger factory never sees them — so the dashboard merely
  // de-dots them (which the collector does too) and never prefixes.
  semconvPrefixes:: [
    'go.',
    'process.',
    'system.',
    'http.',
    'rpc.',
    'network.',
    'url.',
    'k6_',
    'k6.',
  ],

  // unitMap is the UCUM-to-Prometheus translation table the OTel
  // Prometheus exporter uses. It matches
  // pkg/translator/prometheus/normalize_name.go in the upstream
  // contrib repo (commit 2024-10) and must be updated if new units
  // appear in the registry. `1` is the OTel sentinel for
  // "dimensionless" — no suffix.
  unitMap:: {
    '1': '',
    '%': 'percent',
    d: 'days',
    h: 'hours',
    min: 'minutes',
    s: 'seconds',
    ms: 'milliseconds',
    us: 'microseconds',
    ns: 'nanoseconds',
    By: 'bytes',
    KiBy: 'kibibytes',
    MiBy: 'mebibytes',
    GiBy: 'gibibytes',
    TiBy: 'tibibytes',
    KBy: 'kilobytes',
    MBy: 'megabytes',
    GBy: 'gigabytes',
    Hz: 'hertz',
    Cel: 'celsius',
    V: 'volts',
    A: 'amperes',
    J: 'joules',
    W: 'watts',
    g: 'grams',
    m: 'meters',
    // already-translated forms emitted by some call sites
    microseconds: 'microseconds',
    milliseconds: 'milliseconds',
    seconds: 'seconds',
    bytes: 'bytes',
  },

  // unitSuffix maps an OTel unit string to the Prometheus suffix.
  // Annotation-only units (`{hits}`, `{misses}`, `{operations}`)
  // collapse to empty per the upstream translator. Unit `1` is a
  // special case: for gauges the translator emits `_ratio` (a
  // dimensionless gauge is almost always a 0..1 ratio); for any
  // other kind it emits no suffix.
  unitSuffix(unit, kind)::
    if unit == null || unit == '' then ''
    else if unit == '1' then (if kind == 'gauge' then 'ratio' else '')
    else if std.startsWith(unit, '{') then ''
    else std.get($.unitMap, unit, unit),

  // normalizeName applies the upstream OTel→Prom transformation
  // (unit suffix + `_total` for counters + `_ratio` for
  // dimensionless gauges) to a name that has already been
  // de-dotted. Mirrors the upstream `addUnitTokens` and `_total`
  // insertion rules.
  normalizeName(name, kind, unit)::
    local sfx = $.unitSuffix(unit, kind);
    local withUnit =
      if sfx == '' || std.endsWith(name, '_' + sfx) then name
      else name + '_' + sfx;
    if kind == 'counter' && !std.endsWith(withUnit, '_total') then
      withUnit + '_total'
    else withUnit,

  // isSemconv returns true if a metric name comes from OTel
  // auto-instrumentation.
  isSemconv(name)::
    std.foldl(
      function(acc, p) acc || std.startsWith(name, p),
      $.semconvPrefixes,
      false,
    ),

  dotsToUnderscores(s):: std.strReplace(s, '.', '_'),

  // metadataOf returns { kind, unit } for an OTel-style metric name
  // (the dotted form, before any rewriting). Returns null if the
  // name is unknown — typically a semconv metric we don't track.
  metadataOf(name):: std.get(metadata, name, null),

  // histogramSuffixes are the per-metric variants a Prometheus
  // histogram is exposed as. Dashboard queries reference
  // `<metric>_bucket`/`<metric>_count`/`<metric>_sum`; the unit /
  // _total normalisation applies to the base, not the suffix.
  histogramSuffixes:: ['_bucket', '_count', '_sum'],

  // stripHistogramSuffix returns [suffix, base] for a name that
  // matches one of [`_bucket`, `_count`, `_sum`], else ['', name].
  stripHistogramSuffix(name)::
    local match(suffixes) =
      if std.length(suffixes) == 0 then ['', name]
      else
        local sfx = suffixes[0];
        if std.endsWith(name, sfx) then
          [sfx, std.substr(name, 0, std.length(name) - std.length(sfx))]
        else match(suffixes[1:]);
    match($.histogramSuffixes),

  // modeConfig returns the policy knobs for a given mode name. Three
  // orthogonal choices:
  //   prefix    — add the `ledger_` namespace to our metrics?
  //   normalize — apply the OTel→Prom unit-suffix and `_total` rules?
  //   native    — Prometheus 3.x stored OTel histograms as native
  //               histograms (one time series carrying the bucket
  //               data, no `_bucket`/`_count`/`_sum` split, no `le`
  //               label). When true, `_bucket` suffixes are dropped
  //               from queries and the `le` label is removed from
  //               grouping clauses.
  //
  // Semconv metric names are never prefixed regardless of `prefix`.
  modeConfig(mode)::
    if mode == 'otel' then { otel: true, prefix: false, normalize: false, native: false }
    else if mode == 'prom' then { otel: false, prefix: true, normalize: false, native: false }
    else if mode == 'prom-normalized' then { otel: false, prefix: true, normalize: true, native: false }
    else if mode == 'prom-normalized-native' then { otel: false, prefix: true, normalize: true, native: true }
    else if mode == 'prom-noprefix' then { otel: false, prefix: false, normalize: false, native: false }
    else if mode == 'prom-noprefix-normalized' then { otel: false, prefix: false, normalize: true, native: false }
    else if mode == 'prom-noprefix-normalized-native' then { otel: false, prefix: false, normalize: true, native: true }
    else error 'unknown mode: ' + mode,

  // transformMetric applies the policy to a single metric name.
  // Inputs are always in OTel dot-notation, optionally with a
  // Prometheus histogram suffix (`_bucket` / `_count` / `_sum`).
  // The output is determined by [modeConfig].
  //
  // In `native` mode the `_bucket` suffix is dropped because a
  // native histogram exposes the buckets through the bare metric
  // (e.g. `histogram_quantile(0.95, rate(metric[5m]))`). The
  // `_sum` and `_count` suffixes are deliberately preserved — they
  // don't exist as separate series in native histograms, so the
  // panels that depend on `_sum / _count` ratios surface a "No
  // data" rather than a silent ratio of 1; the operator can then
  // rewrite those panels to `histogram_avg`.
  transformMetric(name, mode):: (
    local cfg = $.modeConfig(mode);
    if cfg.otel then name
    else
      local stripped = $.stripHistogramSuffix(name);
      local origSfx = stripped[0];
      local base = stripped[1];
      local effectiveSfx =
        if cfg.native && origSfx == '_bucket' then ''
        else origSfx;
      local isSC = $.isSemconv(base);
      local underscored = $.dotsToUnderscores(base);
      // Skip the application prefix when the name already carries
      // it (an existing `ledger.…` instrument from call sites that
      // hand-rolled the namespace) — otherwise we'd emit
      // `ledger_ledger_…`. Mirrors the same check in Go's
      // `transformName`.
      local alreadyPrefixed = std.startsWith(underscored, $.prefix + '_');
      local prefixed =
        if isSC || !cfg.prefix || alreadyPrefixed then underscored
        else $.prefix + '_' + underscored;
      local transformed =
        if cfg.normalize then
          local meta = $.metadataOf(base);
          if meta == null then prefixed
          else $.normalizeName(prefixed, meta.kind, meta.unit)
        else prefixed;
      transformed + effectiveSfx
  ),

  // transformLabel applies the policy to a label name. Labels are
  // never prefixed; the collector simply replaces dots with
  // underscores, so we mirror that here.
  transformLabel(name, mode)::
    if mode == 'otel' then name
    else $.dotsToUnderscores(name),

  // The output modes — the dashboard generator iterates over this
  // list to emit one JSON file per mode. Pick the one matching your
  // (server flag, collector normalisation) combination — see
  // misc/devenv/monitoring-dashboards/README.md for the matrix.
  modes:: [
    'otel',
    'prom',
    'prom-normalized',
    'prom-normalized-native',
    'prom-noprefix',
    'prom-noprefix-normalized',
    'prom-noprefix-normalized-native',
  ],
}
