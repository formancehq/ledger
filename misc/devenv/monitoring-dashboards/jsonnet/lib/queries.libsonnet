// PromQL helper functions used by panel constructors.
//
// These produce the *OTel-form* expressions (dot notation). The
// transform.libsonnet walker rewrites them on a per-mode basis when
// the dashboard is generated for `prom`, so call sites never have to
// branch on the naming mode.

{
  // clusterNode is the standard label-selector applied to every
  // application metric: filter by the operator-selected cluster and
  // node so dashboards work in multi-cluster Grafana setups.
  clusterNode:: 'service.cluster=~"$cluster", service.node_id=~"$node"',

  // histogramAvg emits a sentinel string that lib/transform.libsonnet
  // expands AT GENERATION TIME, differently per histogram mode:
  //
  //   classic — sum(rate(<metric>_sum{sel}[$__rate_interval])) by (<by>)
  //           / sum(rate(<metric>_count{sel}[$__rate_interval])) by (<by>)
  //   native  — histogram_avg(sum(rate(<metric>{sel}[$__rate_interval])) by (<by>))
  //
  // Section files call this helper instead of writing the ratio
  // inline; the classic form does not work on Prometheus native
  // histograms (no _sum / _count series) and the native form
  // (`histogram_avg`) does not work on classic histograms (the
  // function is undefined on bucket-fan-out series). Picking the
  // right one is a transform-time decision.
  //
  // - `metric` is the OTel dot-notation base name without any
  //   suffix (`admission.command.duration`, not `..._sum`).
  // - `by` is an optional list of OTel label names in dot form
  //   (`['service.node_id']`). The walker rewrites them per mode.
  // - `selector` is the label filter inside `{…}` minus the braces.
  //   Defaults to the standard cluster/node filter.
  histogramAvg(metric, by=[], selector=null)::
    local sel = if selector == null then $.clusterNode else selector;
    '__hist_avg(' + metric + '|' + std.join(',', by) + '|' + sel + ')__',

  // cluster filters only by cluster (used for cluster-wide rollups
  // like the leader gauge).
  cluster:: 'service.cluster=~"$cluster"',

  // selectorWith combines the standard cluster/node filter with
  // extra label selectors.
  selectorWith(extra):: $.clusterNode + (if std.length(extra) == 0 then '' else ', ' + extra),

  // rate returns a per-second rate over $__rate_interval.
  rate(metric, selector=null)::
    'rate(' + metric + '{' + (if selector == null then $.clusterNode else selector) + '}[$__rate_interval])',

  // sumRate aggregates the rate; `by` is a list of labels.
  sumRate(metric, by=[], selector=null)::
    local r = $.rate(metric, selector);
    if std.length(by) == 0 then 'sum(' + r + ')'
    else 'sum(' + r + ') by (' + std.join(', ', by) + ')',

  // histogramQuantile computes a percentile over a histogram metric
  // (where `metric` is the base name; the _bucket suffix is added
  // automatically) using $__rate_interval and aggregating by the
  // standard label set.
  histogramQuantile(percentile, metric, by=['le', 'service.node_id'], selector=null)::
    'histogram_quantile(' + percentile + ', sum(rate(' + metric + '_bucket{' +
    (if selector == null then $.clusterNode else selector) + '}[$__rate_interval])) by (' +
    std.join(', ', by) + '))',
}
