// Panel constructors that produce Grafana dashboard JSON directly.
//
// Each constructor takes the essential parameters (title, grid
// position, query/queries, unit) and fills in the visual boilerplate
// that the Grafana export carries. The values match the Grafana
// 11.x defaults the dashboard was authored on so the rendered
// dashboards are visually identical to the previous JSON export.

local nextID = {
  // Panel IDs only need to be unique within the dashboard. We assign
  // them sequentially at composition time (see main.jsonnet), so
  // panel constructors leave the id empty and the composer fills it.
};

local promDatasource = {
  type: 'prometheus',
  uid: '${datasource}',
};

local pyroDatasource = {
  type: 'grafana-pyroscope-datasource',
  uid: '${pyroscope}',
};

local defaultThresholds = {
  mode: 'absolute',
  steps: [
    { color: 'green', value: 0 },
    { color: 'red', value: 80 },
  ],
};

local timeseriesFieldConfigDefaults(unit, opts={}) = {
  color: { mode: 'palette-classic' },
  custom: {
    axisBorderShow: false,
    axisCenteredZero: false,
    axisColorMode: 'text',
    axisLabel: '',
    axisPlacement: 'auto',
    barAlignment: 0,
    barWidthFactor: 0.6,
    drawStyle: std.get(opts, 'drawStyle', 'line'),
    fillOpacity: std.get(opts, 'fillOpacity', 10),
    gradientMode: 'none',
    hideFrom: { legend: false, tooltip: false, viz: false },
    insertNulls: false,
    lineInterpolation: 'linear',
    lineWidth: 1,
    pointSize: 5,
    scaleDistribution: { type: std.get(opts, 'scale', 'linear') },
    showPoints: std.get(opts, 'showPoints', 'never'),
    showValues: false,
    spanNulls: std.get(opts, 'spanNulls', false),
    stacking: { group: 'A', mode: std.get(opts, 'stackMode', 'none') },
    thresholdsStyle: { mode: 'off' },
  },
  mappings: [],
  thresholds: defaultThresholds,
} + (if unit != null then { unit: unit } else {})
+ (if std.objectHas(opts, 'min') then { min: opts.min } else {})
+ (if std.objectHas(opts, 'max') then { max: opts.max } else {});

local timeseriesOptions = {
  legend: {
    calcs: [],
    displayMode: 'list',
    placement: 'bottom',
    showLegend: true,
  },
  tooltip: { hideZeros: false, mode: 'single', sort: 'none' },
};

// promTarget builds a prometheus query target. `refId` is mandatory
// when multiple targets share a panel (Grafana uses it for the
// override system); panels with a single target conventionally use
// 'A'.
local promTarget(expr, refId='A', legendFormat=null) = {
  datasource: promDatasource,
  editorMode: 'code',
  expr: expr,
  range: true,
  refId: refId,
} + (if legendFormat != null then { legendFormat: legendFormat } else {});

{
  // Re-export for callers that need the raw datasource/target
  // helpers (overrides, panel composition, …).
  promDatasource:: promDatasource,
  pyroDatasource:: pyroDatasource,
  promTarget:: promTarget,

  // reflow re-assigns the `y` coordinate of each panel by running a
  // simple 2-D bin-packing on a 24-column grid: panels are sorted by
  // their authored (y, x), then each is placed at the lowest y that
  // does not overlap previously-placed panels in its column range.
  // `x`, `w` and `h` are left untouched at this stage so the column
  // structure the author chose (e.g. two panels side by side at
  // w=12) is preserved. expandToFillRow is then applied so panels
  // that end up alone in their visual band stretch right to fill
  // the empty space rather than leaving a half-row gap. The combined
  // pass makes the dashboard immune to drift in the hand-edited y
  // values of the original Grafana export.
  reflow(panels)::
    if std.length(panels) == 0 then []
    else
      $.expandToFillRow($.repackY(panels)),

  // repackY does the bin-packing step on its own.
  repackY(panels)::
    local sortKey(p) = p.gridPos.y * 1000 + p.gridPos.x;
    local sorted = std.sort(panels, sortKey);
    local placeOne(state, p) =
      local x = p.gridPos.x;
      local w = p.gridPos.w;
      local h = p.gridPos.h;
      local maxY = std.foldl(
        function(acc, i) std.max(acc, state.skyline[i]),
        std.range(x, x + w - 1),
        0,
      );
      local newSkyline = std.mapWithIndex(
        function(i, v) if i >= x && i < x + w then maxY + h else v,
        state.skyline,
      );
      {
        skyline: newSkyline,
        out: state.out + [p { gridPos+: { y: maxY } }],
      };
    std.foldl(
      placeOne,
      sorted,
      { skyline: [0 for _ in std.range(0, 23)], out: [] },
    ).out,

  // expandToFillRow extends each panel's `w` rightwards to the
  // first edge of another panel sharing the same vertical band —
  // or to column 24 if none. This eliminates the half-row gaps
  // that appear after repackY when an odd number of panels share a
  // visual row (e.g. three panels intended as side-by-side that
  // landed in a 2-column section).
  expandToFillRow(panels)::
    std.map(
      function(p)
        local x = p.gridPos.x;
        local y = p.gridPos.y;
        local w = p.gridPos.w;
        local h = p.gridPos.h;
        local rightEdge = std.foldl(
          function(acc, q)
            if q == p then acc
            else
              local qx = q.gridPos.x;
              local qy = q.gridPos.y;
              local qh = q.gridPos.h;
              if qx >= x + w && qy < y + h && y < qy + qh then std.min(acc, qx)
              else acc,
          panels,
          24,
        );
        local newW = rightEdge - x;
        if newW > w then p { gridPos+: { w: newW } } else p,
      panels,
    ),

  // row produces a collapsed row that contains nested panels. The
  // nested panels are reflowed so authored y-coordinates do not
  // need to be kept globally consistent — the bin-packer regenerates
  // them based on (x, w, h) and the order the section file lists
  // them in.
  row(title, y, panels=[], collapsed=true):: {
    collapsed: collapsed,
    gridPos: { h: 1, w: 24, x: 0, y: y },
    panels: $.reflow(panels),
    title: title,
    type: 'row',
  },

  // timeseries builds the default time-series panel with one or
  // more PromQL targets. `targets` is a list of either strings (then
  // refIds are auto-assigned A, B, …) or { expr, legendFormat } objects.
  timeseries(title, gridPos, targets, unit=null, description=null, opts={}):: {
    type: 'timeseries',
    title: title,
    datasource: promDatasource,
    gridPos: gridPos,
    fieldConfig: {
      defaults: timeseriesFieldConfigDefaults(unit, opts),
      overrides: std.get(opts, 'overrides', []),
    },
    options: timeseriesOptions,
    targets: [
      local refId = std.char(std.codepoint('A') + i);
      if std.isString(targets[i]) then
        promTarget(targets[i], refId)
      else
        promTarget(targets[i].expr, refId, std.get(targets[i], 'legendFormat'))
      for i in std.range(0, std.length(targets) - 1)
    ],
  } + (if description != null then { description: description } else {}),

  // heatmap builds a Grafana heatmap panel — used here for queue
  // load distributions. The data source is expected to emit
  // histogram buckets (the *_bucket variants of a histogram metric);
  // the panel renders them with cell intensity proportional to count.
  heatmap(title, gridPos, expr, description=null, opts={}):: {
    type: 'heatmap',
    title: title,
    datasource: promDatasource,
    gridPos: gridPos,
    fieldConfig: {
      defaults: {
        custom: {
          hideFrom: { legend: false, tooltip: false, viz: false },
          scaleDistribution: { type: 'linear' },
        },
      },
      overrides: [],
    },
    options: {
      calculate: false,
      cellGap: 1,
      color: {
        exponent: 0.5,
        fill: 'dark-orange',
        mode: 'scheme',
        reverse: false,
        scale: 'exponential',
        scheme: std.get(opts, 'scheme', 'Spectral'),
        steps: 64,
      },
      exemplars: { color: 'rgba(255,0,255,0.7)' },
      filterValues: { le: 1e-9 },
      legend: { show: true },
      rowsFrame: { layout: 'auto', value: 'Count' },
      tooltip: { mode: 'single', showColorScale: false, yHistogram: false },
      yAxis: {
        axisPlacement: 'left',
        reverse: false,
      } + (if std.objectHas(opts, 'unit') then { unit: opts.unit } else {}),
    },
    pluginVersion: '11.4.0',
    targets: [
      promTarget(expr, 'A', std.get(opts, 'legendFormat', '{{le}}'))
      + { format: 'heatmap' },
    ],
  } + (if description != null then { description: description } else {}),

  // gauge displays a single-value gauge with thresholds.
  gauge(title, gridPos, expr, unit=null, description=null, opts={}):: {
    type: 'gauge',
    title: title,
    datasource: promDatasource,
    gridPos: gridPos,
    fieldConfig: {
      defaults: {
        color: { mode: 'thresholds' },
        mappings: [],
        thresholds: std.get(opts, 'thresholds', defaultThresholds),
      } + (if unit != null then { unit: unit } else {})
      + (if std.objectHas(opts, 'min') then { min: opts.min } else {})
      + (if std.objectHas(opts, 'max') then { max: opts.max } else {}),
      overrides: [],
    },
    options: {
      minVizHeight: 75,
      minVizWidth: 75,
      orientation: 'auto',
      reduceOptions: {
        calcs: [std.get(opts, 'reducer', 'lastNotNull')],
        fields: '',
        values: false,
      },
      showThresholdLabels: false,
      showThresholdMarkers: true,
      sizing: 'auto',
    },
    pluginVersion: '11.4.0',
    targets: [promTarget(expr, 'A', std.get(opts, 'legendFormat', null))],
  } + (if description != null then { description: description } else {}),

  // stat displays a single-value big-number with sparkline.
  stat(title, gridPos, expr, unit=null, description=null, opts={}):: {
    type: 'stat',
    title: title,
    datasource: promDatasource,
    gridPos: gridPos,
    fieldConfig: {
      defaults: {
        color: { mode: std.get(opts, 'colorMode', 'thresholds') },
        mappings: [],
        thresholds: std.get(opts, 'thresholds', defaultThresholds),
      } + (if unit != null then { unit: unit } else {}),
      overrides: [],
    },
    options: {
      colorMode: 'value',
      graphMode: 'area',
      justifyMode: 'auto',
      orientation: 'auto',
      percentChangeColorMode: 'standard',
      reduceOptions: {
        calcs: [std.get(opts, 'reducer', 'lastNotNull')],
        fields: '',
        values: false,
      },
      showPercentChange: false,
      textMode: 'auto',
      wideLayout: true,
    },
    pluginVersion: '11.4.0',
    targets: [promTarget(expr, 'A', std.get(opts, 'legendFormat', null))],
  } + (if description != null then { description: description } else {}),

  // histogram displays an instant snapshot distribution.
  histogram(title, gridPos, expr, unit=null, description=null, opts={}):: {
    type: 'histogram',
    title: title,
    datasource: promDatasource,
    gridPos: gridPos,
    fieldConfig: {
      defaults: {
        color: { mode: 'thresholds' },
        custom: {
          fillOpacity: 80,
          gradientMode: 'none',
          hideFrom: { legend: false, tooltip: false, viz: false },
          lineWidth: 1,
        },
        mappings: [],
        thresholds: defaultThresholds,
      } + (if unit != null then { unit: unit } else {}),
      overrides: [],
    },
    options: {
      bucketOffset: 0,
      combine: false,
      legend: {
        calcs: [],
        displayMode: 'list',
        placement: 'bottom',
        showLegend: true,
      },
    },
    pluginVersion: '11.4.0',
    targets: [promTarget(expr, 'A', std.get(opts, 'legendFormat', null)) + { format: 'heatmap', instant: true }],
  } + (if description != null then { description: description } else {}),

  // flamegraph wraps a Pyroscope flame-graph target.
  flamegraph(title, gridPos, profileType, description=null):: {
    type: 'flamegraph',
    title: title,
    datasource: pyroDatasource,
    gridPos: gridPos,
    targets: [{
      datasource: pyroDatasource,
      groupBy: [],
      labelSelector: '{service_name="ledger", version="$version"}',
      profileTypeId: profileType,
      queryType: 'profile',
      refId: 'A',
    }],
    options: {},
  } + (if description != null then { description: description } else {}),

  // withIDs walks a flat list of rows (each containing nested
  // panels) and assigns unique sequential IDs at every level.
  // Grafana requires IDs to be unique within the dashboard; how
  // they're allocated is irrelevant.
  withIDs(rows)::
    local flat = std.foldl(
      function(state, row)
        local rowID = state.next;
        local startNested = rowID + 1;
        local nested = std.mapWithIndex(
          function(i, p) p + { id: startNested + i },
          std.get(row, 'panels', [])
        );
        state + {
          next: startNested + std.length(nested),
          rows+: [row + { id: rowID, panels: nested }],
        },
      rows,
      { next: 1, rows: [] }
    );
    flat.rows,
}
