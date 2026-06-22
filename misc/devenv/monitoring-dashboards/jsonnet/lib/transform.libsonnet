// transform.libsonnet
//
// Walks an entire Grafana dashboard JSON tree (loaded from
// `ledger-metrics.json`) and rewrites every PromQL expression,
// templating query and variable regex according to the configured
// naming policy. Used by main.jsonnet to emit the otel and prom
// variants of the dashboard from a single source.
//
// Once individual panels are rewritten into proper Jsonnet modules
// (see README), they will replace nodes in the loaded tree before
// this transform runs.

local naming = import 'naming.libsonnet';

{
  // metricNamePattern captures a contiguous run of dotted identifier
  // characters — the longest token that could be a metric name.
  // PromQL expressions reference metric names as bare identifiers
  // (followed by an optional label selector), histogram-derived
  // suffixes (_bucket, _count, _sum) or inside an explicit
  // `{__name__="..."}` selector. The walker handles all three.

  // isIdentChar returns true for characters that may appear inside
  // an unquoted PromQL identifier (metric name, label name).
  isIdentChar(c)::
    (c >= 'a' && c <= 'z') ||
    (c >= 'A' && c <= 'Z') ||
    (c >= '0' && c <= '9') ||
    c == '_' || c == ':' || c == '.',

  // isIdentStart returns true for characters that may legitimately
  // start a PromQL identifier. Crucially excludes digits and `.`:
  // tokens like `0.50` (a numeric literal — the percentile argument
  // to histogram_quantile) are NOT identifiers and must not be
  // collected. Treating them as identifiers caused
  // `histogram_quantile(0.50, …)` to be rewritten as
  // `histogram_quantile(ledger_0_50, …)`.
  isIdentStart(c)::
    (c >= 'a' && c <= 'z') ||
    (c >= 'A' && c <= 'Z') ||
    c == '_' || c == ':',

  // collectIdent scans s from i and returns [end, ident] where ident
  // is the longest contiguous identifier starting at i. end is the
  // index of the first non-identifier character (or len(s)).
  collectIdent(s, i)::
    local n = std.length(s);
    local step(j) =
      if j >= n then j
      else if $.isIdentChar(s[j]) then step(j + 1)
      else j;
    local end = step(i);
    [end, std.substr(s, i, end - i)],

  // stripHistogramSuffix returns the base metric name with any
  // _bucket / _count / _sum suffix removed, plus the suffix (or '').
  stripHistogramSuffix(name)::
    local suffixes = ['_bucket', '_count', '_sum'];
    local match(idx) =
      if idx >= std.length(suffixes) then ['', name]
      else
        local sfx = suffixes[idx];
        if std.endsWith(name, sfx) then
          [sfx, std.substr(name, 0, std.length(name) - std.length(sfx))]
        else match(idx + 1);
    match(0),

  // shouldRewriteIdent returns true if ident looks like a metric name
  // emitted by the application or by an instrumented library —
  // anything that contains a dot OR matches the histogram-suffix
  // shape on a known metric. Plain identifiers without dots and
  // without a known suffix are left alone (they are PromQL function
  // names, label names, etc.).
  shouldRewriteIdent(ident)::
    // Fast path: any dot means it's a metric name in OTel notation.
    if std.length(std.findSubstr('.', ident)) > 0 then true
    else
      // Slow path: identifiers without dots can still be metric
      // names if they end with a histogram-derived suffix.
      local stripped = $.stripHistogramSuffix(ident);
      stripped[0] != '' && std.length(std.findSubstr('.', stripped[1])) > 0,

  // rewriteIdent applies the naming policy to a single identifier
  // that looks like a metric name. `naming.transformMetric` strips
  // any `_bucket`/`_count`/`_sum` suffix internally so it can
  // inject the unit suffix between the base and the histogram
  // marker — we delegate fully.
  rewriteIdent(ident, mode):: naming.transformMetric(ident, mode),

  // collectString scans s from i (which must point to the opening
  // quote) and returns [end, content] where content is the unescaped
  // string body and end points past the closing quote.
  collectString(s, i)::
    local n = std.length(s);
    local quote = s[i];
    local step(j, acc) =
      if j >= n then [j, acc]
      else if s[j] == quote then [j + 1, acc]
      else if s[j] == '\\' && j + 1 < n then
        step(j + 2, acc + s[j] + s[j + 1])
      else step(j + 1, acc + s[j]);
    step(i + 1, ''),

  // skipWhitespace returns the next index >= i that is not a space
  // or tab character. PromQL allows whitespace between selectors and
  // their comparators / commas.
  skipWhitespace(s, i)::
    local n = std.length(s);
    local step(j) =
      if j >= n then j
      else if s[j] == ' ' || s[j] == '\t' || s[j] == '\n' then step(j + 1)
      else j;
    step(i),

  // followedByComparator returns true if the next non-whitespace
  // character at or after i is one of `=`, `!`, or `~` (the prefix of
  // any PromQL label comparator).
  followedByComparator(s, i)::
    local n = std.length(s);
    local j = $.skipWhitespace(s, i);
    j < n && (s[j] == '=' || s[j] == '!' || s[j] == '~'),

  // labelListKeywords are PromQL keywords whose parenthesised
  // argument is a *label* list rather than an expression — the
  // identifiers inside must be rewritten with transformLabel, not
  // transformMetric. Without this distinction a clause such as
  // `sum by (service.node_id)` would have its label name treated
  // as a metric and incorrectly gain the `ledger_` prefix.
  labelListKeywords:: {
    by: true,
    without: true,
    on: true,
    ignoring: true,
    group_left: true,
    group_right: true,
  },

  // inLabelList returns true when the top of the paren stack is a
  // label-list context (encoded as `L`; normal parens are `N`).
  inLabelList(parenStack)::
    std.length(parenStack) > 0 &&
    parenStack[std.length(parenStack) - 1] == 'L',

  // expandHistAvg renders the appropriate PromQL for a histogram
  // mean given a base metric, an optional grouping label list and a
  // label selector. Classic and native histograms have
  // incompatible representations, so this function is the bridge
  // section files lean on via queries.histogramAvg.
  expandHistAvg(metric, by, selector, native)::
    local byClause = if std.length(by) == 0 then '' else ' by (' + std.join(', ', by) + ')';
    local braced = if selector == '' then '' else '{' + selector + '}';
    if native then
      'histogram_avg(sum(rate(' + metric + braced + '[$__rate_interval]))' + byClause + ')'
    else
      'sum(rate(' + metric + '_sum' + braced + '[$__rate_interval]))' + byClause +
      ' / sum(rate(' + metric + '_count' + braced + '[$__rate_interval]))' + byClause,

  // preExpandHelpers replaces every `__hist_avg(metric|by|sel)__`
  // sentinel emitted by queries.histogramAvg with the histogram-
  // mode-appropriate PromQL. Runs before the walker so the emitted
  // PromQL still goes through the regular prefix / unit-suffix /
  // label rewriting like any other expression.
  preExpandHelpers(expr, mode)::
    local marker = '__hist_avg(';
    local chunks = std.split(expr, marker);
    if std.length(chunks) == 1 then expr
    else
      local cfg = naming.modeConfig(mode);
      local processChunk(chunk) =
        local closePos = std.findSubstr(')__', chunk);
        if std.length(closePos) == 0 then marker + chunk
        else
          local body = std.substr(chunk, 0, closePos[0]);
          local rest = std.substr(chunk, closePos[0] + 3, std.length(chunk) - closePos[0] - 3);
          local parts = std.split(body, '|');
          local metric = parts[0];
          local by =
            if std.length(parts) < 2 || parts[1] == '' then []
            else std.split(parts[1], ',');
          local selector = if std.length(parts) < 3 then '' else parts[2];
          $.expandHistAvg(metric, by, selector, cfg.native) + rest;
      chunks[0] + std.join('', [
        processChunk(chunks[i])
        for i in std.range(1, std.length(chunks) - 1)
      ]),

  // dropLeFromGrouping removes the `le` label from PromQL grouping
  // clauses (by/without/on/ignoring/group_*). Used in native
  // histogram mode where the `le` label does not exist — the bucket
  // boundaries live inside the native histogram value, not on a
  // dedicated label. Handles the four positional variants we see
  // in the dashboard:
  //   `(le, X, Y)` → `(X, Y)`     `(X, Y, le)` → `(X, Y)`
  //   `(X, le, Y)` → `(X, Y)`     `(le)`       → `()`
  // followed by a final cleanup that drops the now-empty `by ()`
  // suffix entirely so the rendered expressions stay legible.
  dropLeFromGrouping(expr)::
    local s1 = std.strReplace(expr, '(le, ', '(');
    local s2 = std.strReplace(s1, ', le)', ')');
    local s3 = std.strReplace(s2, ', le,', ',');
    local s4 = std.strReplace(s3, '(le)', '()');
    // Strip the empty grouping clauses that result from `by (le)`
    // becoming `by ()`. The four keywords PromQL accepts are
    // handled the same way.
    local s5 = std.strReplace(s4, ' by ()', '');
    local s6 = std.strReplace(s5, ' without ()', '');
    local s7 = std.strReplace(s6, ' on ()', '');
    local s8 = std.strReplace(s7, ' ignoring ()', '');
    s8,

  // rewriteExpr walks a PromQL expression and rewrites:
  //   - bare identifiers that look like metric names (raft.fsm.logs_…)
  //   - label names inside { … } selectors (service.cluster=…)
  //   - label names inside `by (…)` / `on (…)` / `without (…)` /
  //     `ignoring (…)` clauses (idem)
  //   - quoted metric names {"raft.fsm.logs_appended", …}
  //     (the Prometheus 3.x quoted-name syntax used for OTel metrics
  //     whose names contain dots)
  //   - quoted label names inside selectors {"service.cluster"=~…}
  //   - the legacy __name__="metric.name" selector form
  //
  // Label *values* (right-hand side of =, =~, !=, !~) are left
  // untouched: they reference user data or template variables.
  // Numeric literals (`0.50`, `1e-9`) are recognised as such and not
  // collected as identifiers — see isIdentStart.
  rewriteExpr(rawExpr, mode)::
    // Helpers like queries.histogramAvg leave sentinels in the
    // source expressions; expand them first so the walker sees
    // ordinary PromQL.
    local expr = $.preExpandHelpers(rawExpr, mode);
    if mode == 'otel' then expr
    else
      local n = std.length(expr);
      // State carried by the walker:
      //   acc             accumulated output
      //   inSelector      true while inside a { ... } selector
      //   expectingValue  true after a comparator: the next token is
      //                   a label value, not a name
      //   lastLabelName   most recent label name observed inside a
      //                   { … } selector (used to transform the
      //                   value of `__name__` selectors as a
      //                   metric name)
      //   lastIdent       most recent bare identifier — peeked at on
      //                   `(` to decide whether the parens introduce
      //                   a label-list context
      //   parens          stack of paren kinds (`L` = label list,
      //                   `N` = normal). Used to detect we're inside
      //                   `by (…)` / `on (…)` / … so the contained
      //                   identifiers get the label-name treatment.
      local step(i, acc, inSelector, expectingValue, lastLabelName, lastIdent, parens) =
        if i >= n then acc
        else
          local c = expr[i];
          if c == '"' || c == "'" then
            local sc = $.collectString(expr, i);
            local end = sc[0];
            local body = sc[1];
            local rewrittenBody =
              if !inSelector then
                body
              else if expectingValue then
                if lastLabelName == '__name__' then
                  naming.transformMetric(body, mode)
                else body
              else if $.followedByComparator(expr, end) then
                naming.transformLabel(body, mode)
              else
                naming.transformMetric(body, mode);
            local nextLast =
              if inSelector && !expectingValue && $.followedByComparator(expr, end) then body
              else lastLabelName;
            step(end, acc + c + rewrittenBody + c, inSelector, expectingValue, nextLast, lastIdent, parens)
          else if c == '{' then step(i + 1, acc + c, true, false, '', '', parens)
          else if c == '}' then step(i + 1, acc + c, false, false, '', '', parens)
          else if c == '(' then
            local kind = if std.objectHas($.labelListKeywords, lastIdent) then 'L' else 'N';
            step(i + 1, acc + c, inSelector, expectingValue, lastLabelName, '', parens + kind)
          else if c == ')' then
            local popped = if std.length(parens) > 0 then std.substr(parens, 0, std.length(parens) - 1) else parens;
            step(i + 1, acc + c, inSelector, expectingValue, lastLabelName, '', popped)
          else if c == ',' then step(i + 1, acc + c, inSelector, false, '', '', parens)
          else if c == '=' || c == '!' then
            step(i + 1, acc + c, inSelector, true, lastLabelName, lastIdent, parens)
          else if $.isIdentStart(c) then
            local ic = $.collectIdent(expr, i);
            local end = ic[0];
            local ident = ic[1];
            local isLabelInSelector =
              inSelector && !expectingValue && $.followedByComparator(expr, end);
            local isLabelInGrouping = $.inLabelList(parens);
            local rewritten =
              if isLabelInSelector || isLabelInGrouping then
                naming.transformLabel(ident, mode)
              else if !inSelector && $.shouldRewriteIdent(ident) then
                $.rewriteIdent(ident, mode)
              else if inSelector && expectingValue then
                ident
              else
                if $.shouldRewriteIdent(ident) then $.rewriteIdent(ident, mode) else ident;
            local nextLast = if isLabelInSelector then ident else lastLabelName;
            step(end, acc + rewritten, inSelector, expectingValue, nextLast, ident, parens)
          else step(i + 1, acc + c, inSelector, expectingValue, lastLabelName, lastIdent, parens);
      local walked = step(0, '', false, false, '', '', '');
      if naming.modeConfig(mode).native then $.dropLeFromGrouping(walked)
      else walked,

  // walkValue recursively traverses any JSON value and applies the
  // appropriate rewrite to nodes that carry PromQL expressions or
  // metric/label name references.
  // rewriteRegex transforms a Grafana templating regex field.
  // Grafana exports use `\.` to match a literal dot in a label name
  // (e.g. /service\.cluster="([^"]+)"/). After the collector
  // sanitises the labels there is no dot to match anymore, so we
  // collapse every escaped-dot into the underscore the collector
  // emitted. We deliberately do not touch other parts of the regex.
  rewriteRegex(r, mode)::
    if mode == 'otel' then r
    else std.strReplace(r, '\\.', '_'),

  // rewriteLegendFormat transforms a Grafana legendFormat string.
  // Grafana resolves `{{label.name}}` placeholders against the
  // labels of the returned time series; if our query's labels have
  // been de-dotted by the collector (`service.node_id` becomes
  // `service_node_id`) then the legend template must reference the
  // de-dotted form, otherwise Grafana silently leaves the
  // placeholder empty and every legend entry reads "Node".
  rewriteLegendFormat(s, mode)::
    if mode == 'otel' then s
    else
      local n = std.length(s);
      local step(i, acc) =
        if i >= n then acc
        else if i + 1 < n && s[i] == '{' && s[i + 1] == '{' then
          // collect until closing }}
          local end = (
            local find(j) =
              if j + 1 >= n then n
              else if s[j] == '}' && s[j + 1] == '}' then j
              else find(j + 1);
            find(i + 2)
          );
          local inner = std.substr(s, i + 2, end - (i + 2));
          local trimmed = std.stripChars(inner, ' \t');
          local transformed = naming.transformLabel(trimmed, mode);
          step(end + 2, acc + '{{' + transformed + '}}')
        else step(i + 1, acc + s[i]);
      step(0, ''),

  walkValue(v, mode, path=''):: (
    if std.isObject(v) then
      // expr/query/regex fields carry PromQL or label-name patterns.
      // Each is guarded with std.isString because some dashboard
      // exports use numeric or object values for the same key
      // (templating variable types, panel option values, …).
      {
        [k]: (
          if k == 'expr' && std.isString(v[k]) then
            $.rewriteExpr(v[k], mode)
          else if k == 'query' && std.isString(v[k]) then
            $.rewriteExpr(v[k], mode)
          else if k == 'definition' && std.isString(v[k]) then
            $.rewriteExpr(v[k], mode)
          else if k == 'regex' && std.isString(v[k]) then
            $.rewriteRegex(v[k], mode)
          else if k == 'legendFormat' && std.isString(v[k]) then
            $.rewriteLegendFormat(v[k], mode)
          else $.walkValue(v[k], mode, path + '.' + k)
        )
        for k in std.objectFields(v)
      }
    else if std.isArray(v) then
      [$.walkValue(item, mode, path + '[]') for item in v]
    else v
  ),

  // uidSuffix returns the short form of a mode name used to build
  // the Grafana dashboard UID. Grafana caps UIDs at 40 characters
  // and the full mode names (`prom-noprefix-normalized-native`)
  // overshoot when concatenated with the `ledger-metrics-` prefix,
  // so each mode component is compressed:
  //   prom         → prom
  //   noprefix     → np
  //   normalized   → norm
  //   native       → native
  // and the result is at most `ledger-metrics-prom-np-norm-native`
  // (33 chars).
  uidSuffix(mode)::
    local replacements = [
      ['noprefix', 'np'],
      ['normalized', 'norm'],
    ];
    std.foldl(
      function(acc, r) std.strReplace(acc, r[0], r[1]),
      replacements,
      mode,
    ),

  // dashboard renders the final dashboard JSON for a given mode by
  // walking the source tree.
  dashboard(source, mode)::
    $.walkValue(source, mode) + {
      // Distinguish the variants in Grafana so the operator can
      // import the one that matches their collector.
      title: std.get(source, 'title', 'Ledger Metrics') + ' (' + std.asciiUpper(mode) + ')',
      uid: std.get(source, 'uid', 'ledger-metrics') + '-' + $.uidSuffix(mode),
    },
}
