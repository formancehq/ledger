// Dashboard-level wrapper: assembles the top-level Grafana JSON
// (title, templating, time range, …) around the panel list.
//
// The structure mirrors the previous JSON export so existing
// bookmarks and operator tooling that references uid="ledger-metrics"
// continue to work after the migration.

{
  // The templating variable list. Two datasources (Prometheus +
  // Pyroscope) followed by three query variables (cluster, node,
  // version). Regex fields use the OTel dot-notation form;
  // transform.libsonnet rewrites them for the prom variant.
  templating(uidSuffix='')::
    {
      list: [
        {
          current: { selected: true, text: 'VictoriaMetrics', value: 'VictoriaMetrics' },
          includeAll: false,
          label: 'Datasource',
          multi: false,
          name: 'datasource',
          options: [],
          query: 'prometheus',
          refresh: 1,
          regex: '',
          type: 'datasource',
        },
        {
          current: { selected: true, text: 'Pyroscope', value: 'Pyroscope' },
          includeAll: false,
          label: 'Pyroscope',
          multi: false,
          name: 'pyroscope',
          options: [],
          query: 'grafana-pyroscope-datasource',
          refresh: 1,
          regex: '',
          type: 'datasource',
        },
        {
          allValue: '.*',
          datasource: { type: 'prometheus', uid: '${datasource}' },
          definition: 'query_result(raft.node.lead)',
          description: 'Select a Ledger cluster to filter metrics',
          includeAll: true,
          label: 'Cluster',
          name: 'cluster',
          options: [],
          query: {
            qryType: 1,
            query: 'query_result(raft.node.lead)',
            refId: 'PrometheusVariableQueryEditor-VariableQuery',
          },
          refresh: 1,
          regex: '/service\\.cluster="([^"]+)"/',
          sort: 1,
          type: 'query',
        },
        {
          allValue: '.*',
          datasource: { type: 'prometheus', uid: '${datasource}' },
          definition: 'query_result(raft.node.lead{service.cluster=~"$cluster"})',
          description: 'Select a node to filter metrics',
          includeAll: true,
          label: 'Node',
          name: 'node',
          options: [],
          query: {
            qryType: 1,
            query: 'query_result(raft.node.lead{service.cluster=~"$cluster"})',
            refId: 'PrometheusVariableQueryEditor-VariableQuery',
          },
          refresh: 1,
          regex: '/service\\.node_id="([^"]+)"/',
          sort: 1,
          type: 'query',
        },
        {
          allValue: '.*',
          datasource: { type: 'grafana-pyroscope-datasource', uid: '${pyroscope}' },
          definition: '',
          description: 'Select a version to filter Pyroscope profiles (for comparison)',
          includeAll: true,
          label: 'Version',
          name: 'version',
          options: [],
          query: {
            labelSelector: '{service_name="ledger"}',
            profileTypeId: 'process_cpu:cpu:nanoseconds:cpu:nanoseconds',
            refId: 'PyroscopeVariableQueryEditor-VariableQuery',
          },
          refresh: 1,
          regex: '/version="([^"]+)"/',
          sort: 1,
          type: 'query',
        },
      ],
    },

  // dashboard wraps a list of rows (each containing nested panels)
  // into a complete Grafana dashboard object.
  dashboard(title, uid, rows)::
    {
      annotations: {
        list: [{
          builtIn: 1,
          datasource: { type: 'grafana', uid: '-- Grafana --' },
          enable: true,
          hide: true,
          iconColor: 'rgba(0, 211, 255, 1)',
          name: 'Annotations & Alerts',
          type: 'dashboard',
        }],
      },
      editable: true,
      fiscalYearStartMonth: 0,
      graphTooltip: 0,
      links: [],
      panels: rows,
      preload: false,
      refresh: '5s',
      schemaVersion: 42,
      tags: ['ledger', 'metrics'],
      templating: $.templating(),
      time: { from: 'now-5m', to: 'now' },
      timepicker: {},
      timezone: 'browser',
      title: title,
      uid: uid,
      version: 1,
    },
}
