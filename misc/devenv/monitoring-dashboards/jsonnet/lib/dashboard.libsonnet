// Dashboard-level wrapper: assembles the top-level Grafana JSON
// (title, templating, time range, …) around the panel list.
//
// The structure mirrors the previous JSON export so existing
// bookmarks and operator tooling that references uid="ledger-metrics"
// continue to work after the migration.

{
  // The templating variable list. Two datasources (Prometheus +
  // Pyroscope) followed by three query variables (namespace, cluster
  // and node).
  // Regex fields use the OTel dot-notation form;
  // transform.libsonnet rewrites them for the prom variant.
  templating(uidSuffix='')::
    {
      list: [
        {
          // transform.dashboard replaces this with the datasource that
          // matches the generated histogram mode.
          current: { selected: true, text: 'Prometheus', value: 'Prometheus' },
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
          definition: 'query_result(raft.node.leader)',
          description: 'Select the Kubernetes namespace of the Ledger cluster',
          includeAll: true,
          label: 'Namespace',
          name: 'namespace',
          options: [],
          query: {
            qryType: 1,
            query: 'query_result(raft.node.leader)',
            refId: 'PrometheusVariableQueryEditor-VariableQuery',
          },
          refresh: 1,
          // Set by the operator. Clusters are identified by namespace and
          // cluster name together: a Cluster resource name is only unique
          // within its namespace. Without the operator the label is absent
          // and the All value (.*) still matches every series.
          regex: '/k8s\\.namespace\\.name="([^"]+)"/',
          sort: 1,
          type: 'query',
        },
        {
          allValue: '.*',
          datasource: { type: 'prometheus', uid: '${datasource}' },
          definition: 'query_result(raft.node.leader{k8s.namespace.name=~"$namespace"})',
          description: 'Select a Ledger cluster to filter metrics',
          includeAll: true,
          label: 'Cluster',
          name: 'cluster',
          options: [],
          query: {
            qryType: 1,
            query: 'query_result(raft.node.leader{k8s.namespace.name=~"$namespace"})',
            refId: 'PrometheusVariableQueryEditor-VariableQuery',
          },
          refresh: 1,
          // Key on the cluster name, not the declared cluster ID: IDs repeat
          // across clusters (EN-2031). The operator sets the name to the
          // Cluster resource name; the server defaults it to the cluster ID
          // otherwise, so every series carries it.
          regex: '/formance\\.ledger\\.cluster\\.name="([^"]+)"/',
          sort: 1,
          type: 'query',
        },
        {
          allValue: '.*',
          datasource: { type: 'prometheus', uid: '${datasource}' },
          definition: 'query_result(raft.node.leader{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster"})',
          description: 'Select a node to filter metrics',
          includeAll: true,
          label: 'Node',
          name: 'node',
          options: [],
          query: {
            qryType: 1,
            query: 'query_result(raft.node.leader{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster"})',
            refId: 'PrometheusVariableQueryEditor-VariableQuery',
          },
          refresh: 1,
          regex: '/formance\\.ledger\\.node\\.id="([^"]+)"/',
          // Raft node IDs are integers: sort numerically (1, 2, 10).
          sort: 3,
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
