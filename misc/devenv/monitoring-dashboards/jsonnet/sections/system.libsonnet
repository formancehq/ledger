// System section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

panels.row('System', 0, [
  panels.timeseries(
    'Logs per Second',
    { h: 8, w: 12, x: 0, y: 1 },
    [
      { expr: 'sum(rate(raft.fsm.logs_appended{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (formance.ledger.node.id)', legendFormat: 'Node {{formance.ledger.node.id}}' },
    ], unit='ops',
    description=|||
      Number of logs appended to the store per second per node. This metric represents the actual throughput of the system - how many logs (transactions, metadata changes, etc.) are being committed.
      
      Higher values indicate better performance. A sudden drop may indicate:
      - Leader election in progress
      - Storage backpressure (check Pebble write stalls)
      - Network issues between nodes
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#fsm-metrics
   |||,
  ),

  panels.timeseries(
    'Ping latency',
    { h: 8, w: 12, x: 12, y: 1 },
    [
      { expr: queries.histogramAvg('raft.transport.ping.duration', by=['formance.ledger.node.id', 'peer']), legendFormat: 'Node {{formance.ledger.node.id}} / Peer {{peer}}' },
    ], unit='s',
    description=|||
      Round-trip time (RTT) latency of ping requests between nodes. Measures network health between cluster members.
      
      High latency (>10ms) may cause:
      - Slower consensus
      - Leadership instability
      - Increased transaction latency
      
      Check network configuration if latency is consistently high.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#global-transport-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'HTTP requests count',
    { h: 8, w: 12, x: 0, y: 93 },
    [
      { expr: queries.histogramCountRate('http.server.request.duration', by=['formance.ledger.node.id', 'http.response.status_code']), legendFormat: 'Node {{formance.ledger.node.id}} : {{http.response.status_code}}' },
    ], unit='ops',
    description=|||
      HTTP request rate per node, grouped by status code. Shows API traffic and error rates.
      
      Monitor for:
      - 2xx: Successful requests
      - 4xx: Client errors (bad requests, not found)
      - 5xx: Server errors (investigate immediately)
      
      Sudden spikes in 5xx errors may indicate system issues.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#http-server-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Memory utilization',
    { h: 8, w: 12, x: 12, y: 93 },
    [
      { expr: '{"system.memory.utilization", "system.memory.state"="used", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} : Used' },
      { expr: '{"system.memory.utilization", "system.memory.state"="free", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} : Free' },
    ], unit='percentunit',
    description=|||
      System memory utilization ratio (0-1) showing used vs free memory.
      
      High memory utilization (>0.9) may cause:
      - OOM kills by Kubernetes
      - Performance degradation due to swapping
      - Increased GC pressure
      
      Consider increasing memory limits or scaling horizontally.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#system-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Process CPU time',
    { h: 8, w: 12, x: 0, y: 101 },
    [
      { expr: 'sum by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id) (
  rate(process.cpu.time{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])
)
/
max by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id) (
  go.processor.limit{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}
)', legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='percentunit',
    description=|||
      CPU utilization: process CPU seconds per second divided by the Go processor limit (GOMAXPROCS), per node. 1.0 means every available processor is busy.
      
      Values close to 1.0 indicate CPU saturation. Consider:
      - Profiling to identify hot paths
      - Increasing CPU limits
      - Scaling horizontally
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#process-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto', max: 1 },
  ),

  panels.timeseries(
    'System network traffic',
    { h: 8, w: 12, x: 12, y: 101 },
    [
      { expr: 'rate(system.network.io{network.io.direction="receive", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}}: Reception' },
      { expr: 'rate(system.network.io{network.io.direction="transmit", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}}: Transmission' },
    ], unit='binBps',
    description=|||
      Network I/O throughput showing bytes received and transmitted per second.
      
      High network traffic is expected during:
      - Log replication (leader to followers)
      - Snapshot transfers
      - Client request processing
      
      Sudden drops may indicate network partitions.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#process-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'System memory usage',
    { h: 8, w: 8, x: 0, y: 109 },
    [
      { expr: '{"system.memory.usage", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}: {{system.memory.state}}' },
    ], unit='bytes',
    description=|||
      Absolute system memory usage in bytes, broken down by state (used, free, cached, buffered).
      
      Provides visibility into how memory is allocated at the OS level. Useful for capacity planning and troubleshooting memory pressure.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#process-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Go memory allocated',
    { h: 8, w: 8, x: 8, y: 109 },
    [
      { expr: 'rate(go.memory.allocated{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}}: Allocated' },
    ], unit='binBps',
    description=|||
      Rate of memory allocation by the Go runtime (bytes per second). High allocation rates cause increased GC pressure.
      
      Consistently high allocation rates may indicate:
      - Inefficient code paths creating many short-lived objects
      - Need for object pooling
      - Memory leaks if trend is upward
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#go-runtime-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Leadership status',
    { h: 8, w: 8, x: 16, y: 109 },
    [
      { expr: '{"raft.node.leader", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} → leader {{leader_id}}' },
    ],
    description=|||
      Shows which node is recognized as the Raft leader by each node: each series is 1 for the leader_id a node currently recognizes. All nodes should report the same leader_id.

      A value of 0 (no leader_id) means the node knows no leader (cluster is electing). Monitor for:
      - Frequent leader changes (leadership instability)
      - Split-brain scenarios (different nodes reporting different leaders)
      - Extended periods with no leader
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#node-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Goroutine count',
    { h: 8, w: 8, x: 0, y: 117 },
    [
      { expr: 'go.goroutine.count{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}' },
    ],
    description=|||
      Number of active goroutines in the Go runtime. A steadily increasing count may indicate goroutine leaks.
      
      Normal operation should show a stable count with occasional spikes during high load. Investigate if:
      - Count grows unbounded over time
      - Count is significantly higher than expected
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#go-runtime-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Go memory used',
    { h: 8, w: 8, x: 8, y: 117 },
    [
      { expr: '{"go.memory.used", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}: {{go.memory.type}}' },
    ], unit='bytes',
    description=|||
      Memory currently in use by the Go runtime, broken down by type (stack, heap).
      
      Stack: Memory used by goroutine stacks
      Heap: Memory used by heap-allocated objects
      
      High heap usage may trigger more frequent GC cycles.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#go-runtime-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Go memory allocations',
    { h: 8, w: 8, x: 16, y: 117 },
    [
      { expr: 'rate(go.memory.allocations{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}}: Allocations' },
    ], unit='ops',
    description=|||
      Rate of memory allocations (objects per second) by the Go runtime.
      
      High allocation rates increase GC overhead. Combined with 'Go memory allocated', this helps identify whether you're allocating many small objects or fewer large objects.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#go-runtime-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Go GC goal',
    { h: 8, w: 12, x: 0, y: 125 },
    [
      { expr: 'go.memory.gc.goal{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}: Goal' },
    ], unit='bytes',
    description=|||
      Target heap size for the next GC cycle, set by the Go runtime's pacer.
      
      The GC goal grows as your application uses more memory. If it grows unbounded, you may have a memory leak. The GOGC environment variable controls how aggressively the GC runs.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#go-runtime-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),
])
