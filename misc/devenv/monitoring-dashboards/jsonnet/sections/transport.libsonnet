// Transport & Queues section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

panels.row('Transport & Queues', 1, [
  panels.timeseries(
    'Reception channel incoming messages',
    { h: 6, w: 24, x: 0, y: 2 },
    [
      { expr: queries.histogramCountRate('raft.transport.recv.load', by=['k8s.namespace.name', 'formance.ledger.cluster.name', 'formance.ledger.node.id', 'priority', 'priority_name']), legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}: {{priority_name}}' },
    ], unit='short',
    description=|||
      Rate of Raft messages received from other nodes, grouped by message type.
      
      Message types include:
      - MsgApp: Log replication (AppendEntries)
      - MsgAppResp: Response to AppendEntries
      - MsgHeartbeat/MsgHeartbeatResp: Leader heartbeats
      - MsgVote/MsgVoteResp: Leader election votes
      
      High rates indicate active replication. Zero rates may indicate network issues.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#reception-channel-metrics
   |||,
  ),

  panels.heatmap(
    'Reception channel load (High Priority: Heartbeats)',
    { h: 10, w: 8, x: 0, y: 92 },
    'sum(rate(raft.transport.recv.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node", priority="0"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, le)',
    description=|||
      Heatmap showing queue depth distribution for high-priority received messages (priority 0).
      
      High-priority messages are the leader heartbeats and their responses (MsgHeartbeat, MsgHeartbeatResp).
      
      Consistently high queue depth indicates the node cannot process messages fast enough. Delayed heartbeats can make followers time out and trigger needless elections.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#reception-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Reception channel load (Medium Priority: Votes/Responses)',
    { h: 10, w: 8, x: 8, y: 92 },
    'sum(rate(raft.transport.recv.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node", priority="1"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, le)',
    description=|||
      Heatmap showing queue depth distribution for medium-priority received messages (priority 1).
      
      Medium-priority messages include: MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgAppResp.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#reception-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Reception channel load (Low Priority: Data)',
    { h: 10, w: 8, x: 16, y: 92 },
    'sum(rate(raft.transport.recv.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node", priority="2"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, le)',
    description=|||
      Heatmap showing queue depth distribution for lower-priority received messages (priority 2).
      
      Lower-priority messages include AppendEntries requests (App) which carry log entries for replication.
      
      High queue depth on followers is normal during heavy write load. On the leader, it should be minimal.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#reception-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.gauge(
    'Reception channel full count',
    { h: 4, w: 24, x: 0, y: 102 },
    'sum(increase(raft.transport.recv.overflows{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, priority_name)', unit='short',
    description=|||
      Total number of times the reception channel was full and messages were dropped.
      
      ALERT: Any non-zero value requires investigation!
      
      Dropped messages cause:
      - Delayed log replication
      - Slower consensus
      - Potential leader election timeouts
      
      Increase queue capacity or investigate why processing is slow.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#reception-channel-metrics
   |||, opts={ legendFormat: '{{priority_name}}' },
  ),

  panels.timeseries(
    'Transport Unreachable Channel - Incoming Messages',
    { h: 10, w: 8, x: 0, y: 106 },
    [
      { expr: queries.histogramCountRate('raft.transport.unreachable.load', by=['k8s.namespace.name', 'formance.ledger.cluster.name', 'formance.ledger.node.id']), legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='ops',
    description=|||
      Rate of 'unreachable' notifications received. These indicate that a peer node could not be reached.
      
      High rates suggest:
      - Network connectivity issues
      - Peer node is down or overloaded
      - DNS resolution problems
      
      Correlate with ping latency and leadership status.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#unreachable-channel-metrics
   |||,
  ),

  panels.heatmap(
    'Unreachable Channel Load',
    { h: 10, w: 8, x: 8, y: 106 },
    'sum(
  rate(raft.transport.unreachable.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])
) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, le)',
    description=|||
      Heatmap showing queue depth distribution for the unreachable notification queue.
      
      High values indicate many peers are becoming unreachable.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#unreachable-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.gauge(
    'Unreachable channel full count',
    { h: 10, w: 8, x: 16, y: 106 },
    'sum(increase(raft.transport.unreachable.overflows{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id)', unit='short',
    description=|||
      Total number of times the unreachable notification channel was full.
      
      Non-zero values indicate the system cannot process unreachable notifications fast enough, which may delay failure detection.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#unreachable-channel-metrics
   |||, opts={ legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
  ),

  panels.timeseries(
    'Pending Send Queue Throughput',
    { h: 10, w: 8, x: 0, y: 116 },
    [
      { expr: queries.histogramCountRate('raft.send.pending_batch.load', by=['k8s.namespace.name', 'formance.ledger.cluster.name', 'formance.ledger.node.id']), legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='ops',
    description=|||
      Throughput of the pending send queue. This is the rate at which message batches are being queued for dispatch to peers.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#pending-send-queue-metrics
   |||,
  ),

  panels.heatmap(
    'Pending Send Queue Load',
    { h: 10, w: 8, x: 8, y: 116 },
    'sum(
  rate(raft.send.pending_batch.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])
) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, le)',
    description=|||
      Heatmap showing queue depth distribution for the pending send queue.
      
      High values indicate messages are being queued faster than they can be dispatched.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#pending-send-queue-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.timeseries(
    'Pending Send Queue Full Count',
    { h: 10, w: 8, x: 16, y: 116 },
    [
      { expr: 'sum(increase(raft.send.pending_batch.overflows{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id)', legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='short',
    description=|||
      Number of times the pending send queue was full. Alert if non-zero.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#pending-send-queue-metrics
   |||, opts={ drawStyle: 'bars', fillOpacity: 50 },
  ),

  panels.timeseries(
    'Send channel incoming messages',
    { h: 6, w: 12, x: 0, y: 126 },
    [
      { expr: queries.histogramCountRate('raft.transport.peer.sending.load', by=['k8s.namespace.name', 'formance.ledger.cluster.name', 'formance.ledger.node.id', 'peer', 'priority_name']), legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}: Peer {{peer}} / {{priority_name}}' },
    ], unit='short',
    description=|||
      Rate of Raft messages being queued for sending to each peer, grouped by message type.
      
      Shows outbound message flow to other cluster members:
      - MsgApp: Log replication to followers (leader only)
      - MsgHeartbeat: Heartbeats to maintain leadership
      - MsgVote: Vote requests during elections
      
      High rates on the leader indicate active replication.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#per-peer-sending-metrics
   |||,
  ),

  panels.timeseries(
    'Send channel full count',
    { h: 6, w: 12, x: 12, y: 126 },
    [
      { expr: 'sum(increase(raft.transport.peer.sending.overflows{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, peer, priority_name)', legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}: Peer {{peer}} / {{priority_name}}' },
    ], unit='short',
    description=|||
      Number of batches dropped per interval because the per-peer send channel was full.
      
      ALERT: Non-zero counts indicate messages to peers are being dropped!
      
      This causes:
      - Delayed replication to affected peer
      - Potential follower lag
      - Need for snapshot transfer if too far behind
      
      Investigate network connectivity to the affected peer.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#per-peer-sending-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.heatmap(
    'Send channel load (High Priority: Heartbeats)',
    { h: 10, w: 8, x: 0, y: 132 },
    'sum(rate(raft.transport.peer.sending.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node", priority="0"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, peer, le)',
    description=|||
      Heatmap showing per-peer send queue depth for high-priority messages (priority 0).
      
      High-priority outbound messages are the leader heartbeats and their responses (MsgHeartbeat, MsgHeartbeatResp).
      
      These messages keep leadership alive. High queue depth may let followers time out and trigger needless elections.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#per-peer-sending-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Send channel load (Medium Priority: Votes/Responses)',
    { h: 10, w: 8, x: 8, y: 132 },
    'sum(rate(raft.transport.peer.sending.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node", priority="1"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, peer, le)',
    description=|||
      Heatmap showing queue depth distribution for medium-priority outgoing messages (priority 1).
      
      Medium-priority messages include: MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgAppResp.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#per-peer-sending-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Send channel load (Low Priority: Data)',
    { h: 10, w: 8, x: 16, y: 132 },
    'sum(rate(raft.transport.peer.sending.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node", priority="2"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, peer, le)',
    description=|||
      Heatmap showing per-peer send queue depth for lower-priority messages (priority 2).
      
      Lower-priority outbound messages are AppendEntries requests carrying log entries for replication.
      
      High queue depth indicates the leader is producing entries faster than they can be sent to followers. This may be due to:
      - Network bandwidth limitations
      - Slow followers
      - High write throughput
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#per-peer-sending-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.timeseries(
    'Propose queue incoming messages',
    { h: 8, w: 8, x: 0, y: 142 },
    [
      { expr: queries.histogramCountRate('admission.propose_queue.load', by=['k8s.namespace.name', 'formance.ledger.cluster.name', 'formance.ledger.node.id']), legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}: Incoming' },
    ], unit='ops',
    description=|||
      Rate of proposals (transactions) entering and leaving the propose queue.
      
      Incoming: Transactions submitted by clients
      Outgoing: Transactions processed by Raft
      
      If incoming >> outgoing, the queue is building up (backpressure). Check:
      - Leadership status (only leader processes proposals)
      - Apply entries latency
      - Storage write stalls
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#propose-queue-metrics
   |||,
  ),

  panels.heatmap(
    'Propose channel load',
    { h: 8, w: 8, x: 8, y: 142 },
    'sum(rate(admission.propose_queue.load_bucket{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id, le)',
    description=|||
      Heatmap showing propose queue depth distribution over time.
      
      The propose queue buffers transactions before they enter Raft consensus. Queue depth indicates backpressure:
      - Low values: System keeping up with load
      - High values: Transactions waiting to be processed
      
      Persistently high queue depth may require:
      - Increasing throughput capacity
      - Reducing client request rate
      - Investigating bottlenecks
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#propose-queue-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.timeseries(
    'Propose queue full count',
    { h: 8, w: 8, x: 16, y: 142 },
    [
      { expr: 'sum(increase(admission.propose_queue.overflows{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (k8s.namespace.name, formance.ledger.cluster.name, formance.ledger.node.id)', legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}}' },
    ], unit='short',
    description=|||
      Total number of times the propose queue was full and proposals were dropped.
      
      CRITICAL ALERT: Any non-zero value means transactions are being rejected!
      
      Clients will receive errors when this happens. Immediate action required:
      - Scale horizontally
      - Increase queue capacity
      - Reduce client load
      - Investigate processing bottlenecks
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#propose-queue-metrics
   |||,
  ),

  panels.timeseries(
    'Pending responses',
    { h: 7, w: 12, x: 0, y: 150 },
    [
      { expr: '{"raft.transport.sending.pending_response.count", k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: '{{formance.ledger.cluster.name}} / Node {{formance.ledger.node.id}} / Peer {{peer}}' },
    ],
    description=|||
      Number of responses awaited from each peer node. Shows in-flight requests to other cluster members.
      
      High values indicate:
      - Slow peer nodes
      - Network congestion
      - Processing bottlenecks on remote nodes
      
      Persistently high values for a specific peer may indicate that peer is struggling.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#global-transport-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),
])
