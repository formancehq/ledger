// Transport & Queues section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Transport & Queues', 1, [
  panels.timeseries(
    'Reception channel incoming messages',
    { h: 6, w: 24, x: 0, y: 2 },
    [
      { expr: 'rate(raft.transport.recv.load_count{service.cluster=~"$cluster", service.node_id=~"$node", scope.name="raft.transport"}[30s])', legendFormat: 'Node {{service.node_id}}: p{{scope.attributes.priority}}' },
    ], unit='short',
    description=|||
      Rate of Raft messages received from other nodes, grouped by message type.
      
      Message types include:
      - MsgApp: Log replication (AppendEntries)
      - MsgAppResp: Response to AppendEntries
      - MsgHeartbeat/MsgHeartbeatResp: Leader heartbeats
      - MsgVote/MsgVoteResp: Leader election votes
      
      High rates indicate active replication. Zero rates may indicate network issues.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#reception-channel-metrics
   |||,
  ),

  panels.heatmap(
    'Reception channel load (High Priority: Heartbeats)',
    { h: 10, w: 8, x: 0, y: 92 },
    'sum(
  rate(raft.transport.recv.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node", scope.attributes.priority=~"1"}[$__rate_interval])
) by (service.node_id, le)
',
    description=|||
      Heatmap showing queue depth distribution for high-priority received messages (priority 1).
      
      High-priority messages include:
      - AppendEntries responses (AppResp)
      - Vote requests/responses (Vote, VoteResp)
      - Pre-vote requests (PreVote, PreVoteResp)
      
      Consistently high queue depth indicates the node cannot process messages fast enough. This may delay leader election or log replication acknowledgments.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#reception-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Reception channel load (Medium Priority: Votes/Responses)',
    { h: 10, w: 8, x: 8, y: 92 },
    'sum(
  rate(raft.transport.recv.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node", scope.attributes.priority=~"1"}[$__rate_interval])
) by (service.node_id, le)',
    description=|||
      Heatmap showing queue depth distribution for medium-priority received messages (priority 1).
      
      Medium-priority messages include: MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgAppResp.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#reception-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Reception channel load (Low Priority: Data)',
    { h: 10, w: 8, x: 16, y: 92 },
    'sum(
  rate(raft.transport.recv.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node", scope.attributes.priority=~"2"}[$__rate_interval])
) by (service.node_id, le)
',
    description=|||
      Heatmap showing queue depth distribution for lower-priority received messages (priority 2).
      
      Lower-priority messages include AppendEntries requests (App) which carry log entries for replication.
      
      High queue depth on followers is normal during heavy write load. On the leader, it should be minimal.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#reception-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.gauge(
    'Reception channel full count',
    { h: 4, w: 24, x: 0, y: 102 },
    'sum(increase(raft.transport.recv.full{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, scope.attributes.priority_name)', unit='short',
    description=|||
      Total number of times the reception channel was full and messages were dropped.
      
      ALERT: Any non-zero value requires investigation!
      
      Dropped messages cause:
      - Delayed log replication
      - Slower consensus
      - Potential leader election timeouts
      
      Increase queue capacity or investigate why processing is slow.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#reception-channel-metrics
   |||, opts={ legendFormat: '{{scope.attributes.priority_name}}' },
  ),

  panels.timeseries(
    'Transport Unreachable Channel - Incoming Messages',
    { h: 10, w: 8, x: 0, y: 106 },
    [
      { expr: 'rate(raft.transport.unreachable.load_count{service.cluster=~"$cluster", service.node_id=~"$node", scope.name="raft.transport"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Rate of 'unreachable' notifications received. These indicate that a peer node could not be reached.
      
      High rates suggest:
      - Network connectivity issues
      - Peer node is down or overloaded
      - DNS resolution problems
      
      Correlate with ping latency and leadership status.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#unreachable-channel-metrics
   |||,
  ),

  panels.heatmap(
    'Unreachable Channel Load',
    { h: 10, w: 8, x: 8, y: 106 },
    'sum(
  rate(raft.transport.unreachable.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])
) by (service.node_id, le)',
    description=|||
      Heatmap showing queue depth distribution for the unreachable notification queue.
      
      High values indicate many peers are becoming unreachable.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#unreachable-channel-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.gauge(
    'Unreachable channel full count',
    { h: 10, w: 8, x: 16, y: 106 },
    'sum(increase(raft.transport.unreachable.full{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', unit='short',
    description=|||
      Total number of times the unreachable notification channel was full.
      
      Non-zero values indicate the system cannot process unreachable notifications fast enough, which may delay failure detection.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#unreachable-channel-metrics
   |||, opts={ legendFormat: '{{scope.name}}' },
  ),

  panels.timeseries(
    'Pending Send Queue Throughput',
    { h: 10, w: 8, x: 0, y: 116 },
    [
      { expr: 'sum(rate(raft.send.pending_messages.load_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Throughput of the pending send queue. This is the rate at which message batches are being queued for dispatch to peers.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#pending-send-queue-metrics
   |||,
  ),

  panels.heatmap(
    'Pending Send Queue Load',
    { h: 10, w: 8, x: 8, y: 116 },
    'sum(
  rate(raft.send.pending_messages.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])
) by (service.node_id, le)',
    description=|||
      Heatmap showing queue depth distribution for the pending send queue.
      
      High values indicate messages are being queued faster than they can be dispatched.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#pending-send-queue-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.timeseries(
    'Pending Send Queue Full Count',
    { h: 10, w: 8, x: 16, y: 116 },
    [
      { expr: 'sum(increase(raft.send.pending_messages.full{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='short',
    description=|||
      Number of times the pending send queue was full. Alert if non-zero.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#pending-send-queue-metrics
   |||, opts={ drawStyle: 'bars', fillOpacity: 50 },
  ),

  panels.timeseries(
    'Send channel incoming messages',
    { h: 6, w: 12, x: 0, y: 126 },
    [
      { expr: 'sum(rate(raft.transport.peer.sending.load_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, scope.attributes.peer, type)', legendFormat: 'Node {{service.node_id}}: Peer {{scope.attributes.peer}}' },
    ], unit='short',
    description=|||
      Rate of Raft messages being queued for sending to each peer, grouped by message type.
      
      Shows outbound message flow to other cluster members:
      - MsgApp: Log replication to followers (leader only)
      - MsgHeartbeat: Heartbeats to maintain leadership
      - MsgVote: Vote requests during elections
      
      High rates on the leader indicate active replication.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#per-peer-sending-metrics
   |||,
  ),

  panels.timeseries(
    'Send channel full count',
    { h: 6, w: 12, x: 12, y: 126 },
    [
      { expr: 'sum(increase(raft.transport.peer.sending.full{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, scope.attributes.priority_name)', legendFormat: 'Node {{service.node_id}} : Peer {{peer}}: p{{scope.attributes.priority}}' },
    ], unit='ops',
    description=|||
      Rate of times the per-peer send channel was full and messages were dropped.
      
      ALERT: Non-zero rates indicate messages to peers are being dropped!
      
      This causes:
      - Delayed replication to affected peer
      - Potential follower lag
      - Need for snapshot transfer if too far behind
      
      Investigate network connectivity to the affected peer.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#per-peer-sending-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.heatmap(
    'Send channel load (High Priority: Heartbeats)',
    { h: 10, w: 8, x: 0, y: 132 },
    'sum(
  rate(raft.transport.peer.sending.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node", scope.attributes.priority=~"1"}[$__rate_interval])
) by (service.node_id, scope.attributes.peer, le)
',
    description=|||
      Heatmap showing per-peer send queue depth for high-priority messages (priority 1).
      
      High-priority outbound messages:
      - AppendEntries responses
      - Vote requests/responses
      - Pre-vote requests/responses
      
      These messages are critical for consensus. High queue depth may delay leader election or acknowledgment of replicated entries.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#per-peer-sending-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Send channel load (Medium Priority: Votes/Responses)',
    { h: 10, w: 8, x: 8, y: 132 },
    'sum(
  rate(raft.transport.peer.sending.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node", scope.attributes.priority=~"1"}[$__rate_interval])
) by (service.node_id, le)',
    description=|||
      Heatmap showing queue depth distribution for medium-priority outgoing messages (priority 1).
      
      Medium-priority messages include: MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgAppResp.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#per-peer-sending-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.heatmap(
    'Send channel load (Low Priority: Data)',
    { h: 10, w: 8, x: 16, y: 132 },
    'sum(
  rate(raft.transport.peer.sending.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node", scope.attributes.priority=~"2"}[$__rate_interval])
) by (service.node_id, scope.attributes.peer, le)
',
    description=|||
      Heatmap showing per-peer send queue depth for lower-priority messages (priority 2).
      
      Lower-priority outbound messages are AppendEntries requests carrying log entries for replication.
      
      High queue depth indicates the leader is producing entries faster than they can be sent to followers. This may be due to:
      - Network bandwidth limitations
      - Slow followers
      - High write throughput
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#per-peer-sending-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.timeseries(
    'Propose queue incoming messages',
    { h: 8, w: 8, x: 0, y: 142 },
    [
      { expr: 'rate(admission.propose_queue.load_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}: Incoming' },
    ], unit='ops',
    description=|||
      Rate of proposals (transactions) entering and leaving the propose queue.
      
      Incoming: Transactions submitted by clients
      Outgoing: Transactions processed by Raft
      
      If incoming >> outgoing, the queue is building up (backpressure). Check:
      - Leadership status (only leader processes proposals)
      - Apply entries latency
      - Storage write stalls
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#propose-queue-metrics
   |||,
  ),

  panels.heatmap(
    'Propose channel load',
    { h: 8, w: 8, x: 8, y: 142 },
    'sum(
  rate(admission.propose_queue.load_bucket[$__rate_interval])
) by (le)
',
    description=|||
      Heatmap showing propose queue depth distribution over time.
      
      The propose queue buffers transactions before they enter Raft consensus. Queue depth indicates backpressure:
      - Low values: System keeping up with load
      - High values: Transactions waiting to be processed
      
      Persistently high queue depth may require:
      - Increasing throughput capacity
      - Reducing client request rate
      - Investigating bottlenecks
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#propose-queue-metrics
   |||,
    opts={ legendFormat: '__auto' },
  ),

  panels.timeseries(
    'Propose queue full count',
    { h: 8, w: 8, x: 16, y: 142 },
    [
      { expr: 'sum(increase(admission.propose_queue.full{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='short',
    description=|||
      Total number of times the propose queue was full and proposals were dropped.
      
      CRITICAL ALERT: Any non-zero value means transactions are being rejected!
      
      Clients will receive errors when this happens. Immediate action required:
      - Scale horizontally
      - Increase queue capacity
      - Reduce client load
      - Investigate processing bottlenecks
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#propose-queue-metrics
   |||,
  ),

  panels.timeseries(
    'Pending responses',
    { h: 7, w: 12, x: 0, y: 150 },
    [
      { expr: '{"raft.transport.sending.pending_response", service.cluster=~"$cluster", service.node_id=~"$node"}', legendFormat: 'Node {{service.node_id}} / Peer : {{scope.attributes.peer}}' },
    ],
    description=|||
      Number of responses awaited from each peer node. Shows in-flight requests to other cluster members.
      
      High values indicate:
      - Slow peer nodes
      - Network congestion
      - Processing bottlenecks on remote nodes
      
      Persistently high values for a specific peer may indicate that peer is struggling.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#global-transport-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),
])
