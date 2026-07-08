package wal

// Marker files persisted at the top of the WAL data directory. Each marker
// encodes a small piece of durable per-peer state whose lifetime is tied
// to a specific (pod, PVC) incarnation: it must survive restarts of the
// same process but be dropped when the PVC is reprovisioned.
//
// New markers go here. The read/write helpers live in the file named
// after the marker (see cluster_joined_marker.go, instance_id_marker.go).
const (
	// ClusterJoinedMarkerFile proves that this node has been accepted
	// by the cluster as a member. For pod-0 it is written by the
	// bootstrap path immediately after the initial ConfState snapshot
	// is persisted; for the other pods it is written by tryAddLearner
	// after the leader accepts the JoinAsLearner RPC. Presence-only —
	// the file's content is irrelevant. The operator's StatefulSet
	// entrypoint checks it in shell before launching the server, so
	// its name is public API for tooling.
	ClusterJoinedMarkerFile = "CLUSTER_JOINED"

	// InstanceIDMarkerFile holds this peer's InstanceIDLen-byte
	// instance UUID (EN-1045). Generated once at the peer's very
	// first boot, immutable for the lifetime of the WAL directory,
	// communicated to the leader via JoinAsLearner so the leader can
	// discriminate a still-alive removed pod from a fresh pod at the
	// same nodeID. See
	// docs/technical/architecture/subsystems/consensus/removed-member-registry.md.
	InstanceIDMarkerFile = "INSTANCE_ID"
)

// InstanceIDLen is the fixed byte length of the INSTANCE_ID payload.
const InstanceIDLen = 16
