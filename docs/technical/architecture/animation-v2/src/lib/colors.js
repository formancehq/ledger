// Stage colour tokens — kept here so the step definitions stay framework-pure
// (no DOM reads) but the CSS variables remain the single source of truth.
// Anything reading these can also fall back to `var(--grpc)` etc. in CSS.
export const COLORS = {
  grpc:  "#c3e88d", // client traffic
  raft:  "#f78c6c", // Raft replication
  apply: "#c792ea", // FSM apply
  resp:  "#89ddff", // response
  index: "#addb67", // workers
};
