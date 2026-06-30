package plan

import "errors"

// ErrCacheHorizonExceeded is returned by Builder.Build when admission predicts
// 2+ cache generation rotations will fire on the FSM side between this
// proposal's propose-time and its apply-time (cache.CheckCache → CacheUnreachable).
//
// In that regime, any preload value resolved now would be discarded by the
// rotations before the FSM apply reads it: a preload landed in gen0 is rotated
// into gen1 by the first rotation and dropped by the second. The proposal's
// reads would then miss the preloaded data, producing inconsistent results.
//
// Admission rejects the proposal so it never enters Raft. The client retries
// — by then admission's snapshot is fresh again and CheckCache returns a usable
// status. The audit log is *not* an issue here: the order never landed, so no
// audit entry is owed (this is a system-level rejection, not a user-domain
// business outcome).
//
// Under a correctly tuned rotation-threshold and a healthy FSM apply rate, this
// error should not fire. Recurring occurrences indicate either a too-low
// rotation threshold or FSM apply falling behind admission (overload).
//
// Mapped to gRPC codes.Unavailable in adapter/grpc/server.go so existing client
// retry interceptors handle it transparently.
var ErrCacheHorizonExceeded = errors.New("admission cache horizon exceeded: 2+ cache rotations predicted between propose and apply")
