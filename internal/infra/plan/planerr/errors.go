// Package planerr holds the infrastructure-level sentinel errors that the
// plan pipeline surfaces. It lives in a leaf package so both the plan
// package (proposer / builder side) and the state package (FSM applier
// side) can reference the sentinels without introducing an import cycle:
// state <-- plan already exists, so state cannot import plan directly.
//
// Consumers are expected to reach these through the plan package
// re-exports (plan.ErrCacheHorizonExceeded) rather than importing
// planerr directly, keeping the public API surface on plan.
package planerr

import "errors"

// ErrCacheHorizonExceeded fires when admission's CheckCache verdict is
// CacheUnreachable (2+ cache generation rotations predicted between
// propose-time and apply-time). See plan.ErrCacheHorizonExceeded for the
// full rationale.
var ErrCacheHorizonExceeded = errors.New("admission cache horizon exceeded: 2+ cache rotations predicted between propose and apply")
