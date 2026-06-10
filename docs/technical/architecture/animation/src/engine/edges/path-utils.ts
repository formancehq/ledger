import type { EdgePath, EdgeSegment } from "../types";

// EdgePath utilities shared by the scheduler and any consumer that
// needs to walk / invert segments. Kept here (and not in scheduler.ts)
// so neither side has to pull the other in.

// Forward + reverse round-trip path for a "call" edge. The dot walks
// the forward segments, then the same segments reversed in opposite
// order — landing back at the caller along the same SVG geometry.
export function roundTripVia(forward: EdgePath): EdgePath {
  const fwd = normalize(forward);
  const rev = [...fwd].reverse().map(flip);
  return [...fwd, ...rev];
}

// Coerce a possibly-single EdgePath into the array form, leaving
// existing arrays untouched.
export function normalize(p: EdgePath): EdgeSegment[] {
  return Array.isArray(p) ? p : [p];
}

// Flip a single segment's direction. String segments default to
// forward; object segments toggle their `reverse` flag.
export function flip(seg: EdgeSegment): EdgeSegment {
  if (typeof seg === "string") return { id: seg, reverse: true };
  return { id: seg.id, reverse: !seg.reverse };
}
