import { makeDot } from "./anim.js";
import { STEP_REST, blockPosition } from "./geometry.js";

// Both kinds of "parked" dots (rest after a step, blocked at a lock gate) share
// the same lifecycle: pin a circle at a fixed (x, y), attach a CSS class for
// the visual treatment, store a reference on the tx, clear it later.
//
// A slot can hold either a single dot (most steps) or an array of dots (fan-out
// steps like ②/③ where one tx is conceptually present on multiple boxes at
// once). placeAt accepts either a single anchor or an array; clearSlot handles
// both shapes uniformly so the rest of the codebase doesn't have to care.
function placeAt(tx, slot, at, radius, cssClass) {
  clearSlot(tx, slot);
  if (!at) return;
  const anchors = Array.isArray(at) ? at : [at];
  const dots = [];
  for (const a of anchors) {
    if (!a) continue;
    const dot = makeDot(tx.color, radius);
    dot.setAttribute("cx", a.x);
    dot.setAttribute("cy", a.y);
    dot.classList.add(cssClass);
    dots.push(dot);
  }
  tx[slot] = dots.length === 1 ? dots[0] : dots;
}
function clearSlot(tx, slot) {
  const held = tx[slot];
  if (!held) return;
  const dots = Array.isArray(held) ? held : [held];
  for (const dot of dots) {
    if (dot?.parentNode) dot.parentNode.removeChild(dot);
  }
  tx[slot] = null;
}

// Resting dot — visible while paused between steps. The position is the
// edge of the box the tx has just entered.
export function placeRestDot(tx, stepIndex) {
  placeAt(tx, "restDot", STEP_REST[stepIndex], 5, "rest-dot");
}
export function clearRestDot(tx) { clearSlot(tx, "restDot"); }

// Blocked dot — pulses red at a lock gate. Stacking with a rest dot is wrong
// so we clear that first.
export function showBlockedDot(tx) {
  clearRestDot(tx);
  placeAt(tx, "blockedDot", blockPosition(tx.stepIndex), 6, "blocked-dot");
}
export function clearBlockedDot(tx) { clearSlot(tx, "blockedDot"); }

// Drop everything a tx owns in the SVG layer.
export function clearTxDots(tx) {
  clearRestDot(tx);
  clearBlockedDot(tx);
}
