// SVG path-driven dot animation. Dots literally ride the static <path d=…>
// rendered by the Diagram component — they read getPointAtLength so they can't
// drift away from the arcs. Imperative because RAF + setAttribute is the
// cheapest way to push pixel updates at 60fps without going through reactivity.

let dotsRoot = null;
export function setDotsRoot(el) { dotsRoot = el; }

export function makeDot(color, r = 6, txId = null) {
  const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  c.setAttribute("r", r);
  c.setAttribute("cx", -100);
  c.setAttribute("cy", -100);
  c.setAttribute("fill", color);
  c.setAttribute("filter", "drop-shadow(0 0 6px " + color + "88)");
  // Tag the dot so clearTxAnimDots(txId) can wipe everything this tx left
  // behind before its next action runs. Without the tag, dots from other
  // in-flight txs would be collateral damage.
  if (txId !== null && txId !== undefined) c.setAttribute("data-tx-id", String(txId));
  if (dotsRoot) dotsRoot.appendChild(c);
  return c;
}

function getPathHandle(arg) {
  if (typeof arg === "string") return { path: document.getElementById(arg), reverse: false };
  return { path: document.getElementById(arg.id), reverse: !!arg.reverse };
}

// Walks `dot` along one or more SVG paths over `duration` ms total. Each path
// segment gets an equal slice of time. Returns a Promise that resolves when
// done. The dot is LEFT at the end of the last segment — that's the "rest
// dot" by construction. The caller (runCycle) is responsible for calling
// clearTxAnimDots(txId) before the next action runs to wipe the leftovers.
export function anim(dot, paths, duration) {
  const segments = Array.isArray(paths) ? paths.map(getPathHandle) : [getPathHandle(paths)];
  const each = duration / segments.length;
  return new Promise(resolve => {
    let i = 0;
    function runSegment() {
      if (i >= segments.length) {
        // Don't remove the dot — leave it at the end of the last path so
        // the next step's rest position matches reality without a separate
        // STEP_REST table to maintain in sync.
        resolve();
        return;
      }
      const { path, reverse } = segments[i++];
      if (!path) { runSegment(); return; }
      const len = path.getTotalLength();
      const t0 = performance.now();
      function step(t) {
        const u = Math.min(1, (t - t0) / each);
        const at = reverse ? (1 - u) * len : u * len;
        const pt = path.getPointAtLength(at);
        dot.setAttribute("cx", pt.x);
        dot.setAttribute("cy", pt.y);
        if (u < 1) requestAnimationFrame(step);
        else runSegment();
      }
      requestAnimationFrame(step);
    }
    runSegment();
  });
}

// Per-tx convenience wrapper: create a tagged dot of the tx's color and
// walk it along the given path(s). Folds the makeDot + anim boilerplate
// that every step's action used to spell out by hand.
export function travel(tx, paths, duration = 700, r = 5) {
  return anim(makeDot(tx.color, r, tx.id), paths, duration);
}

// Removes every dot currently parented to the dotsRoot. Used by Restart.
export function clearAllDots() {
  if (!dotsRoot) return;
  while (dotsRoot.firstChild) dotsRoot.removeChild(dotsRoot.firstChild);
}

// Removes every dot tagged with the given txId. Called by runCycle right
// before each action runs so the previous step's "anim leftover" dots are
// wiped before the new step's anims emit new ones. Other in-flight txs'
// dots stay because they're tagged with their own ids.
export function clearTxAnimDots(txId) {
  if (!dotsRoot) return;
  const sel = `[data-tx-id="${String(txId)}"]`;
  const nodes = dotsRoot.querySelectorAll(sel);
  nodes.forEach(n => n.remove());
}
