// SVG path-driven dot animation. Dots literally ride the static <path d=…>
// rendered by the Diagram component — they read getPointAtLength so they can't
// drift away from the arcs. Imperative because RAF + setAttribute is the
// cheapest way to push pixel updates at 60fps without going through reactivity.

let dotsRoot = null;
export function setDotsRoot(el) { dotsRoot = el; }

export function makeDot(color, r = 6) {
  const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  c.setAttribute("r", r);
  c.setAttribute("cx", -100);
  c.setAttribute("cy", -100);
  c.setAttribute("fill", color);
  c.setAttribute("filter", "drop-shadow(0 0 6px " + color + "88)");
  if (dotsRoot) dotsRoot.appendChild(c);
  return c;
}

function getPathHandle(arg) {
  if (typeof arg === "string") return { path: document.getElementById(arg), reverse: false };
  return { path: document.getElementById(arg.id), reverse: !!arg.reverse };
}

// Walks `dot` along one or more SVG paths over `duration` ms total. Each path
// segment gets an equal slice of time. Returns a Promise that resolves when
// done; the dot is removed at the end.
export function anim(dot, paths, duration) {
  const segments = Array.isArray(paths) ? paths.map(getPathHandle) : [getPathHandle(paths)];
  const each = duration / segments.length;
  return new Promise(resolve => {
    let i = 0;
    function runSegment() {
      if (i >= segments.length) {
        dot.parentNode?.removeChild(dot);
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

// Removes every dot currently parented to the dotsRoot. Used by Restart and by
// applyCheckpoint to wipe leftovers before re-placing rest dots.
export function clearAllDots() {
  if (!dotsRoot) return;
  while (dotsRoot.firstChild) dotsRoot.removeChild(dotsRoot.firstChild);
}
