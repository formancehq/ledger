// SVG path-driven dot animation. Dots literally ride the static <path d=…>
// rendered by the Diagram component — they read getPointAtLength so they
// can't drift away from the arcs. Imperative because RAF + setAttribute is
// the cheapest way to push pixel updates at 60fps without going through
// React reactivity.

// Optional batch tag baked onto an animated dot's DOM (data-batch-id +
// data-batch-kind) so a hover overlay can resolve the dot back to a
// batch context. Only the engine pipeline currently passes them; the
// engine itself doesn't need the type but keeping the parameter lets
// future code carry richer per-dot metadata without re-threading the
// signature.
type BatchTag = { kind: "propose" | "apply"; id: string };

export type PathHandle = string | { id: string; reverse?: boolean };

// Owns the <g id="dots"> SVG node Diagram renders into. One instance
// per engine handle (constructed in createEngine); React components
// reach it via `useEngine().dotsLayer` and call `make()` / `attach()`
// directly. No module-level singleton.
export class DotsLayer {
  private root: SVGGElement | null = null;

  attach(el: SVGGElement | null): void { this.root = el; }

  make(color: string, r: number, txId: string | number | null, batch: BatchTag | null = null): SVGCircleElement {
    const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    c.setAttribute("r", String(r));
    c.setAttribute("cx", "-100");
    c.setAttribute("cy", "-100");
    c.setAttribute("fill", color);
    c.setAttribute("filter", "drop-shadow(0 0 6px " + color + "88)");
    // Tag the dot so clearForTx(txId) can wipe everything this tx left
    // behind before its next action runs. Without the tag, dots from other
    // in-flight txs would be collateral damage.
    if (txId !== null && txId !== undefined) c.setAttribute("data-tx-id", String(txId));
    // Optional batch tag — set when the batch identity is known at dot
    // creation time. Dots that don't carry it can still trigger the hover
    // overlay if the tx is currently part of a batch (Diagram.tsx looks
    // up data-tx-id in txRegistry as a fallback).
    if (batch) {
      c.setAttribute("data-batch-kind", batch.kind);
      c.setAttribute("data-batch-id", batch.id);
    }
    // Pointer cursor on every dot — the registry-based fallback may match
    // any tx-tagged dot, so we don't know upfront whether the hover will
    // produce an overlay.
    if (txId !== null && txId !== undefined) c.style.cursor = "pointer";
    if (this.root) this.root.appendChild(c);
    return c;
  }

  clearAll(): void {
    if (!this.root) return;
    while (this.root.firstChild) this.root.removeChild(this.root.firstChild);
  }

  clearForTx(txId: string | number): void {
    if (!this.root) return;
    const nodes = this.root.querySelectorAll(`[data-tx-id="${String(txId)}"]`);
    nodes.forEach(n => n.remove());
  }
}

// No module-level singleton: each engine handle owns its own DotsLayer
// (instantiated inside createEngine). Consumers reach it via
// `useEngine().dotsLayer.{attach,make,clearAll,clearForTx}`. Tests can
// create a fresh DotsLayer per case.

function getPathHandle(arg: PathHandle): { path: SVGPathElement | null; reverse: boolean } {
  if (typeof arg === "string") return { path: document.getElementById(arg) as unknown as SVGPathElement | null, reverse: false };
  return { path: document.getElementById(arg.id) as unknown as SVGPathElement | null, reverse: !!arg.reverse };
}

// Walks `dot` along one or more SVG paths over `duration` ms total. Each
// segment gets an equal time slice. The dot is left at the end of the last
// segment — that's the "rest dot" by construction. The caller (runCycle)
// clears tagged dots before the next action runs to wipe leftovers.
export function anim(dot: SVGCircleElement, paths: PathHandle | PathHandle[], duration: number): Promise<void> {
  const segments = Array.isArray(paths) ? paths.map(getPathHandle) : [getPathHandle(paths)];
  const each = duration / segments.length;
  return new Promise<void>(resolve => {
    let i = 0;
    function runSegment() {
      if (i >= segments.length) {
        // Leave the dot at the last path's endpoint so the next step's
        // rest position matches reality without a STEP_REST table.
        resolve();
        return;
      }
      const { path, reverse } = segments[i++];
      if (!path) { runSegment(); return; }
      const segPath = path;
      const len = segPath.getTotalLength();
      const t0 = performance.now();
      function step(t: number) {
        const u = Math.min(1, (t - t0) / each);
        const at = reverse ? (1 - u) * len : u * len;
        const pt = segPath.getPointAtLength(at);
        dot.setAttribute("cx", String(pt.x));
        dot.setAttribute("cy", String(pt.y));
        if (u < 1) requestAnimationFrame(step);
        else runSegment();
      }
      requestAnimationFrame(step);
    }
    runSegment();
  });
}

// (previous `travel(tx, paths, …)` helper was unused after the engine
// rewrite; removed during the DI refactor since it relied on the
// module-level dotsLayer singleton.)
