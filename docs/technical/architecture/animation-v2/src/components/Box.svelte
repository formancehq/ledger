<script>
  // Generic box renderer. Reads geometry + default labels off a BOXES entry
  // (see lib/layout.js). For simple boxes (title + sub) the defaults suffice;
  // complex boxes (Followers, Leader, FSMs with reactive idx labels) pass a
  // `children` snippet to render their custom contents in addition to or
  // instead of the auto title/sub.
  let {
    box,                                  // BOXES entry: { id, x, y, w, h, title?, sub?, stroke?, titleY?, subY? }
    highlights = [],                      // currently-lit box ids
    klass     = "box",                    // override for special variants ("box box-leader")
    stroke    = box?.stroke ?? null,      // override for colored strokes
    children  = undefined,                // optional snippet for extra SVG inside the <g>
  } = $props();

  const lit = $derived(highlights.includes(box.id));
  // Heuristic vertical positions for title/sub that match the original
  // hand-placed coords within a couple of pixels across all box heights:
  //   h ≤ 45  (Cache, Pebble)         → title y+18, sub y+33
  //   h ≤ 55  (Workers)               → title y+23, sub y+40
  //   h ≤ 65  (gRPC, Ctrl, Adm)       → title y+25, sub y+44
  //   else    (Client)                → title y+30, sub y+48
  // Override per-box via box.titleY / box.subY for non-conforming layouts.
  const titleY = $derived(box.y + (box.titleY ?? (box.h <= 45 ? 18 : box.h <= 55 ? 23 : box.h <= 65 ? 25 : 30)));
  const subY   = $derived(box.y + (box.subY   ?? (box.h <= 45 ? 33 : box.h <= 55 ? 40 : box.h <= 65 ? 44 : 48)));
  const cx     = $derived(box.x + box.w / 2);
</script>

<g id={box.id} class:highlight={lit}>
  <rect class={klass} x={box.x} y={box.y} width={box.w} height={box.h} {stroke}/>
  {#if box.title}
    <text class="label"    x={cx} y={titleY} text-anchor="middle" fill={box.titleFill ?? null}>{box.title}</text>
  {/if}
  {#if box.sub}
    <text class="sublabel" x={cx} y={subY} text-anchor="middle">{box.sub}</text>
  {/if}
  {@render children?.()}
</g>
