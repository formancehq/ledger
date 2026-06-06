<script>
  import { onMount } from "svelte";
  import { app } from "../lib/state.svelte.js";
  import { setDotsRoot } from "../lib/anim.js";
  import { BOXES } from "../lib/layout.js";
  import Box from "./Box.svelte";

  // The current step's highlighted box ids (set by App via the runCycle bus).
  let { highlights = [] } = $props();

  let dotsEl = $state(null);

  onMount(() => { setDotsRoot(dotsEl); });

  // Flash an SVG text element when its content changes — same retrigger trick
  // as the original (remove → forced reflow → re-add).
  function flash(el) {
    if (!el) return;
    el.classList.remove("idx-flash");
    void el.offsetWidth;
    el.classList.add("idx-flash");
  }
  let leaderIdxEl   = $state(null);
  let leaderApplEl  = $state(null);
  let f1MatchEl     = $state(null);
  let f2MatchEl     = $state(null);
  let f1ApplEl      = $state(null);
  let f2ApplEl      = $state(null);
  let walBoundsEl   = $state(null);
  $effect(() => { app.raft.leaderIdx;     flash(leaderIdxEl);   });
  $effect(() => { app.raft.leaderApplied; flash(leaderApplEl);  });
  $effect(() => { app.raft.f1Match;       flash(f1MatchEl);     });
  $effect(() => { app.raft.f2Match;       flash(f2MatchEl);     });
  $effect(() => { app.raft.f1Applied;     flash(f1ApplEl);      });
  $effect(() => { app.raft.f2Applied;     flash(f2ApplEl);      });
  $effect(() => { app.wal.leaderFirst; app.wal.leaderLast; flash(walBoundsEl); });
</script>

<svg viewBox="0 0 1200 650" preserveAspectRatio="xMidYMid meet">
  <defs>
    <radialGradient id="dotGlow">
      <stop offset="0%"  stop-color="white" stop-opacity="0.9"/>
      <stop offset="60%" stop-color="white" stop-opacity="0.0"/>
    </radialGradient>
  </defs>

  <!-- Stage labels -->
  <text class="stage-label" x="80"   y="40">Client</text>
  <text class="stage-label" x="280"  y="40">Ingress</text>
  <text class="stage-label" x="510"  y="40">Consensus (Raft)</text>
  <text class="stage-label" x="835"  y="40">Apply (FSM → Cache + Pebble)</text>
  <text class="stage-label" x="1090" y="40">Workers</text>

  <!-- Client + Ingress (forward pipeline boxes) -->
  <Box box={BOXES.client} {highlights}/>
  <Box box={BOXES.grpc}   {highlights}/>
  <Box box={BOXES.ctrl}   {highlights}/>
  <Box box={BOXES.adm}    {highlights}/>

  <!-- Raft cluster (Leader at the BOTTOM) — followers + leader carry their
       own reactive {term/idx, matchIdx} labels, so they use the children
       snippet to draw on top of the base rect. -->
  {#each [
    { box: BOXES.followerF2, matchRef: "f2", title: "FOLLOWER" },
    { box: BOXES.followerF1, matchRef: "f1", title: "FOLLOWER" },
  ] as { box, matchRef, title } (box.id)}
    <Box {box} {highlights}>
      {#snippet children()}
        <text class="node-title" x={box.x + box.w / 2} y={box.y + 22} text-anchor="middle" fill="#82aaff">{title}</text>
        <text class="sublabel"   x={box.x + box.w / 2} y={box.y + 40} text-anchor="middle">append entries → ack</text>
        <circle cx={box.x + 18} cy={box.y + 64} r={4} fill="#82aaff"/>
        {#if matchRef === "f1"}
          <text bind:this={f1MatchEl} class="sublabel" x={box.x + 30} y={box.y + 68} text-anchor="start">match idx {app.raft.f1Match}</text>
        {:else}
          <text bind:this={f2MatchEl} class="sublabel" x={box.x + 30} y={box.y + 68} text-anchor="start">match idx {app.raft.f2Match}</text>
        {/if}
      {/snippet}
    </Box>
  {/each}

  <Box box={BOXES.leader} {highlights} klass="box box-leader">
    {#snippet children()}
      <text class="node-title" x={BOXES.leader.x + BOXES.leader.w / 2} y={BOXES.leader.y + 22} text-anchor="middle" fill="#ffcb6b">LEADER (raft)</text>
      <text class="sublabel"   x={BOXES.leader.x + BOXES.leader.w / 2} y={BOXES.leader.y + 40} text-anchor="middle">propose &amp; replicate</text>
      <circle cx={BOXES.leader.x + 18} cy={BOXES.leader.y + 68} r={5} fill="#ffcb6b" class="pulse"/>
      <text bind:this={leaderIdxEl} class="sublabel" x={BOXES.leader.x + 30} y={BOXES.leader.y + 72} text-anchor="start">term {app.raft.term} · idx {app.raft.leaderIdx}</text>
    {/snippet}
  </Box>

  <!-- Follower WAL boxes — small red rects under each follower Raft node. -->
  {#each [BOXES.walF2, BOXES.walF1] as walBox (walBox.id)}
    <Box box={walBox} {highlights} stroke="#ff6b6b">
      {#snippet children()}
        <text class="sublabel" x={walBox.x + walBox.w / 2} y={walBox.y + 14} text-anchor="middle" fill="#ff6b6b">WAL</text>
      {/snippet}
    </Box>
  {/each}

  <!-- Leader WAL — wider box with a [first..last] bounds badge. firstIndex
       is the log's truncation floor (1 until compaction lands), lastIndex
       advances at every step ② Ready-tick batch. Aligned horizontally with
       the leader box above. -->
  <Box box={BOXES.walL} {highlights} stroke="#ff6b6b">
    {#snippet children()}
      <text class="sublabel" x={BOXES.walL.x + BOXES.walL.w / 2} y={BOXES.walL.y + 16} text-anchor="middle" fill="#ff6b6b">WAL</text>
      <text bind:this={walBoundsEl} class="applied-idx-leader" x={BOXES.walL.x + BOXES.walL.w / 2} y={BOXES.walL.y + 33} text-anchor="middle" fill="#ff6b6b">{app.wal.leaderLast < app.wal.leaderFirst ? "(empty)" : `[${app.wal.leaderFirst}..${app.wal.leaderLast}]`}</text>
    {/snippet}
  </Box>

  <!-- Background log compactor — truncates the WAL once entries are safely
       applied + snapshotted. Animations land in a follow-up; the box is
       drawn now so the consensus column reflects the final layout. -->
  <Box box={BOXES.compactor} {highlights} stroke="#ff6b6b"/>

  <!-- FSM (one per node) — each carries its own reactive `applied`/`lastPersistedIndex` label. -->
  <Box box={BOXES.fsmF2} {highlights}>
    {#snippet children()}
      <text class="label" x={BOXES.fsmF2.x + BOXES.fsmF2.w / 2} y={BOXES.fsmF2.y + 18} text-anchor="middle">FSM</text>
      <text bind:this={f2ApplEl} class="applied-idx-follower" x={BOXES.fsmF2.x + BOXES.fsmF2.w / 2} y={BOXES.fsmF2.y + 33} text-anchor="middle">applied {app.raft.f2Applied}</text>
    {/snippet}
  </Box>
  <Box box={BOXES.fsmF1} {highlights}>
    {#snippet children()}
      <text class="label" x={BOXES.fsmF1.x + BOXES.fsmF1.w / 2} y={BOXES.fsmF1.y + 18} text-anchor="middle">FSM</text>
      <text bind:this={f1ApplEl} class="applied-idx-follower" x={BOXES.fsmF1.x + BOXES.fsmF1.w / 2} y={BOXES.fsmF1.y + 33} text-anchor="middle">applied {app.raft.f1Applied}</text>
    {/snippet}
  </Box>
  <Box box={BOXES.fsmL} {highlights}>
    {#snippet children()}
      <text class="label" x={BOXES.fsmL.x + BOXES.fsmL.w / 2} y={BOXES.fsmL.y + 18} text-anchor="middle">FSM</text>
      <text bind:this={leaderApplEl} class="applied-idx-leader" x={BOXES.fsmL.x + BOXES.fsmL.w / 2} y={BOXES.fsmL.y + 34} text-anchor="middle">lastPersistedIndex = {app.raft.leaderApplied}</text>
    {/snippet}
  </Box>

  <!-- Cache + Pebble (in-memory + durable store). -->
  <Box box={BOXES.cache}  {highlights}/>
  <Box box={BOXES.pebble} {highlights}/>

  <!-- Notifier — narrow vertical box with rotated text. -->
  <Box box={BOXES.notifier} {highlights} stroke="#ffcb6b">
    {#snippet children()}
      <text class="node-title" x={BOXES.notifier.x + 14} y={BOXES.notifier.y + 70} text-anchor="middle" fill="#ffcb6b"
            transform="rotate(-90 {BOXES.notifier.x + 14} {BOXES.notifier.y + 70})">NOTIFIER</text>
      <text class="sublabel" x={BOXES.notifier.x + 14} y={BOXES.notifier.y + 175} text-anchor="middle"
            transform="rotate(-90 {BOXES.notifier.x + 14} {BOXES.notifier.y + 175})">signal.FanOut</text>
    {/snippet}
  </Box>

  <!-- Subscribers tail the log via the notifier broadcast. -->
  <Box box={BOXES.workerIndex}    {highlights}/>
  <Box box={BOXES.workerSinks}    {highlights}/>
  <Box box={BOXES.workerArchiver} {highlights}/>
  <Box box={BOXES.workerSealer}   {highlights}/>

  <!-- Static edges (every animated dot rides exactly one of these via getPointAtLength) -->
  <path id="e-client-grpc"      class="edge" d="M160,315 C190,315 200,230 220,230"/>
  <path id="e-grpc-ctrl"        class="edge" d="M300,260 L300,280"/>
  <path id="e-ctrl-adm"         class="edge" d="M300,340 L300,360"/>
  <path id="e-adm-leader"       class="edge" d="M380,390 C420,390 440,410 470,410"/>
  <path id="e-leader-f1"        class="edge" d="M580,380 L580,330"/>
  <path id="e-leader-f2"        class="edge" d="M580,380 C400,380 400,160 470,160"/>
  <!-- Buffered apply channel between each node's Ready loop and its own
       applier goroutine (chan applyWork, 1 in applier.go). Path split in
       two halves: *-fsm-1 = Ready loop → channel slot (midpoint), *-fsm-2
       = channel slot → FSM (applier picks up). The *-1 leg is where ⑤a
       parks the dot to visualise "pending message in channel". -->
  <path id="e-f2-fsm-1"         class="queue-edge" d="M690,150 L735,145"/>
  <path id="e-f2-fsm-2"         class="queue-edge" d="M735,145 L780,140"/>
  <path id="e-f1-fsm-1"         class="queue-edge" d="M690,270 L735,270"/>
  <path id="e-f1-fsm-2"         class="queue-edge" d="M735,270 L780,270"/>
  <path id="e-leader-fsm-1"     class="queue-edge" d="M690,400 L735,400.5"/>
  <path id="e-leader-fsm-2"     class="queue-edge" d="M735,400.5 L780,401"/>
  <path id="e-fsm-cache"        class="edge" d="M860,422 L817,438"/>
  <path id="e-fsm-pebble"       class="edge" d="M860,422 L902,438"/>
  <!-- FSM Leader → notifier — one signal.FanOut fires when batch commit returns -->
  <path id="e-fsm-notifier"        class="edge" d="M940,401 C950,401 950,315 958,315"/>
  <!-- Notifier → workers (4 subscribers; same broadcast event reaches each) -->
  <path id="e-notifier-w-index"    class="edge" d="M986,315 C993,315 993,145 1000,145"/>
  <path id="e-notifier-w-sinks"    class="edge" d="M986,315 C993,315 993,225 1000,225"/>
  <path id="e-notifier-w-archiver" class="edge" d="M986,315 C993,315 993,305 1000,305"/>
  <path id="e-notifier-w-sealer"   class="edge" d="M986,315 C993,315 993,405 1000,405"/>
  <path id="e-leader-adm"       class="edge" d="M470,440 C440,440 410,400 380,390" stroke="#3a4d72"/>

  <!-- Admission consultation rails — Cache departs from the right, Pebble from
       the left, so the longer Pebble arc nests around the shorter Cache arc.
       Both arcs thread the gap between the leader's WAL (top y=537) and the
       compactor (top y=590), aiming midpoints at y≈555 / y≈575. -->
  <path id="e-consult-cache"    class="consult-edge" d="M340,420 C340,590 720,590 817,480"/>
  <path id="e-consult-pebble"   class="consult-edge" d="M260,420 C260,617 800,617 902,480"/>

  <!-- WAL side-persistence rails (Raft bottom → separate WAL box) -->
  <path id="e-wal-f2"           class="edge" d="M650,200 L650,215" stroke="#ff6b6b" opacity="0.55"/>
  <path id="e-wal-f1"           class="edge" d="M650,330 L650,345" stroke="#ff6b6b" opacity="0.55"/>
  <path id="e-wal-leader"       class="edge" d="M580,480 L580,495" stroke="#ff6b6b" opacity="0.55"/>

  <!-- Compactor rails — wall-clock driven (5s). Reads lastAppliedIndex from
       Pebble, then issues wal.Truncate on the leader's WAL. -->
  <path id="e-compactor-pebble" class="edge" d="M690,611 C780,611 870,520 902,480" stroke="#ff6b6b" opacity="0.55"/>
  <path id="e-compactor-wal"    class="edge" d="M580,590 L580,537" stroke="#ff6b6b" opacity="0.55"/>

  <!-- Animated dots layer — populated imperatively by lib/anim.js -->
  <g bind:this={dotsEl} id="dots"></g>
</svg>
