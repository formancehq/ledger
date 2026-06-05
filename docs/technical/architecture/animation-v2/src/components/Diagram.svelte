<script>
  import { onMount } from "svelte";
  import { app } from "../lib/state.svelte.js";
  import { setDotsRoot } from "../lib/anim.js";

  // The current step's highlighted box ids (set by App via the runCycle bus).
  let { highlights = [] } = $props();

  let dotsEl = $state(null);

  onMount(() => { setDotsRoot(dotsEl); });

  // Helper: is this svg group highlighted right now?
  const isHi = (id) => highlights.includes(id);

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
  $effect(() => { app.raft.leaderIdx;     flash(leaderIdxEl);   });
  $effect(() => { app.raft.leaderApplied; flash(leaderApplEl);  });
  $effect(() => { app.raft.f1Match;       flash(f1MatchEl);     });
  $effect(() => { app.raft.f2Match;       flash(f2MatchEl);     });
  $effect(() => { app.raft.f1Applied;     flash(f1ApplEl);      });
  $effect(() => { app.raft.f2Applied;     flash(f2ApplEl);      });
</script>

<svg viewBox="0 0 1200 565" preserveAspectRatio="xMidYMid meet">
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

  <!-- Client -->
  <g id="box-client" class:highlight={isHi("box-client")}>
    <rect class="box" x="40" y="280" width="120" height="70"/>
    <text class="label"    x="100" y="310" text-anchor="middle">gRPC Client</text>
    <text class="sublabel" x="100" y="328" text-anchor="middle">Apply(CreateLog)</text>
  </g>

  <!-- Ingress (on Leader) -->
  <g id="box-grpc" class:highlight={isHi("box-grpc")}>
    <rect class="box" x="220" y="200" width="160" height="60"/>
    <text class="label"    x="300" y="225" text-anchor="middle">gRPC Server</text>
    <text class="sublabel" x="300" y="244" text-anchor="middle">BucketService :8888</text>
  </g>
  <g id="box-ctrl" class:highlight={isHi("box-ctrl")}>
    <rect class="box" x="220" y="280" width="160" height="60"/>
    <text class="label"    x="300" y="305" text-anchor="middle">Routed Controller</text>
    <text class="sublabel" x="300" y="324" text-anchor="middle">forward to leader</text>
  </g>
  <g id="box-adm" class:highlight={isHi("box-adm")}>
    <rect class="box" x="220" y="360" width="160" height="60"/>
    <text class="label"    x="300" y="385" text-anchor="middle">Admission</text>
    <text class="sublabel" x="300" y="404" text-anchor="middle">preload volumes</text>
  </g>

  <!-- Raft cluster (Leader at the BOTTOM) -->
  <g id="nodeF2" class:highlight={isHi("nodeF2")}>
    <rect class="box" x="470" y="120" width="220" height="80"/>
    <text class="node-title" x="580" y="142" text-anchor="middle" fill="#82aaff">FOLLOWER</text>
    <text class="sublabel"   x="580" y="160" text-anchor="middle">append entries → ack</text>
    <circle cx="488" cy="184" r="4" fill="#82aaff"/>
    <text bind:this={f2MatchEl} class="sublabel" x="500" y="188" text-anchor="start">match idx {app.raft.f2Match}</text>
  </g>
  <g id="walF2" class:highlight={isHi("walF2")}>
    <rect class="box" x="610" y="215" width="80" height="20" stroke="#ff6b6b" stroke-width="1.5"/>
    <text class="sublabel" x="650" y="229" text-anchor="middle" fill="#ff6b6b">WAL</text>
  </g>

  <g id="nodeF1" class:highlight={isHi("nodeF1")}>
    <rect class="box" x="470" y="250" width="220" height="80"/>
    <text class="node-title" x="580" y="272" text-anchor="middle" fill="#82aaff">FOLLOWER</text>
    <text class="sublabel"   x="580" y="290" text-anchor="middle">append entries → ack</text>
    <circle cx="488" cy="314" r="4" fill="#82aaff"/>
    <text bind:this={f1MatchEl} class="sublabel" x="500" y="318" text-anchor="start">match idx {app.raft.f1Match}</text>
  </g>
  <g id="walF1" class:highlight={isHi("walF1")}>
    <rect class="box" x="610" y="345" width="80" height="20" stroke="#ff6b6b" stroke-width="1.5"/>
    <text class="sublabel" x="650" y="359" text-anchor="middle" fill="#ff6b6b">WAL</text>
  </g>

  <g id="nodeL" class:highlight={isHi("nodeL")}>
    <rect class="box box-leader" x="470" y="380" width="220" height="100"/>
    <text class="node-title" x="580" y="402" text-anchor="middle" fill="#ffcb6b">LEADER (raft)</text>
    <text class="sublabel"   x="580" y="420" text-anchor="middle">propose &amp; replicate</text>
    <circle cx="488" cy="448" r="5" fill="#ffcb6b" class="pulse"/>
    <text bind:this={leaderIdxEl} class="sublabel" x="500" y="452" text-anchor="start">term {app.raft.term} · idx {app.raft.leaderIdx}</text>
  </g>
  <g id="walL" class:highlight={isHi("walL")}>
    <rect class="box" x="610" y="495" width="80" height="20" stroke="#ff6b6b" stroke-width="1.5"/>
    <text class="sublabel" x="650" y="509" text-anchor="middle" fill="#ff6b6b">WAL</text>
  </g>

  <!-- FSM (one per node) + Cache + Pebble -->
  <g id="fsmF2" class:highlight={isHi("fsmF2")}>
    <rect class="box" x="780" y="120" width="160" height="40"/>
    <text class="label" x="860" y="138" text-anchor="middle">FSM</text>
    <text bind:this={f2ApplEl} class="applied-idx-follower" x="860" y="153" text-anchor="middle">applied {app.raft.f2Applied}</text>
  </g>
  <g id="fsmF1" class:highlight={isHi("fsmF1")}>
    <rect class="box" x="780" y="250" width="160" height="40"/>
    <text class="label" x="860" y="268" text-anchor="middle">FSM</text>
    <text bind:this={f1ApplEl} class="applied-idx-follower" x="860" y="283" text-anchor="middle">applied {app.raft.f1Applied}</text>
  </g>
  <g id="fsmL" class:highlight={isHi("fsmL")}>
    <rect class="box" x="780" y="380" width="160" height="42"/>
    <text class="label" x="860" y="398" text-anchor="middle">FSM</text>
    <text bind:this={leaderApplEl} class="applied-idx-leader" x="860" y="414" text-anchor="middle">lastPersistedIndex = {app.raft.leaderApplied}</text>
  </g>
  <g id="box-cache" class:highlight={isHi("box-cache")}>
    <rect class="box" x="780" y="438" width="75" height="42" stroke="#c792ea"/>
    <text class="label"    x="817" y="456" text-anchor="middle" fill="#c792ea">Cache</text>
    <text class="sublabel" x="817" y="471" text-anchor="middle">in-memory</text>
  </g>
  <g id="box-pebble" class:highlight={isHi("box-pebble")}>
    <rect class="box" x="865" y="438" width="75" height="42"/>
    <text class="label"    x="902" y="456" text-anchor="middle">Pebble</text>
    <text class="sublabel" x="902" y="471" text-anchor="middle">durable</text>
  </g>

  <!-- Workers -->
  <!-- Log notifier — single broadcast point (signal.FanOut at machine.go:837) -->
  <g id="box-notifier" class:highlight={isHi("box-notifier")}>
    <rect class="box" x="958" y="200" width="28" height="230" stroke="#ffcb6b"/>
    <text class="node-title" x="972" y="270" text-anchor="middle" fill="#ffcb6b"
          transform="rotate(-90 972 270)">NOTIFIER</text>
    <text class="sublabel" x="972" y="375" text-anchor="middle"
          transform="rotate(-90 972 375)">signal.FanOut</text>
  </g>

  <!-- Five subscribers tail the log via the notifier broadcast -->
  <g id="box-worker-index" class:highlight={isHi("box-worker-index")}>
    <rect class="box" x="1000" y="120" width="180" height="50"/>
    <text class="label"    x="1090" y="143" text-anchor="middle">Index Builder</text>
    <text class="sublabel" x="1090" y="160" text-anchor="middle">tail log → ReadStore · batch ~1000</text>
  </g>
  <g id="box-worker-sinks" class:highlight={isHi("box-worker-sinks")}>
    <rect class="box" x="1000" y="200" width="180" height="50"/>
    <text class="label"    x="1090" y="223" text-anchor="middle">Event Sinks</text>
    <text class="sublabel" x="1090" y="240" text-anchor="middle">Kafka · NATS · batch ~64</text>
  </g>
  <g id="box-worker-archiver" class:highlight={isHi("box-worker-archiver")}>
    <rect class="box" x="1000" y="280" width="180" height="50"/>
    <text class="label"    x="1090" y="303" text-anchor="middle">Cold Storage (S3)</text>
    <text class="sublabel" x="1090" y="320" text-anchor="middle">FSM-dispatched archive jobs</text>
  </g>
  <g id="box-worker-sealer" class:highlight={isHi("box-worker-sealer")}>
    <rect class="box" x="1000" y="380" width="180" height="50"/>
    <text class="label"    x="1090" y="403" text-anchor="middle">Sealer</text>
    <text class="sublabel" x="1090" y="420" text-anchor="middle">periods · BLAKE3 hash chain</text>
  </g>

  <!-- Static edges (every animated dot rides exactly one of these via getPointAtLength) -->
  <path id="e-client-grpc"      class="edge" d="M160,315 C190,315 200,230 220,230"/>
  <path id="e-grpc-ctrl"        class="edge" d="M300,260 L300,280"/>
  <path id="e-ctrl-adm"         class="edge" d="M300,340 L300,360"/>
  <path id="e-adm-leader"       class="edge" d="M380,390 C420,390 440,410 470,410"/>
  <path id="e-leader-f1"        class="edge" d="M580,380 L580,330"/>
  <path id="e-leader-f2"        class="edge" d="M580,380 C400,380 400,160 470,160"/>
  <path id="e-f2-fsm"           class="edge" d="M690,150 L780,140"/>
  <path id="e-f1-fsm"           class="edge" d="M690,270 L780,270"/>
  <path id="e-leader-fsm"       class="edge" d="M690,400 L780,401"/>
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
       the left, so the longer Pebble arc nests around the shorter Cache arc. -->
  <path id="e-consult-cache"    class="consult-edge" d="M340,420 C340,560 720,560 817,480"/>
  <path id="e-consult-pebble"   class="consult-edge" d="M260,420 C260,585 800,585 902,480"/>

  <!-- WAL side-persistence rails (Raft bottom → separate WAL box) -->
  <path id="e-wal-f2"           class="edge" d="M650,200 L650,215" stroke="#ff6b6b" opacity="0.55"/>
  <path id="e-wal-f1"           class="edge" d="M650,330 L650,345" stroke="#ff6b6b" opacity="0.55"/>
  <path id="e-wal-leader"       class="edge" d="M650,480 L650,495" stroke="#ff6b6b" opacity="0.55"/>

  <!-- Animated dots layer — populated imperatively by lib/anim.js -->
  <g bind:this={dotsEl} id="dots"></g>
</svg>
