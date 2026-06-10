import { useEffect, useRef, useState } from "react";
import { useShallow } from "zustand/react/shallow";
import { useEngine, useEngineStore } from "../engine/EngineContext";
import { NODE, NODE_MARKERS, junctionEdges } from "../engine/topology";
import type { EdgeSnapshot, NodeId, TokenInFlight } from "../engine/types";
import EngineNodeOverlay from "./EngineNodeOverlay";
import EngineTokenOverlay from "./EngineTokenOverlay";
import { txIdOf } from "../engine/banner";
import type { LeaderState } from "../engine/nodes/leader";
import type { FollowerState } from "../engine/nodes/follower";
import type { FsmState, FsmSimpleState } from "../engine/nodes/fsm";
import type { NotifierState } from "../engine/nodes/notifier";
import type { TrackerState } from "../engine/nodes/tracker";
import type { WalState } from "../engine/nodes/wal";
import { BOXES } from "../lib/layout";
import { anim } from "../lib/anim";
import Box from "./Box";
import QueueArc from "./QueueArc";
import FlashText from "./FlashText";
import BatchDot from "./BatchDot";

// Per-hop wall-clock animation duration. Each token traverses one path
// in this much time; the next tick can fire while it's in flight (so
// the engine isn't blocked on visuals).
const HOP_MS = 500;

// Per-tx visual stagger applied at spawn time when multiple new tokens
// surface in the same React effect run (= same engine tick). Without
// it, parallel emits spawn at the exact same wall-clock instant and the
// dots overlap. Kept SMALL (20ms) so the entire fan-out fits inside one
// tick window (250ms) — without that constraint, a queue drain emitting
// 8 tokens would land its tokens across 3 ticks, breaking the leader's
// batched flush (each tick would only see 1–3 Proposes → small AEs).
const STAGGER_MS = 20;

// Color picked by tx id so multiple in-flight txs are distinguishable.
const PALETTE = [
  "#82aaff", "#c3e88d", "#ffcb6b", "#c792ea",
  "#89ddff", "#addb67", "#ff9bd2", "#fbc02d",
];
function colorForTx(txId: number | null): string {
  if (txId == null) return "#7a8aa6";
  return PALETTE[(txId - 1) % PALETTE.length];
}

interface EngineDiagramProps {}

export default function EngineDiagram(_: EngineDiagramProps) {
  const { scheduler, store, edges: engineEdges, dotsLayer } = useEngine();
  const tokens      = useEngineStore(useShallow(s => s.tokens));
  const nodes       = useEngineStore(useShallow(s => s.nodes));
  const edges       = useEngineStore(useShallow(s => s.edges));
  const activeNodes = useEngineStore(useShallow(s => s.activeNodes));
  const paused      = useEngineStore(s => s.paused);
  const dotsRef = useRef<SVGGElement>(null);
  const [hovered, setHovered]           = useState<{ id: NodeId; x: number; y: number } | null>(null);
  const [hoveredToken, setHoveredToken] = useState<{ token: TokenInFlight; x: number; y: number } | null>(null);
  // Click-pin: when set, the overlay sticks to this token regardless
  // of cursor movement. Cleared by clicking somewhere that isn't a
  // dot (the SVG background) or by clicking another dot (which repins).
  const [pinnedToken,  setPinnedToken]  = useState<{ token: TokenInFlight; x: number; y: number } | null>(null);

  // Find the topmost <g id="..."> ancestor of `target` whose id maps
  // to a registered engine node. Lets us bind one mousemove handler
  // on the SVG and identify which box the cursor is over.
  function findHoveredNode(target: EventTarget | null): NodeId | null {
    let n: Element | null = target as Element | null;
    while (n && n instanceof Element) {
      const id = n.getAttribute("id");
      if (id && nodes[id]) return id as NodeId;
      n = n.parentElement;
    }
    return null;
  }
  // makeDot tags each in-flight dot with data-tx-id="tok-<id>", so a
  // hover on a moving dot can recover the underlying TokenInFlight.
  function findHoveredToken(target: EventTarget | null): TokenInFlight | null {
    let n: Element | null = target as Element | null;
    while (n && n instanceof Element) {
      const tid = n.getAttribute("data-tx-id");
      if (tid && tid.startsWith("tok-")) {
        const id = Number(tid.slice(4));
        return tokens.find(t => t.id === id) ?? null;
      }
      n = n.parentElement;
    }
    return null;
  }
  function onSvgMouseMove(e: React.MouseEvent<SVGSVGElement>): void {
    // While pinned, ignore hover updates so the overlay stays put.
    if (pinnedToken) return;
    const tok = findHoveredToken(e.target);
    if (tok) {
      setHoveredToken({ token: tok, x: e.clientX, y: e.clientY });
      if (hovered) setHovered(null);
      return;
    }
    if (hoveredToken) setHoveredToken(null);
    const id = findHoveredNode(e.target);
    if (!id) {
      if (hovered) setHovered(null);
      return;
    }
    setHovered({ id, x: e.clientX, y: e.clientY });
  }
  function onSvgMouseLeave(): void {
    if (pinnedToken) return;
    setHovered(null);
    setHoveredToken(null);
  }
  function onSvgClick(e: React.MouseEvent<SVGSVGElement>): void {
    const tok = findHoveredToken(e.target);
    if (tok) {
      setPinnedToken({ token: tok, x: e.clientX, y: e.clientY });
      setHoveredToken(null);
      setHovered(null);
    } else {
      // Click on empty space (or non-dot SVG element) unpins.
      if (pinnedToken) setPinnedToken(null);
    }
  }

  // Bind the dots layer once — dotsLayer is provided by the engine
  // handle (one instance per <EngineProvider>) so it's stable for the
  // lifetime of this component.
  useEffect(() => {
    dotsLayer.attach(dotsRef.current);
    return () => dotsLayer.attach(null);
  }, [dotsLayer]);

  // Token animation bridge — for each token id that appears in
  // `tokens`, spawn ONE animated dot and call scheduler.landToken when
  // its anim resolves. We track started ids in a ref so re-renders
  // don't re-animate.
  //
  // Per-tx stagger: when N new tokens surface in the same effect run
  // (= same engine tick — drain-all means multiple emits land in one
  // setState batch), their anim starts are spaced N×STAGGER_MS apart
  // so the dots visibly fan out rather than overlap. Tokens arriving
  // across separate ticks/effect runs get no stagger (offset resets).
  const animatedIds = useRef<Set<number>>(new Set());
  const parkedDots  = useRef<Map<number, SVGCircleElement>>(new Map());
  useEffect(() => {
    let staggerIdx = 0;
    for (const tok of tokens) {
      if (animatedIds.current.has(tok.id)) continue;
      // Parked tokens published by scheduler.landToken are visible only
      // for hover-resolution; their dot already exists from the prior
      // anim run. Midhop tokens never park (TS already knows: no
      // `parked` field on the midhop variant), so the `parked` check
      // is only meaningful for send/call.
      if (tok.kind !== "midhop" && tok.parked) continue;
      animatedIds.current.add(tok.id);
      const delay = staggerIdx * STAGGER_MS;
      staggerIdx++;
      setTimeout(() => {
        const color = colorForTx(txIdOf(tok.msg));
        const dot   = dotsLayer.make(color, 6, `tok-${tok.id}`);
        // Scale duration by segment count so each SVG sub-path gets
        // the full HOP_MS (anim divides duration / segments.length).
        const segCount = Array.isArray(tok.via) ? tok.via.length : 1;
        anim(dot, tok.via, HOP_MS * segCount)
          .then(() => {
            // Land first — the scheduler's park-vs-delete decision (which
            // depends on whether the token has an onLand midhop callback)
            // is then mirrored here by reading the post-land published
            // state. Midpoint hops are deleted (no parking) so the
            // transient dot doesn't sit next to its synchronous handoff
            // successor.
            scheduler.landToken(tok.id);
            const survivor = store.state.tokens.find(t => t.id === tok.id);
            // Midhop tokens are always deleted by the scheduler on land
            // (never park). For send/call, mirror the scheduler's
            // park-vs-delete decision via the published `parked` flag.
            if (survivor && survivor.kind !== "midhop" && survivor.parked) {
              dot.classList.add("parked-dot");
              parkedDots.current.set(tok.id, dot);
            } else {
              dot.remove();
            }
            animatedIds.current.delete(tok.id);
          })
          .catch(err => {
            console.error("token anim failed", err);
            dot.remove();
            animatedIds.current.delete(tok.id);
          });
      }, delay);
    }
  }, [tokens]);

  // Resume sweep — when the engine transitions paused → running, drop
  // all parked dots and let the scheduler garbage-collect their tokens.
  useEffect(() => {
    if (paused) return;
    if (parkedDots.current.size === 0) return;
    for (const dot of parkedDots.current.values()) dot.remove();
    parkedDots.current.clear();
    scheduler.clearParkedTokens();
  }, [paused]);

  // Reconcile parked dots with the published token list. The scheduler
  // drops a parked token whenever its msg is consumed by the receiving
  // node's handle (see _consumeParkedFor) — when that token id leaves
  // `tokens`, remove its dot. Also covers the reset case (tokens=[]).
  useEffect(() => {
    if (parkedDots.current.size === 0) return;
    const live = new Set(tokens.map(t => t.id));
    for (const [id, dot] of parkedDots.current) {
      if (!live.has(id)) {
        dot.remove();
        parkedDots.current.delete(id);
      }
    }
  }, [tokens]);

  // Reads — narrow casts at the boundary; the engine snapshots typing
  // would propagate too far otherwise (see store.ts notes).
  const leader   = nodes[NODE.leader]?.state    as LeaderState   | undefined;
  const followF1 = nodes[NODE.followerF1]?.state as FollowerState | undefined;
  const followF2 = nodes[NODE.followerF2]?.state as FollowerState | undefined;
  const fsmL     = nodes[NODE.fsmL]?.state      as FsmState        | undefined;
  const fsmF1    = nodes[NODE.fsmF1]?.state     as FsmSimpleState  | undefined;
  const fsmF2    = nodes[NODE.fsmF2]?.state     as FsmSimpleState  | undefined;
  const notifier = nodes[NODE.notifier]?.state  as NotifierState | undefined;
  const tracker  = nodes[NODE.tracker]?.state    as TrackerState | undefined;
  const walL     = nodes[NODE.walLeader]?.state as WalState | undefined;
  // Edge snapshots are consumed inside the junctionEdges() loop below
  // (one lookup per buffering edge via `edges[edge.id]`); we don't need
  // to pull individual edge snapshots into named locals here anymore.

  function walBoundsText(s: WalState | undefined): string {
    if (!s || (s.lastSyncIdx === 0 && s.firstSyncIdx === 1 && s.truncations === 0)) {
      return "[empty]";
    }
    const trunc = s.truncations > 0 ? ` · ${s.truncations} trunc` : "";
    // After a truncation past the last fsync (firstSyncIdx > lastSyncIdx)
    // the WAL is fully drained — there's no "first..last" window to
    // display. Surface the next-write index instead so the reader sees
    // where the next fsync will land.
    if (s.firstSyncIdx > s.lastSyncIdx) {
      return `[empty · next ${s.firstSyncIdx}]${trunc}`;
    }
    return `[${s.firstSyncIdx}..${s.lastSyncIdx}]${trunc}`;
  }

  return (
    <>
    <svg viewBox="190 70 1010 580" preserveAspectRatio="xMidYMid meet"
         onMouseMove={onSvgMouseMove}
         onMouseLeave={onSvgMouseLeave}
         onClick={onSvgClick}>
      <text className="stage-label" x={300}  y={100}>Parallel Path</text>
      <text className="stage-label" x={510}  y={100}>Consensus (Raft)</text>
      <text className="stage-label" x={835}  y={100}>Apply (FSM)</text>
      <text className="stage-label" x={1090} y={100}>Workers</text>

      <Box box={BOXES.client} highlights={activeNodes}/>
      <Box box={BOXES.grpc} highlights={activeNodes}/>
      <Box box={BOXES.ctrl} highlights={activeNodes}/>
      <Box box={BOXES.adm} highlights={activeNodes}/>
      <Box box={BOXES.tracker} highlights={activeNodes}>
        <text className="label" x={BOXES.tracker.x + BOXES.tracker.w / 2} y={BOXES.tracker.y + 16} textAnchor="middle">IndexTracker</text>
        <FlashText value={tracker?.nextIndex ?? 1} className="sublabel" x={BOXES.tracker.x + BOXES.tracker.w / 2} y={BOXES.tracker.y + 32} textAnchor="middle">next index = {tracker?.nextIndex ?? 1}</FlashText>
      </Box>

      {[
        { box: BOXES.followerF2, follower: followF2 },
        { box: BOXES.followerF1, follower: followF1 },
      ].map(({ box, follower }) => (
        <Box key={box.id} box={box} highlights={activeNodes}>
          <text className="node-title" x={box.x + box.w / 2} y={box.y + 22} textAnchor="middle" fill="#82aaff">FOLLOWER</text>
          <text className="sublabel"   x={box.x + box.w / 2} y={box.y + 40} textAnchor="middle">append entries → ack</text>
          <FlashText value={follower?.log.length ?? 0} className="sublabel" x={box.x + 30} y={box.y + 60} textAnchor="start">log idx {follower?.log.length ?? 0}</FlashText>
          <FlashText value={follower?.commitIdx ?? 0} className="sublabel" x={box.x + 30} y={box.y + 74} textAnchor="start">commited index {follower?.commitIdx ?? 0}</FlashText>
        </Box>
      ))}

      <Box box={BOXES.leader} klass="box box-leader" highlights={activeNodes}>
        <text className="node-title" x={BOXES.leader.x + BOXES.leader.w / 2} y={BOXES.leader.y + 22} textAnchor="middle" fill="#ffcb6b">LEADER (raft)</text>
        <text className="sublabel"   x={BOXES.leader.x + BOXES.leader.w / 2} y={BOXES.leader.y + 40} textAnchor="middle">propose &amp; replicate</text>
        <FlashText value={leader?.log.length ?? 0} className="sublabel" x={BOXES.leader.x + 30} y={BOXES.leader.y + 68} textAnchor="start">log idx {leader?.log.length ?? 0}</FlashText>
        <FlashText value={leader?.commitIdx ?? 0} className="sublabel" x={BOXES.leader.x + 30} y={BOXES.leader.y + 80} textAnchor="start">commited index {leader?.commitIdx ?? 0}</FlashText>
      </Box>

      {[BOXES.walF2, BOXES.walF1].map(walBox => (
        <Box key={walBox.id} box={walBox} highlights={activeNodes} stroke="#ff6b6b">
          <text className="sublabel" x={walBox.x + walBox.w / 2} y={walBox.y + 14} textAnchor="middle" fill="#ff6b6b">WAL</text>
        </Box>
      ))}
      <Box box={BOXES.walL} highlights={activeNodes} stroke="#ff6b6b">
        <text className="sublabel" x={BOXES.walL.x + BOXES.walL.w / 2} y={BOXES.walL.y + 16} textAnchor="middle" fill="#ff6b6b">WAL</text>
        <FlashText value={walBoundsText(walL)} className="applied-idx-leader" x={BOXES.walL.x + BOXES.walL.w / 2} y={BOXES.walL.y + 33} textAnchor="middle" fill="#ff6b6b">{walBoundsText(walL)}</FlashText>
      </Box>
      <Box box={BOXES.compactor} highlights={activeNodes} stroke="#ff6b6b"/>

      <Box box={BOXES.fsmF2} highlights={activeNodes}>
        <text className="label" x={BOXES.fsmF2.x + BOXES.fsmF2.w / 2} y={BOXES.fsmF2.y + 18} textAnchor="middle">FSM</text>
        <FlashText value={fsmF2?.appliedIdx ?? 0} className="applied-idx-follower" x={BOXES.fsmF2.x + BOXES.fsmF2.w / 2} y={BOXES.fsmF2.y + 33} textAnchor="middle">applied {fsmF2?.appliedIdx ?? 0}</FlashText>
      </Box>
      <Box box={BOXES.fsmF1} highlights={activeNodes}>
        <text className="label" x={BOXES.fsmF1.x + BOXES.fsmF1.w / 2} y={BOXES.fsmF1.y + 18} textAnchor="middle">FSM</text>
        <FlashText value={fsmF1?.appliedIdx ?? 0} className="applied-idx-follower" x={BOXES.fsmF1.x + BOXES.fsmF1.w / 2} y={BOXES.fsmF1.y + 33} textAnchor="middle">applied {fsmF1?.appliedIdx ?? 0}</FlashText>
      </Box>
      <Box box={BOXES.fsmL} highlights={activeNodes}>
        <text className="label" x={BOXES.fsmL.x + BOXES.fsmL.w / 2} y={BOXES.fsmL.y + 18} textAnchor="middle">FSM</text>
        <FlashText value={fsmL?.appliedIdx ?? 0} className="applied-idx-leader" x={BOXES.fsmL.x + BOXES.fsmL.w / 2} y={BOXES.fsmL.y + 34} textAnchor="middle">lastPersistedIndex = {fsmL?.appliedIdx ?? 0}</FlashText>
      </Box>

      {/* Buffering edges (QueueEdge / ChannelEdge) — each carries a
          `junction` declaring its midpoint (x,y), src/dst box anchors,
          and badge style. We render the two-segment SVG arc + QueueBadge
          from that single declaration via `QueueArc`, so the badge can
          never drift away from where the path actually terminates. If
          the junction defines a `held` callback, we additionally render
          a BatchDot at `fromAnchor` (= caller's box edge) showing items
          blocked at the edge entry — either from the edge's `held`
          buffer (QueueEdge) or from the caller's own state (ChannelEdge). */}
      {junctionEdges(engineEdges).map(edge => {
        const j    = edge.junction;
        const snap = edges[edge.id] as EdgeSnapshot | undefined;
        // Badge counts every msg currently between src and dst: queue
        // depth + path-1 in-flight (incoming) + path-2 in-flight
        // (outgoing). A token that LANDED at dst and is parked there
        // (paused mode) is NOT counted — semantically it's already at
        // dst (in its mailbox), not on the wire.
        const path2InTransit = tokens.filter(t =>
          t.kind !== "midhop" && !t.parked && typeof t.via === "string" && t.via === j.pathIds[1],
        ).length;
        const count = (snap?.queue.length ?? 0) + (snap?.mailboxSize ?? 0) + path2InTransit;
        const cap   = snap?.capacity ?? 0;
        const src   = j.fromAnchor();
        const dst   = j.toAnchor();
        const peek  = <S,>(id: NodeId): S | undefined => nodes[id]?.state as S | undefined;
        const heldCount = (j.held && snap) ? j.held(peek, snap) : 0;
        return (
          <g key={edge.id}>
            <QueueArc
              id1={j.pathIds[0]} id2={j.pathIds[1]}
              from={[src.x, src.y]} mid={[j.x, j.y]} to={[dst.x, dst.y]}
              count={count}
              accent={j.accent}
              title={j.title({ queue: snap?.queue ?? [], held: snap?.held ?? [], capacity: cap, forwarded: snap?.forwarded ?? 0, rejected: snap?.rejected ?? 0, mailboxSize: snap?.mailboxSize ?? 0 })}
            />
            {heldCount > 0 && (
              <BatchDot
                cx={src.x} cy={src.y}
                count={heldCount}
                accent={j.accent}
                title={`${heldCount} item${heldCount > 1 ? "s" : ""} waiting at edge entry (queue saturated)`}
              />
            )}
          </g>
        );
      })}

      {/* Node-edge BatchDot markers — back-pressure indicators glued to
          a box perimeter (input or output side). Their `anchor` derives
          from `BOXES.<node>.{leftMid,rightMid}()` so a box move auto-
          propagates. `showWhenEmpty` keeps a faded "0" pill visible at
          all times for unambiguous user feedback. */}
      {NODE_MARKERS.map((m, i) => {
        const p     = m.anchor();
        const peek  = <S,>(id: NodeId): S | undefined => nodes[id]?.state as S | undefined;
        const count = m.count(peek);
        return (
          <BatchDot
            key={i}
            cx={p.x} cy={p.y}
            count={count}
            accent={m.accent}
            title={m.title}
            showWhenEmpty
          />
        );
      })}

      {/* Applier nodes — small inline boxes between each raft node's
          cap-1 apply queue and its FSM. The cap-1 channel feeds the
          applier; the applier then calls fsm via a synchronous CALL
          edge (round-trip dot). FsmAck flows back through the applier. */}
      <g id={NODE.applierL}>
        <rect x={720} y={388} width={40} height={24} rx={4} ry={4}
              className={`applier-box${activeNodes.includes(NODE.applierL) ? " active" : ""}`} />
        <text x={740} y={403} textAnchor="middle" className="applier-label">applier</text>
      </g>
      <g id={NODE.applierF1}>
        <rect x={720} y={258} width={40} height={24} rx={4} ry={4}
              className={`applier-box${activeNodes.includes(NODE.applierF1) ? " active" : ""}`} />
        <text x={740} y={273} textAnchor="middle" className="applier-label">applier</text>
      </g>
      <g id={NODE.applierF2}>
        <rect x={720} y={130} width={40} height={24} rx={4} ry={4}
              className={`applier-box${activeNodes.includes(NODE.applierF2) ? " active" : ""}`} />
        <text x={740} y={145} textAnchor="middle" className="applier-label">applier</text>
      </g>

      <Box box={BOXES.cache}      highlights={activeNodes}/>
      <Box box={BOXES.pebble}     highlights={activeNodes}/>
      <Box box={BOXES.processing} highlights={activeNodes}/>

      <Box box={BOXES.notifier} stroke="#ffcb6b" highlights={activeNodes}>
        <text className="node-title" x={BOXES.notifier.x + 14} y={BOXES.notifier.y + 70} textAnchor="middle" fill="#ffcb6b"
              transform={`rotate(-90 ${BOXES.notifier.x + 14} ${BOXES.notifier.y + 70})`}>NOTIFIER</text>
        <FlashText value={notifier?.lastSeq ?? 0} className="sublabel" x={BOXES.notifier.x + 14} y={BOXES.notifier.y + 175} textAnchor="middle"
              transform={`rotate(-90 ${BOXES.notifier.x + 14} ${BOXES.notifier.y + 175})`}>seq {notifier?.lastSeq ?? 0}</FlashText>
      </Box>

      <Box box={BOXES.workerIndex}    highlights={activeNodes}/>
      <Box box={BOXES.workerSinks}    highlights={activeNodes}/>
      <Box box={BOXES.workerArchiver} highlights={activeNodes}/>
      <Box box={BOXES.workerSealer}   highlights={activeNodes}/>

      <path id="e-client-grpc"  className="edge" d="M300,180 L300,230"/>
      <path id="e-grpc-ctrl"    className="edge" d="M300,290 L300,310"/>
      <path id="e-ctrl-adm"     className="edge" d="M300,370 L300,390"/>
      {/* admission↔leader proposeCh arc + badge are rendered by
          <QueueArc> in the buffering-edges loop above. */}
      <path id="e-leader-adm"   className="edge" d="M470,440 C440,440 410,430 380,438" stroke="#3a4d72"/>
      <path id="e-leader-f1"    className="edge" d="M580,380 L580,330"/>
      <path id="e-leader-f2"    className="edge" d="M580,380 C400,380 400,160 470,160"/>
      {/* Apply path arcs raft↔applier are rendered by <QueueArc> in the
          buffering-edges loop above (the cap-1 ChannelEdges). Only the
          synchronous applier→fsm call edges remain inline here. */}
      <path id="e-applierL-fsm"  className="edge"       d="M760,400 L780,400"/>
      <path id="e-applierF1-fsm" className="edge"       d="M760,270 L780,270"/>
      <path id="e-applierF2-fsm" className="edge"       d="M760,142 L780,140"/>
      {/* Async-future return arcs — FsmAck (applier → raft) routed on a
          DEDICATED curve arching above the apply ChannelEdge so it's
          visually distinct from the forward bufferised hop. Same for
          PebbleAck (pebble → applier) which arcs around the right of
          the pebble box. Stroke is dashed to surface "future resolved
          in background" vs the synchronous forward call. */}
      <path id="e-applierL-leader-ack"      className="edge async-return" d="M720,400 C715,380 695,380 690,400"/>
      <path id="e-applierF1-followerF1-ack" className="edge async-return" d="M720,270 C715,250 695,250 690,270"/>
      <path id="e-applierF2-followerF2-ack" className="edge async-return" d="M720,142 C715,122 695,122 690,142"/>
      <path id="e-pebble-applierL-ack"      className="edge async-return" d="M817,495 C870,488 878,430 760,400"/>
      {/* Boxes swapped: pebble now sits at the left (780-855), cache at
          the right (865-940). The two admission consult arcs swap
          endpoints accordingly. */}
      <path id="e-consult-cache"  className="consult-edge" d="M340,450 C340,590 720,590 902,537"/>
      <path id="e-consult-pebble" className="consult-edge" d="M260,450 C260,617 800,617 817,537"/>
      <path id="e-adm-tracker"    className="consult-edge" d="M235,450 L235,590"/>
      <path id="e-fsm-cache"      className="consult-edge" d="M860,422 L902,495"/>
      {/* Forward fsmL→pebble passes through the qFsmLPebble queue at the
          midpoint (881, 458). The PebbleAck reverse still uses the
          unsplit original arc. */}
      {/* Applier → pebble: cap-1 committer queue at the midpoint.
          Two straight segments instead of a curve so the dot tracks a
          predictable line and the queue badge sits at the natural
          midpoint without creating a visible kink. */}
      {/* applier→pebble cap-1 committer ChannelEdge is rendered by
          <QueueArc> in the buffering-edges loop above. */}
      <path id="e-fsm-processing" className="consult-edge" d="M860,422 L860,590"/>
      {/* applier → notifier (NotifyLogs after pebble ack). The arc
          arches over the FSM box so the line reads "from applier" not
          "from FSM". */}
      <path id="e-fsm-notifier"        className="edge" d="M755,388 C760,355 940,348 958,315"/>
      <path id="e-notifier-w-index"    className="edge" d="M986,315 C993,315 993,145 1000,145"/>
      <path id="e-notifier-w-sinks"    className="edge" d="M986,315 C993,315 993,225 1000,225"/>
      <path id="e-notifier-w-archiver" className="edge" d="M986,315 C993,315 993,305 1000,305"/>
      <path id="e-notifier-w-sealer"   className="edge" d="M986,315 C993,315 993,405 1000,405"/>
      <path id="e-wal-leader" className="consult-edge" d="M580,480 L580,495" stroke="#ff6b6b" opacity={0.55}/>
      <path id="e-wal-f1"     className="consult-edge" d="M650,330 L650,345" stroke="#ff6b6b" opacity={0.55}/>
      <path id="e-wal-f2"     className="consult-edge" d="M650,200 L650,215" stroke="#ff6b6b" opacity={0.55}/>
      <path id="e-compactor-pebble" className="consult-edge" d="M690,611 C780,611 870,560 902,537" stroke="#ff6b6b" opacity={0.55}/>
      <path id="e-compactor-wal"    className="consult-edge" d="M580,590 L580,537"               stroke="#ff6b6b" opacity={0.55}/>

      <g ref={dotsRef} id="dots"/>
    </svg>
    <EngineNodeOverlay  hovered={hovered}      />
    <EngineTokenOverlay hovered={pinnedToken ?? hoveredToken} pinned={pinnedToken != null} />
    </>
  );
}
