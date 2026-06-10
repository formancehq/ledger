import QueueBadge from "./QueueBadge";

type Point = [number, number];

interface Props {
  // SVG path ids for the two hops — referenced by the engine topology
  // when it routes msgs through this arc (hop 1 = sender→queue, hop 2
  // = queue→receiver).
  id1:    string;
  id2:    string;
  // Linear mode: pass three points, the component derives "M…L…" for
  // each hop and positions the badge at `mid`.
  from?:  Point;
  mid?:   Point;
  to?:    Point;
  // Custom-d mode: hand the two raw "d" strings + the midpoint coords.
  // Used by arcs that aren't straight lines (e.g., admission→leader's
  // curved entrance).
  d1?:    string;
  d2?:    string;
  midpoint?: Point;
  // QueueBadge props.
  count:  number;
  accent: string;
  title:  string;
  className?: string;
}

// Composite component bundling the two-segment SVG arc + the queue
// midpoint badge so they can't drift apart when one of them moves.
// Editing the arc updates the badge position automatically (and vice
// versa) — the midpoint is the single source of truth.
export default function QueueArc({
  id1, id2,
  from, mid, to,
  d1, d2, midpoint,
  count, accent, title,
  className = "queue-edge",
}: Props) {
  const m = midpoint ?? mid;
  if (!m) {
    console.error("QueueArc: must pass either `mid` (linear) or `midpoint` (custom-d)");
    return null;
  }
  const finalD1 = d1 ?? (from ? `M${from[0]},${from[1]} L${m[0]},${m[1]}` : null);
  const finalD2 = d2 ?? (to   ? `M${m[0]},${m[1]} L${to[0]},${to[1]}`     : null);
  if (!finalD1 || !finalD2) {
    console.error("QueueArc: linear mode needs all of from/mid/to; custom mode needs d1/d2/midpoint");
    return null;
  }
  return (
    <>
      <path id={id1} className={className} d={finalD1} />
      <path id={id2} className={className} d={finalD2} />
      <QueueBadge cx={m[0]} cy={m[1]} count={count} accent={accent} title={title} />
    </>
  );
}
