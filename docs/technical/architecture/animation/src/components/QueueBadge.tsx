interface Props {
  cx:     number;
  cy:     number;
  count:  number;
  accent: string;
  title:  string;
}

// SVG midpoint badge that sits on a batching arc (admission→leader,
// leader entry, etc.). Shows the queue depth inside a small circle
// styled to match the arc's accent color. Fades when empty so it
// reads as "the queue lives here" without being noisy on a quiet
// engine. Native <title> for tooltip; no hover overlay for now.
export default function QueueBadge({ cx, cy, count, accent, title }: Props) {
  const empty = count === 0;
  return (
    <g className="queue-badge" style={{ pointerEvents: "none" }}>
      <title>{title}</title>
      <circle
        cx={cx} cy={cy} r={6}
        fill="#0d1424"
        stroke={accent}
        strokeWidth={1.6}
        opacity={empty ? 0.45 : 0.95}
      />
      {/* Count rendered above the circle so a parked dot landing at
          the same coords doesn't overlap with the digit. */}
      <text
        x={cx} y={cy - 11}
        textAnchor="middle"
        fontSize={10}
        fontWeight={700}
        fill={accent}
      >
        {count}
      </text>
    </g>
  );
}
