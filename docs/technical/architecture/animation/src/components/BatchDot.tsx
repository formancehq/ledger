interface Props {
  cx:     number;
  cy:     number;
  count:  number;
  accent: string;
  title:  string;
  // When true, the pill stays visible at low opacity even when count===0
  // — same "always-on indicator" behavior as QueueBadge. Defaults to
  // false so existing call sites keep their hide-when-zero contract.
  showWhenEmpty?: boolean;
}

// Visualises a batch waiting at a node's output edge — typically a
// leader/follower that has committed entries past what its downstream
// queue can accept yet. Rendered as a filled pill with the entry count
// inside, pulsing while non-zero. Hidden when count === 0 unless
// showWhenEmpty is set, in which case it stays as a faded "0" pill.
export default function BatchDot({ cx, cy, count, accent, title, showWhenEmpty = false }: Props) {
  const empty = count === 0;
  if (empty && !showWhenEmpty) return null;
  // Pill width scales mildly with count so 1-digit and 3-digit values
  // both fit without crowding.
  const w = count >= 100 ? 28 : count >= 10 ? 22 : 18;
  const h = 14;
  return (
    <g className="batch-dot" style={{ pointerEvents: "none", opacity: empty ? 0.4 : 1 }}>
      <title>{title}</title>
      <rect
        x={cx - w / 2} y={cy - h / 2} width={w} height={h} rx={7} ry={7}
        fill={accent}
        className={empty ? undefined : "pulse"}
      />
      <text
        x={cx} y={cy + 4}
        textAnchor="middle"
        fontSize={10}
        fontWeight={700}
        fill="#0d1424"
      >
        {count}
      </text>
    </g>
  );
}
