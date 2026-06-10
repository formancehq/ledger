import type { ReactNode } from "react";
import type { BoxDef } from "../lib/layout";

interface Props {
  box: BoxDef;
  highlights?: string[];
  klass?: string;
  stroke?: string | null;
  children?: ReactNode;
}

// Generic box renderer. Reads geometry + default labels off a BOXES entry
// (see lib/layout.ts). For simple boxes (title + sub) the defaults suffice;
// complex boxes (Followers, Leader, FSMs with reactive idx labels) pass
// children to render their custom contents in addition to the auto labels.
export default function Box({
  box,
  highlights = [],
  klass = "box",
  stroke,
  children,
}: Props) {
  const lit = highlights.includes(box.id);
  // Heuristic vertical positions for title/sub that match the original
  // hand-placed coords within a couple of pixels across all box heights.
  // Override per-box via box.titleY / box.subY for non-conforming layouts.
  const titleY = box.y + (box.titleY ?? (box.h <= 45 ? 18 : box.h <= 55 ? 23 : box.h <= 65 ? 25 : 30));
  const subY   = box.y + (box.subY   ?? (box.h <= 45 ? 33 : box.h <= 55 ? 40 : box.h <= 65 ? 44 : 48));
  const cx     = box.x + box.w / 2;
  const strokeAttr = stroke ?? box.stroke ?? undefined;

  return (
    <g id={box.id} className={lit ? "highlight" : undefined}>
      <rect className={klass} x={box.x} y={box.y} width={box.w} height={box.h} stroke={strokeAttr} />
      {box.title && (
        <text className="label" x={cx} y={titleY} textAnchor="middle" fill={box.titleFill ?? undefined}>
          {box.title}
        </text>
      )}
      {box.sub && (
        <text className="sublabel" x={cx} y={subY} textAnchor="middle">
          {box.sub}
        </text>
      )}
      {children}
    </g>
  );
}
