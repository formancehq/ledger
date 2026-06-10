import { useState, type ReactNode } from "react";

interface Props {
  title:        string;
  meta?:        ReactNode;
  headerExtra?: ReactNode;        // rendered after the meta (e.g., per-panel button)
  wrapperClass?: string;          // default "cache-panel" (matches legacy CSS)
  children:     ReactNode;
  defaultCollapsed?: boolean;
}

// Generic collapsible-panel wrapper. Click on the header toggles the
// body's visibility via the legacy .collapsed CSS class. Used to wrap
// the right-column panels uniformly so the user can fold what they
// don't need to see right now.

export default function Collapsible({
  title,
  meta,
  headerExtra,
  wrapperClass = "cache-panel",
  children,
  defaultCollapsed = false,
}: Props) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  return (
    <div className={`${wrapperClass}${collapsed ? " collapsed" : ""}`}>
      <div className="cache-header collapsible-header" onClick={() => setCollapsed(!collapsed)}>
        <span className="cache-title">{title}</span>
        {meta != null && <span className="cache-meta">{meta}</span>}
        {headerExtra}
      </div>
      <div className="collapsible-body">{children}</div>
    </div>
  );
}
