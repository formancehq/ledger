import { useState } from "react";
import { Info, ChevronRight, ChevronLeft } from "lucide-react";
import { cn } from "@/lib/utils";

const PANEL_KEY = "ledger-ui-info-panel";

interface InfoPanelProps {
  children: React.ReactNode;
  className?: string;
}

/**
 * Collapsible right-side panel that displays contextual help information.
 * State is persisted in localStorage so the user's preference survives reloads.
 */
export function InfoPanel({ children, className }: InfoPanelProps) {
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(PANEL_KEY) === "collapsed"
  );

  const toggle = () => {
    const next = !collapsed;
    setCollapsed(next);
    localStorage.setItem(PANEL_KEY, next ? "collapsed" : "open");
  };

  return (
    <aside
      className={cn(
        "relative shrink-0 border-l bg-muted/30 transition-[width] duration-200",
        collapsed ? "w-10" : "w-80",
        className
      )}
    >
      <button
        onClick={toggle}
        className="absolute -left-3 top-4 z-10 flex h-6 w-6 items-center justify-center rounded-full border bg-background shadow-sm hover:bg-accent"
        title={collapsed ? "Show help panel" : "Hide help panel"}
      >
        {collapsed ? (
          <ChevronLeft className="h-3 w-3" />
        ) : (
          <ChevronRight className="h-3 w-3" />
        )}
      </button>

      {collapsed ? (
        <div className="flex h-full items-start justify-center pt-12">
          <Info className="h-4 w-4 text-muted-foreground" />
        </div>
      ) : (
        <div className="p-4 space-y-4 overflow-auto h-full text-sm">
          {children}
        </div>
      )}
    </aside>
  );
}

interface InfoSectionProps {
  title: string;
  children: React.ReactNode;
}

/**
 * A titled section inside the InfoPanel.
 */
export function InfoSection({ title, children }: InfoSectionProps) {
  return (
    <div>
      <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-1">
        {title}
      </h3>
      <div className="text-xs text-muted-foreground leading-relaxed space-y-2">
        {children}
      </div>
    </div>
  );
}

interface PageWithInfoProps {
  children: React.ReactNode;
  info?: React.ReactNode;
}

/**
 * Flex layout wrapper: main content on the left, optional InfoPanel on the right.
 * When no `info` is provided, renders only the content without a panel.
 */
export function PageWithInfo({ children, info }: PageWithInfoProps) {
  if (!info) {
    return <>{children}</>;
  }

  return (
    <div className="flex gap-0 -m-6 min-h-full">
      <div className="flex-1 p-6 overflow-auto">{children}</div>
      <InfoPanel>{info}</InfoPanel>
    </div>
  );
}
