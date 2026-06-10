import { useEffect, useRef } from "react";

interface Props extends React.SVGProps<SVGTextElement> {
  value:    string | number;
  children: React.ReactNode;
}

// Wraps an SVG <text> and applies a brief flash class whenever `value`
// changes — mirrors the legacy useFlash hook. Used on box-level metrics
// (wal index, commit idx, applied idx, …) so the eye is drawn to the
// counter that just moved.

const FLASH_CLASS = "flash";
const FLASH_MS    = 700;

export default function FlashText({ value, children, className = "", ...rest }: Props) {
  const ref     = useRef<SVGTextElement>(null);
  const lastRef = useRef<string | number | undefined>(undefined);
  useEffect(() => {
    if (lastRef.current === undefined) {
      // First mount — no flash, just seed the ref.
      lastRef.current = value;
      return;
    }
    if (lastRef.current === value) return;
    lastRef.current = value;
    const el = ref.current;
    if (!el) return;
    el.classList.remove(FLASH_CLASS);
    // Force reflow so re-adding the class restarts the animation.
    void el.getBoundingClientRect();
    el.classList.add(FLASH_CLASS);
    const t = setTimeout(() => el.classList.remove(FLASH_CLASS), FLASH_MS);
    return () => clearTimeout(t);
  }, [value]);
  return (
    <text ref={ref} className={className} {...rest}>{children}</text>
  );
}
