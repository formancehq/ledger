/** Format a defaults value as a placeholder string. Returns undefined if no default. */
export function defaultPlaceholder(
  value: string | number | boolean | undefined | null,
  fallback?: string
): string | undefined {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "boolean") return undefined; // booleans use hint instead
  if (typeof value === "string" && value === "") return fallback;
  return `${value} (default)`;
}

/** Format a defaults boolean as a hint string. Returns undefined if no default. */
export function defaultHint(
  value: boolean | undefined | null
): string | undefined {
  if (value === undefined || value === null) return undefined;
  return `Default: ${value ? "enabled" : "disabled"}`;
}
