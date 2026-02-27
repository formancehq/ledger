export interface DiffEntry {
  path: string;
  oldValue: unknown;
  newValue: unknown;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function collectDiffs(
  oldObj: Record<string, unknown>,
  newObj: Record<string, unknown>,
  prefix: string,
  result: DiffEntry[]
) {
  const allKeys = new Set([...Object.keys(oldObj), ...Object.keys(newObj)]);
  for (const key of allKeys) {
    const path = prefix ? `${prefix}.${key}` : key;
    const oldVal = oldObj[key];
    const newVal = newObj[key];

    if (isPlainObject(oldVal) && isPlainObject(newVal)) {
      collectDiffs(oldVal, newVal, path, result);
    } else if (oldVal !== newVal) {
      // Skip if both are empty/undefined/null
      if (
        (oldVal === undefined || oldVal === null || oldVal === "") &&
        (newVal === undefined || newVal === null || newVal === "")
      ) {
        continue;
      }
      result.push({ path, oldValue: oldVal, newValue: newVal });
    }
  }
}

export function computeDiff(
  oldSpec: Record<string, unknown>,
  newSpec: Record<string, unknown>
): DiffEntry[] {
  const result: DiffEntry[] = [];
  collectDiffs(oldSpec, newSpec, "spec", result);
  return result;
}

export function formatValue(v: unknown): string {
  if (v === undefined || v === null) return "(not set)";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "string" && v === "") return "(not set)";
  return String(v);
}
