import type { Context, Next } from "hono";

/**
 * Parses ALLOWED_NAMESPACES env var (comma-separated) into a Set.
 * Empty or unset means all namespaces are allowed.
 */
const allowedNamespaces: Set<string> | null = (() => {
  const raw = process.env.ALLOWED_NAMESPACES?.trim();
  if (!raw) return null;
  return new Set(raw.split(",").map((s) => s.trim()).filter(Boolean));
})();

/**
 * Returns the allowed namespaces set, or null if unrestricted.
 */
export function getAllowedNamespaces(): Set<string> | null {
  return allowedNamespaces;
}

/**
 * Middleware that rejects requests targeting a namespace not in ALLOWED_NAMESPACES.
 * Extracts :ns from the route params. If ALLOWED_NAMESPACES is not set, all namespaces pass.
 */
export async function namespaceGuard(c: Context, next: Next): Promise<Response | void> {
  if (!allowedNamespaces) return next();

  const ns = c.req.param("ns");
  if (!ns) return next();

  if (!allowedNamespaces.has(ns)) {
    return c.json(
      { error: { message: `Namespace "${ns}" is not allowed. Allowed: ${[...allowedNamespaces].join(", ")}` } },
      403,
    );
  }
  return next();
}
