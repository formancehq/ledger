/**
 * Auth middleware — protects API routes behind session validation and
 * enforces role-based authorization.
 *
 * When auth is disabled, this is a transparent passthrough (does nothing).
 * When auth is enabled, every /api/* request (except health checks and the
 * auth routes themselves) must carry a valid session cookie. Requests without
 * a valid session get a 401 JSON response, which the frontend catches to
 * redirect the user to /api/auth/login.
 *
 * Authorization rules (guests are blocked from):
 *  - POST/PATCH/DELETE /api/ledger-defaults*  (configuration management)
 *  - PATCH /api/namespaces/:ns/ledger-services/:name (generic edit, not scale/restart)
 *
 * The middleware also stores the SessionData in Hono's context (via c.set)
 * so that downstream route handlers can read it with c.get("session").
 */

import type { MiddlewareHandler } from "hono";
import { authEnabled } from "./config.js";
import { getSession, type SessionData } from "./session.js";

/** Hono env type — makes c.get("session") type-safe in route handlers. */
export type AuthEnv = { Variables: { session?: SessionData } };

/** Paths that are always accessible, even without authentication. */
const PUBLIC_PREFIXES = ["/api/health", "/api/auth/"];

/** Check if a PATCH targets the generic LedgerService update (not scale/restart). */
function isGenericLedgerServicePatch(method: string, path: string): boolean {
  if (method !== "PATCH") return false;
  // Match /api/namespaces/:ns/ledger-services/:name but NOT .../scale
  const match = path.match(
    /^\/api\/namespaces\/[^/]+\/ledger-services\/[^/]+$/
  );
  return match !== null;
}

/** Check if a request targets ledger-defaults (admin-only). */
function isLedgerDefaultsMutation(method: string, path: string): boolean {
  if (!path.startsWith("/api/ledger-defaults")) return false;
  return method === "POST" || method === "PATCH" || method === "DELETE";
}

export function createAuthMiddleware(): MiddlewareHandler<AuthEnv> {
  return async (c, next) => {
    if (!authEnabled) return next();

    const path = c.req.path;

    // Frontend static assets (JS, CSS, images) are not behind /api/
    if (!path.startsWith("/api/")) return next();

    // Health checks and auth routes must remain public
    if (PUBLIC_PREFIXES.some((p) => path.startsWith(p))) return next();

    const session = getSession(c.req.header("cookie"));
    if (!session) {
      return c.json({ error: { message: "Unauthorized" } }, 401);
    }

    // Store session so downstream handlers can read it via c.get("session")
    c.set("session", session);

    // Authorization: check role-based restrictions for guests
    if (session.role === "guest") {
      const method = c.req.method;

      if (isLedgerDefaultsMutation(method, path)) {
        return c.json(
          { error: { message: "Forbidden", requiredRole: "admin" } },
          403
        );
      }

      if (isGenericLedgerServicePatch(method, path)) {
        return c.json(
          { error: { message: "Forbidden", requiredRole: "admin" } },
          403
        );
      }
    }

    return next();
  };
}
