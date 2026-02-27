/**
 * Auth middleware — protects API routes behind session validation.
 *
 * When auth is disabled, this is a transparent passthrough (does nothing).
 * When auth is enabled, every /api/* request (except health checks and the
 * auth routes themselves) must carry a valid session cookie. Requests without
 * a valid session get a 401 JSON response, which the frontend catches to
 * redirect the user to /api/auth/login.
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

    return next();
  };
}
