/**
 * Auth middleware — protects API routes behind session validation.
 *
 * When auth is disabled, this is a transparent passthrough (does nothing).
 * When auth is enabled, every /api/* request (except health checks and the
 * auth routes themselves) must carry a valid session cookie. Requests without
 * a valid session get a 401 JSON response, which the frontend catches to
 * redirect the user to /api/auth/login.
 */

import type { MiddlewareHandler } from "hono";
import { authEnabled } from "./config.js";
import { getSession } from "./session.js";

/** Paths that are always accessible, even without authentication. */
const PUBLIC_PREFIXES = ["/api/health", "/api/auth/"];

export function createAuthMiddleware(): MiddlewareHandler {
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

    return next();
  };
}
