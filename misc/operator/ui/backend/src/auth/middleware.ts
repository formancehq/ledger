import type { MiddlewareHandler } from "hono";
import { authEnabled } from "./config.js";
import { getSession } from "./session.js";

const PUBLIC_PREFIXES = ["/api/health", "/api/auth/"];

export function createAuthMiddleware(): MiddlewareHandler {
  return async (c, next) => {
    if (!authEnabled) return next();

    const path = c.req.path;

    // Skip non-API paths (frontend static assets)
    if (!path.startsWith("/api/")) return next();

    // Skip public API paths
    if (PUBLIC_PREFIXES.some((p) => path.startsWith(p))) return next();

    const session = getSession(c.req.header("cookie"));
    if (!session) {
      return c.json({ error: { message: "Unauthorized" } }, 401);
    }

    return next();
  };
}
