import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import api from "./routes/index.js";
import { errorHandler } from "./middleware/error-handler.js";
import { handleUpgrade } from "./ws/terminal.js";
import { authEnabled, loadAuthConfig } from "./auth/config.js";
import { initSessions } from "./auth/session.js";
import { initOidc } from "./auth/oidc.js";
import { createAuthRoutes } from "./auth/routes.js";
import { createAuthMiddleware } from "./auth/middleware.js";

async function start(): Promise<void> {
  // Load auth configuration from env vars. Returns null when AUTH_ENABLED is not "true".
  const authConfig = loadAuthConfig();

  // When auth is enabled, we need to perform OIDC discovery at startup.
  // This fetches the provider's endpoints (authorize, token, userinfo) from
  // its .well-known/openid-configuration URL. It's async, hence the start() wrapper.
  if (authConfig) {
    initSessions(authConfig.sessionSecret);
    console.log(`OIDC discovery: ${authConfig.issuerUrl} ...`);
    await initOidc(authConfig);
    console.log("OIDC discovery complete");
  }

  const app = new Hono();

  app.use("*", logger());
  app.use("*", cors());
  app.onError(errorHandler);

  // Auth middleware — must come before API routes
  app.use("*", createAuthMiddleware());

  // Auth routes
  app.route("/api/auth", createAuthRoutes(authConfig));

  // API routes
  app.route("/api", api);

  // In production, serve the frontend static files
  if (process.env.NODE_ENV === "production") {
    app.use(
      "/*",
      serveStatic({ root: "./frontend/dist" })
    );
    // SPA fallback
    app.get("*", serveStatic({ root: "./frontend/dist", path: "index.html" }));
  }

  const port = parseInt(process.env.PORT ?? "3001", 10);

  console.log(`Auth: ${authEnabled ? "enabled" : "disabled"}`);
  console.log(`Backend listening on http://localhost:${port}`);
  const server = serve({ fetch: app.fetch, port });

  // Attach WebSocket upgrade handler for terminal exec
  (server as import("node:http").Server).on("upgrade", (req, socket, head) => {
    if (!handleUpgrade(req, socket, head)) {
      socket.destroy();
    }
  });
}

start().catch((err) => {
  console.error("Failed to start:", err);
  process.exit(1);
});
