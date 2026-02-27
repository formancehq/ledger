import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import api from "./routes/index.js";
import { errorHandler } from "./middleware/error-handler.js";
import { handleUpgrade } from "./ws/terminal.js";

const app = new Hono();

app.use("*", logger());
app.use("*", cors());
app.onError(errorHandler);

// API routes
app.route("/api", api);

// In production, serve the frontend static files
if (process.env.NODE_ENV === "production") {
  app.use(
    "/*",
    serveStatic({ root: "../frontend/dist" })
  );
  // SPA fallback
  app.get("*", serveStatic({ root: "../frontend/dist", path: "index.html" }));
}

const port = parseInt(process.env.PORT ?? "3001", 10);

console.log(`Backend listening on http://localhost:${port}`);
const server = serve({ fetch: app.fetch, port });

// Attach WebSocket upgrade handler for terminal exec
(server as import("node:http").Server).on("upgrade", (req, socket, head) => {
  if (!handleUpgrade(req, socket, head)) {
    socket.destroy();
  }
});
