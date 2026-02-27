import { Hono } from "hono";
import * as client from "openid-client";
import { authEnabled } from "./config.js";
import type { AuthConfig } from "./config.js";
import { getOidcConfig } from "./oidc.js";
import {
  createSession,
  getSession,
  deleteSessionFromCookie,
  getCookieName,
} from "./session.js";

// In-memory state store for CSRF protection (state → timestamp)
const pendingStates = new Map<string, number>();

// Clean up expired states (5 min TTL)
const STATE_TTL_MS = 5 * 60 * 1000;

function cleanExpiredStates(): void {
  const now = Date.now();
  for (const [state, ts] of pendingStates) {
    if (now - ts > STATE_TTL_MS) pendingStates.delete(state);
  }
}

export function createAuthRoutes(config: AuthConfig | null): Hono {
  const app = new Hono();

  // GET /me — always accessible
  app.get("/me", (c) => {
    if (!authEnabled || !config) {
      return c.json({ enabled: false });
    }
    const session = getSession(c.req.header("cookie"));
    if (!session) {
      return c.json({ enabled: true, authenticated: false }, 401);
    }
    return c.json({
      enabled: true,
      authenticated: true,
      user: {
        id: session.userId,
        email: session.email,
        name: session.name,
      },
    });
  });

  // GET /login — redirect to OIDC authorize endpoint
  app.get("/login", (c) => {
    if (!authEnabled || !config) {
      return c.redirect("/");
    }

    cleanExpiredStates();
    const state = client.randomState();
    pendingStates.set(state, Date.now());

    const redirectTo = client.buildAuthorizationUrl(getOidcConfig(), {
      redirect_uri: config.redirectUri,
      scope: config.scopes,
      state,
    });

    return c.redirect(redirectTo.href);
  });

  // GET /callback — exchange code for tokens
  app.get("/callback", async (c) => {
    if (!authEnabled || !config) {
      return c.redirect("/");
    }

    const callbackUrl = new URL(c.req.url);
    const state = callbackUrl.searchParams.get("state");

    if (!state || !pendingStates.has(state)) {
      return c.text("Invalid or expired state parameter", 400);
    }
    pendingStates.delete(state);

    try {
      const tokens = await client.authorizationCodeGrant(
        getOidcConfig(),
        callbackUrl,
        { expectedState: state },
      );

      const claims = tokens.claims();
      const session = createSession({
        userId: claims?.sub ?? "unknown",
        email: claims?.email as string | undefined,
        name: claims?.name as string | undefined,
        createdAt: Date.now(),
      });

      c.header(
        "Set-Cookie",
        `${session.cookieName}=${session.cookieValue}; Path=/; HttpOnly; SameSite=Lax`,
      );
      return c.redirect("/");
    } catch (err) {
      console.error("OIDC callback error:", err);
      return c.text("Authentication failed", 500);
    }
  });

  // POST /logout — destroy session, clear cookie
  app.post("/logout", (c) => {
    deleteSessionFromCookie(c.req.header("cookie"));
    const postLogoutUri = config?.postLogoutRedirectUri ?? "/";
    c.header(
      "Set-Cookie",
      `${getCookieName()}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`,
    );
    return c.json({ redirectTo: postLogoutUri });
  });

  return app;
}
