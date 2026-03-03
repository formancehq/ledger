/**
 * Auth configuration — reads environment variables and validates them.
 *
 * Authentication is **entirely optional** and controlled by the AUTH_ENABLED
 * env var. When disabled (the default), the rest of the auth module is a no-op
 * and the app behaves exactly as before.
 *
 * Environment variables:
 *
 *  AUTH_ENABLED          "true" to enable (default: "false")
 *  AUTH_ISSUER_URL       OIDC provider URL (e.g. "https://accounts.google.com")
 *  AUTH_CLIENT_ID        OAuth client ID from your OIDC provider
 *  AUTH_CLIENT_SECRET    OAuth client secret (keep it secret!)
 *  AUTH_SESSION_SECRET   Random string (min 32 chars) used to sign session cookies
 *                        Generate one with: openssl rand -hex 32
 *  AUTH_REDIRECT_URI     Where the OIDC provider redirects after login
 *                        (default: http://localhost:<PORT>/api/auth/callback)
 *  AUTH_SCOPES           OAuth scopes to request (default: "openid profile email")
 *  AUTH_POST_LOGOUT_REDIRECT_URI  Where to go after logout (default: "/")
 */

export interface AuthConfig {
  issuerUrl: string;
  clientId: string;
  clientSecret: string;
  sessionSecret: string;
  redirectUri: string;
  scopes: string;
  postLogoutRedirectUri: string;
  roleMapping: Record<string, string>;
}

/** true when the AUTH_ENABLED env var is set to "true" (case-insensitive). */
export const authEnabled: boolean =
  (process.env.AUTH_ENABLED ?? "false").toLowerCase() === "true";

/**
 * Build and validate the auth configuration from environment variables.
 * Returns null when auth is disabled, throws on missing/invalid values.
 */
export function loadAuthConfig(): AuthConfig | null {
  if (!authEnabled) return null;

  const issuerUrl = process.env.AUTH_ISSUER_URL;
  const clientId = process.env.AUTH_CLIENT_ID;
  const clientSecret = process.env.AUTH_CLIENT_SECRET;
  const sessionSecret = process.env.AUTH_SESSION_SECRET;

  if (!issuerUrl) throw new Error("AUTH_ISSUER_URL is required when AUTH_ENABLED=true");
  if (!clientId) throw new Error("AUTH_CLIENT_ID is required when AUTH_ENABLED=true");
  if (!clientSecret) throw new Error("AUTH_CLIENT_SECRET is required when AUTH_ENABLED=true");
  if (!sessionSecret) throw new Error("AUTH_SESSION_SECRET is required when AUTH_ENABLED=true");
  if (sessionSecret.length < 32) {
    throw new Error("AUTH_SESSION_SECRET must be at least 32 characters");
  }

  const port = process.env.PORT ?? "3001";
  const redirectUri =
    process.env.AUTH_REDIRECT_URI ?? `http://localhost:${port}/api/auth/callback`;
  const scopes = process.env.AUTH_SCOPES ?? "openid profile email";
  const postLogoutRedirectUri = process.env.AUTH_POST_LOGOUT_REDIRECT_URI ?? "/";

  let roleMapping: Record<string, string> = {};
  const rawRoleMapping = process.env.AUTH_ROLE_MAPPING;
  if (rawRoleMapping) {
    try {
      roleMapping = JSON.parse(rawRoleMapping);
    } catch {
      console.warn("AUTH_ROLE_MAPPING is not valid JSON, ignoring");
    }
  }

  return {
    issuerUrl,
    clientId,
    clientSecret,
    sessionSecret,
    redirectUri,
    scopes,
    postLogoutRedirectUri,
    roleMapping,
  };
}
