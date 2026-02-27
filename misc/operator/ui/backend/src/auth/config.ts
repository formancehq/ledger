export interface AuthConfig {
  issuerUrl: string;
  clientId: string;
  clientSecret: string;
  sessionSecret: string;
  redirectUri: string;
  scopes: string;
  postLogoutRedirectUri: string;
}

export const authEnabled: boolean =
  (process.env.AUTH_ENABLED ?? "false").toLowerCase() === "true";

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

  return {
    issuerUrl,
    clientId,
    clientSecret,
    sessionSecret,
    redirectUri,
    scopes,
    postLogoutRedirectUri,
  };
}
