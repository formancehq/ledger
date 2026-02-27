/**
 * OIDC client initialization.
 *
 * At startup, discovery() fetches the provider's .well-known/openid-configuration
 * (e.g. https://accounts.google.com/.well-known/openid-configuration) to learn
 * the authorize, token, and userinfo endpoints automatically. This means you
 * only need to provide the issuer URL, not every individual endpoint.
 */

import * as client from "openid-client";
import type { AuthConfig } from "./config.js";

let oidcConfig: client.Configuration;

/** Perform OIDC discovery — must be called once at startup. */
export async function initOidc(config: AuthConfig): Promise<void> {
  oidcConfig = await client.discovery(
    new URL(config.issuerUrl),
    config.clientId,
    config.clientSecret,
  );
}

export function getOidcConfig(): client.Configuration {
  if (!oidcConfig) {
    throw new Error("OIDC not initialized — call initOidc() first");
  }
  return oidcConfig;
}
