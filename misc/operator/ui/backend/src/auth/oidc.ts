import * as client from "openid-client";
import type { AuthConfig } from "./config.js";

let oidcConfig: client.Configuration;

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
