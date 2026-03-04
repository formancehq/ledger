import type { KeyBundle } from "shared";
import { coreApi } from "./client.js";

/**
 * Read the Ed25519 key bundle from an agent's Secret.
 * Returns the same JSON format as `kubectl-ledger agents get-key --bundle`.
 */
export async function getAgentKeyBundle(
  namespace: string,
  secretName: string,
  scopes: string[],
  subject: string,
): Promise<KeyBundle> {
  const secret = await coreApi.readNamespacedSecret({ namespace, name: secretName });

  const decode = (key: string): string => {
    const raw = secret.data?.[key];
    if (!raw) return "";
    return Buffer.from(raw, "base64").toString("utf-8");
  };

  return {
    signingKey: decode("seed.hex"),
    keyId: decode("key-id"),
    scopes,
    subject,
  };
}
