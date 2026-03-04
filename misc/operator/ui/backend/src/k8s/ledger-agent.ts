import {
  customApi,
  CRD_GROUP,
  CRD_VERSION,
  LEDGER_AGENT_PLURAL,
} from "./client.js";

export interface LedgerAgent {
  apiVersion?: string;
  kind?: string;
  metadata: {
    name: string;
    namespace?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    scopes: string[];
    selector: {
      matchLabels: Record<string, string>;
    };
  };
  status?: {
    phase?: string;
    keyID?: string;
    secretRef?: { namespace: string; name: string };
  };
}

export async function getLedgerAgent(
  namespace: string,
  name: string,
): Promise<LedgerAgent | null> {
  try {
    const res = await customApi.getNamespacedCustomObject({
      group: CRD_GROUP,
      version: CRD_VERSION,
      namespace,
      plural: LEDGER_AGENT_PLURAL,
      name,
    });
    return res as unknown as LedgerAgent;
  } catch (err: unknown) {
    if (isNotFound(err)) return null;
    throw err;
  }
}

export async function createLedgerAgent(
  namespace: string,
  body: Record<string, unknown>,
): Promise<LedgerAgent> {
  const res = await customApi.createNamespacedCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    namespace,
    plural: LEDGER_AGENT_PLURAL,
    body: {
      apiVersion: `${CRD_GROUP}/${CRD_VERSION}`,
      kind: "LedgerAgent",
      ...body,
    },
  });
  return res as unknown as LedgerAgent;
}

function isNotFound(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  // @kubernetes/client-node v1.x throws ApiException with a `code` property.
  if ("code" in err && (err as Error & { code: number }).code === 404) return true;
  return false;
}
