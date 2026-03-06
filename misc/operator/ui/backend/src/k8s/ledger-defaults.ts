import type { LedgerDefaults } from "shared";
import {
  customApi,
  CRD_GROUP,
  CRD_VERSION,
  LEDGER_DEFAULTS_PLURAL,
} from "./client.js";

export async function listLedgerDefaults(): Promise<LedgerDefaults[]> {
  const res = await customApi.listClusterCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    plural: LEDGER_DEFAULTS_PLURAL,
  });
  return ((res as any).items ?? []) as LedgerDefaults[];
}

export async function getLedgerDefaults(
  name: string
): Promise<LedgerDefaults> {
  const res = await customApi.getClusterCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    plural: LEDGER_DEFAULTS_PLURAL,
    name,
  });
  return res as unknown as LedgerDefaults;
}

export async function createLedgerDefaults(
  body: Record<string, unknown>
): Promise<LedgerDefaults> {
  const res = await customApi.createClusterCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    plural: LEDGER_DEFAULTS_PLURAL,
    body: {
      apiVersion: `${CRD_GROUP}/${CRD_VERSION}`,
      kind: "LedgerDefaults",
      ...body,
    },
  });
  return res as unknown as LedgerDefaults;
}

export async function deleteLedgerDefaults(name: string): Promise<void> {
  await customApi.deleteClusterCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    plural: LEDGER_DEFAULTS_PLURAL,
    name,
  });
}

export async function patchLedgerDefaults(
  name: string,
  patch: Record<string, unknown>
): Promise<LedgerDefaults> {
  const res = await customApi.patchClusterCustomObject(
    {
      group: CRD_GROUP,
      version: CRD_VERSION,
      plural: LEDGER_DEFAULTS_PLURAL,
      name,
      body: patch,
    },
    { headers: { "Content-Type": "application/merge-patch+json" } },
  );
  return res as unknown as LedgerDefaults;
}
