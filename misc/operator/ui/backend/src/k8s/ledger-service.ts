import type { LedgerService } from "shared";
import {
  customApi,
  CRD_GROUP,
  CRD_VERSION,
  LEDGER_SERVICE_PLURAL,
} from "./client.js";

export async function listLedgerServices(
  namespace: string
): Promise<LedgerService[]> {
  const res = await customApi.listNamespacedCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    namespace,
    plural: LEDGER_SERVICE_PLURAL,
  });
  return ((res as any).items ?? []) as LedgerService[];
}

export async function getLedgerService(
  namespace: string,
  name: string
): Promise<LedgerService> {
  const res = await customApi.getNamespacedCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    namespace,
    plural: LEDGER_SERVICE_PLURAL,
    name,
  });
  return res as unknown as LedgerService;
}

export async function createLedgerService(
  namespace: string,
  body: Record<string, unknown>
): Promise<LedgerService> {
  const res = await customApi.createNamespacedCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    namespace,
    plural: LEDGER_SERVICE_PLURAL,
    body: {
      apiVersion: `${CRD_GROUP}/${CRD_VERSION}`,
      kind: "LedgerService",
      ...body,
    },
  });
  return res as unknown as LedgerService;
}

export async function deleteLedgerService(
  namespace: string,
  name: string
): Promise<void> {
  await customApi.deleteNamespacedCustomObject({
    group: CRD_GROUP,
    version: CRD_VERSION,
    namespace,
    plural: LEDGER_SERVICE_PLURAL,
    name,
  });
}

export async function patchLedgerService(
  namespace: string,
  name: string,
  patch: Record<string, unknown>
): Promise<LedgerService> {
  const res = await customApi.patchNamespacedCustomObject(
    {
      group: CRD_GROUP,
      version: CRD_VERSION,
      namespace,
      plural: LEDGER_SERVICE_PLURAL,
      name,
      body: patch,
    },
    { headers: { "Content-Type": "application/merge-patch+json" } },
  );
  return res as unknown as LedgerService;
}
