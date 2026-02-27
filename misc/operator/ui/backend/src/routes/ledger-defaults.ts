import { Hono } from "hono";
import {
  listLedgerDefaults,
  getLedgerDefaults,
  createLedgerDefaults,
  deleteLedgerDefaults,
  patchLedgerDefaults,
} from "../k8s/ledger-defaults.js";
import { listLedgerServices } from "../k8s/ledger-service.js";
import { coreApi } from "../k8s/client.js";
import type { LedgerService } from "shared";

async function findReferencingServices(
  defaultsName: string
): Promise<Array<{ name: string; namespace: string }>> {
  const nsRes = await coreApi.listNamespace();
  const refs: Array<{ name: string; namespace: string }> = [];

  for (const ns of nsRes.items ?? []) {
    const nsName = ns.metadata?.name;
    if (!nsName) continue;
    try {
      const services = await listLedgerServices(nsName);
      for (const svc of services) {
        if ((svc as LedgerService).spec?.defaultsRef === defaultsName) {
          refs.push({
            name: (svc as LedgerService).metadata.name,
            namespace: nsName,
          });
        }
      }
    } catch {
      // skip namespaces where we can't list (permission errors, etc.)
    }
  }
  return refs;
}

const app = new Hono();

// List all LedgerDefaults with reference counts
app.get("/ledger-defaults", async (c) => {
  const defaults = await listLedgerDefaults();

  // Build reference counts by scanning all LedgerServices
  const nsRes = await coreApi.listNamespace();
  const allServices: LedgerService[] = [];
  for (const ns of nsRes.items ?? []) {
    const nsName = ns.metadata?.name;
    if (!nsName) continue;
    try {
      const services = await listLedgerServices(nsName);
      allServices.push(...services);
    } catch {
      // skip
    }
  }

  const items = defaults.map((d) => {
    const count = allServices.filter(
      (s) => s.spec?.defaultsRef === d.metadata.name
    ).length;
    return { ledgerDefaults: d, referencedByCount: count };
  });

  return c.json(items);
});

// Get single LedgerDefaults with referencing services
app.get("/ledger-defaults/:name", async (c) => {
  const name = c.req.param("name");
  const [ledgerDefaults, referencedBy] = await Promise.all([
    getLedgerDefaults(name),
    findReferencingServices(name),
  ]);
  return c.json({ ledgerDefaults, referencedBy });
});

// Create LedgerDefaults
app.post("/ledger-defaults", async (c) => {
  const body = await c.req.json();
  const result = await createLedgerDefaults(body);
  return c.json(result, 201);
});

// Patch LedgerDefaults
app.patch("/ledger-defaults/:name", async (c) => {
  const name = c.req.param("name");
  const body = await c.req.json();
  const result = await patchLedgerDefaults(name, body);
  return c.json(result);
});

// Delete LedgerDefaults
app.delete("/ledger-defaults/:name", async (c) => {
  const name = c.req.param("name");
  await deleteLedgerDefaults(name);
  return c.json({ ok: true });
});

export default app;
