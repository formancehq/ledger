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

async function listAllServices(): Promise<LedgerService[]> {
  const nsRes = await coreApi.listNamespace();
  const namespaces = (nsRes.items ?? [])
    .map((ns) => ns.metadata?.name)
    .filter(Boolean) as string[];

  const results = await Promise.allSettled(
    namespaces.map((ns) => listLedgerServices(ns))
  );

  return results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
}

function findReferencingServices(
  allServices: LedgerService[],
  defaultsName: string
): Array<{ name: string; namespace: string }> {
  return allServices
    .filter((svc) => svc.spec?.defaultsRef === defaultsName)
    .map((svc) => ({
      name: svc.metadata.name,
      namespace: svc.metadata.namespace ?? "",
    }));
}

const app = new Hono();

// List all LedgerDefaults with reference counts
app.get("/ledger-defaults", async (c) => {
  const [defaults, allServices] = await Promise.all([
    listLedgerDefaults(),
    listAllServices(),
  ]);

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
  const [ledgerDefaults, allServices] = await Promise.all([
    getLedgerDefaults(name),
    listAllServices(),
  ]);
  const referencedBy = findReferencingServices(allServices, name);
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
