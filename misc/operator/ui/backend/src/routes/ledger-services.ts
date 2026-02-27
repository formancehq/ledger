import { Hono } from "hono";
import {
  listLedgerServices,
  getLedgerService,
  createLedgerService,
  deleteLedgerService,
  patchLedgerService,
} from "../k8s/ledger-service.js";
import { listPods, listPvcs, listServices, listEvents } from "../k8s/resources.js";

const app = new Hono();

// List LedgerServices in a namespace
app.get("/namespaces/:ns/ledger-services", async (c) => {
  const ns = c.req.param("ns");
  const items = await listLedgerServices(ns);
  return c.json(items);
});

// Get single LedgerService with related resources
app.get("/namespaces/:ns/ledger-services/:name", async (c) => {
  const { ns, name } = c.req.param();
  const [ledgerService, pods, pvcs, services, events] = await Promise.all([
    getLedgerService(ns, name),
    listPods(ns, name),
    listPvcs(ns, name),
    listServices(ns, name),
    listEvents(ns, name),
  ]);
  return c.json({ ledgerService, pods, pvcs, services, events });
});

// Create LedgerService
app.post("/namespaces/:ns/ledger-services", async (c) => {
  const ns = c.req.param("ns");
  const body = await c.req.json();
  const result = await createLedgerService(ns, body);
  return c.json(result, 201);
});

// Delete LedgerService
app.delete("/namespaces/:ns/ledger-services/:name", async (c) => {
  const { ns, name } = c.req.param();
  await deleteLedgerService(ns, name);
  return c.json({ ok: true });
});

// Scale LedgerService
app.patch("/namespaces/:ns/ledger-services/:name/scale", async (c) => {
  const { ns, name } = c.req.param();
  const { replicas } = await c.req.json<{ replicas: number }>();
  const result = await patchLedgerService(ns, name, {
    spec: { replicas },
  });
  return c.json(result);
});

// Patch LedgerService (generic spec update)
app.patch("/namespaces/:ns/ledger-services/:name", async (c) => {
  const { ns, name } = c.req.param();
  const body = await c.req.json();
  const result = await patchLedgerService(ns, name, body);
  return c.json(result);
});

// Restart LedgerService (rolling restart via pod annotation)
app.post("/namespaces/:ns/ledger-services/:name/restart", async (c) => {
  const { ns, name } = c.req.param();
  const result = await patchLedgerService(ns, name, {
    spec: {
      podAnnotations: {
        "ledger.formance.com/restartedAt": new Date().toISOString(),
      },
    },
  });
  return c.json(result);
});

export default app;
