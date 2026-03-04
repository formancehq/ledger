import { Hono } from "hono";
import {
  listLedgerServices,
  getLedgerService,
  createLedgerService,
  deleteLedgerService,
  patchLedgerService,
} from "../k8s/ledger-service.js";
import { getLedgerAgent, createLedgerAgent } from "../k8s/ledger-agent.js";
import { listPods, listPvcs, listServices, listEvents } from "../k8s/resources.js";
import type { AuthEnv } from "../auth/middleware.js";
import type { SessionData } from "../auth/session.js";

const app = new Hono<AuthEnv>();

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

  // Guests must select a pre-registered configuration
  const session = c.get("session");
  if (session?.role === "guest" && !body.spec?.defaultsRef) {
    return c.json(
      { error: { message: "Forbidden: guests must select a configuration (defaultsRef)", requiredRole: "admin" } },
      403
    );
  }

  // Annotate and label with owner info from the authenticated session (if any)
  const ownerLabel = session ? sanitizeK8sLabelValue(session.userId) : undefined;
  if (session) {
    body.metadata ??= {};
    body.metadata.annotations = {
      ...body.metadata.annotations,
      "ledger.formance.com/created-by": session.userId,
      ...(session.email && { "ledger.formance.com/created-by-email": session.email }),
    };
    body.metadata.labels = {
      ...body.metadata.labels,
      ...(ownerLabel && { "ledger.formance.com/owner": ownerLabel }),
    };
  }

  const result = await createLedgerService(ns, body);

  // Create a LedgerAgent for the authenticated user so they can immediately
  // interact with the cluster using Ed25519 request signing.
  if (session && ownerLabel) {
    await ensureLedgerAgentForUser(ns, session, ownerLabel);
  }

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

  // Guests can only scale up to 5 replicas
  const session = c.get("session");
  if (session?.role === "guest" && replicas > 5) {
    return c.json(
      { error: { message: "Forbidden: guests can scale to a maximum of 5 replicas", requiredRole: "admin" } },
      403
    );
  }

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

/**
 * Sanitize a string for use as a Kubernetes label value.
 * Label values must be 63 chars or less, alphanumeric + [-_.], and must
 * start and end with an alphanumeric character.
 */
function sanitizeK8sLabelValue(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "-")   // replace invalid chars
    .replace(/^[^a-z0-9]+/, "")       // strip leading non-alphanum
    .replace(/[^a-z0-9]+$/, "")       // strip trailing non-alphanum
    .slice(0, 63);
}

/**
 * Sanitize a string for use as a Kubernetes resource name.
 * Names must be lowercase alphanumeric + hyphens, max 253 chars,
 * start and end with alphanumeric.
 */
function sanitizeK8sName(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")      // replace invalid chars with hyphens
    .replace(/-+/g, "-")              // collapse multiple hyphens
    .replace(/^-+/, "")               // strip leading hyphens
    .replace(/-+$/, "")               // strip trailing hyphens
    .slice(0, 253);
}

/**
 * Ensure a LedgerAgent exists for the given user in the namespace.
 * The agent matches all LedgerServices labeled with the user's owner label.
 * If the agent already exists, this is a no-op.
 */
async function ensureLedgerAgentForUser(
  namespace: string,
  session: SessionData,
  ownerLabel: string,
): Promise<void> {
  const agentName = sanitizeK8sName(`user-${ownerLabel}`);
  if (!agentName) return;

  // Check if agent already exists (idempotent)
  const existing = await getLedgerAgent(namespace, agentName);
  if (existing) return;

  await createLedgerAgent(namespace, {
    metadata: {
      name: agentName,
      annotations: {
        "ledger.formance.com/created-by": session.userId,
        ...(session.email && { "ledger.formance.com/created-by-email": session.email }),
      },
    },
    spec: {
      scopes: ["read", "write"],
      selector: {
        matchLabels: {
          "ledger.formance.com/owner": ownerLabel,
        },
      },
    },
  });
}

export default app;
