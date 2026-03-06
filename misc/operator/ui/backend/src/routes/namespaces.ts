import { Hono } from "hono";
import { coreApi } from "../k8s/client.js";
import { getAllowedNamespaces } from "../middleware/namespace-guard.js";

const app = new Hono();

app.get("/", async (c) => {
  const res = await coreApi.listNamespace();
  const allowed = getAllowedNamespaces();
  const namespaces = (res.items ?? [])
    .map((ns) => ({
      name: ns.metadata?.name ?? "",
      status: ns.status?.phase ?? "Unknown",
    }))
    .filter((ns) => !allowed || allowed.has(ns.name));
  return c.json(namespaces);
});

export default app;
