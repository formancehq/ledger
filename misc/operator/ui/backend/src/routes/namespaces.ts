import { Hono } from "hono";
import { coreApi } from "../k8s/client.js";

const app = new Hono();

app.get("/", async (c) => {
  const res = await coreApi.listNamespace();
  const namespaces = (res.items ?? []).map((ns) => ({
    name: ns.metadata?.name ?? "",
    status: ns.status?.phase ?? "Unknown",
  }));
  return c.json(namespaces);
});

export default app;
