import { Hono } from "hono";
import namespaces from "./namespaces.js";
import ledgerServices from "./ledger-services.js";
import ledgerDefaults from "./ledger-defaults.js";
import watch from "./watch.js";

const api = new Hono();

api.get("/health", (c) => c.json({ status: "ok" }));

api.route("/namespaces", namespaces);
api.route("/", ledgerServices);
api.route("/", ledgerDefaults);
api.route("/watch", watch);

export default api;
