import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import {
  k8sWatch,
  CRD_GROUP,
  CRD_VERSION,
  LEDGER_SERVICE_PLURAL,
  LEDGER_DEFAULTS_PLURAL,
} from "../k8s/client.js";

// Debounce: collapse multiple rapid K8s events into a single SSE push.
// K8s watches fire a burst of events on connect (the full state sync), and
// during rollouts pods flip through several states in quick succession.
const DEBOUNCE_MS = 500;

interface WatchTarget {
  path: string;
  params: Record<string, string>;
  event: string;
}

function namespacedTargets(ns: string): WatchTarget[] {
  return [
    {
      path: `/apis/${CRD_GROUP}/${CRD_VERSION}/namespaces/${ns}/${LEDGER_SERVICE_PLURAL}`,
      params: {},
      event: "ledger-services",
    },
    {
      path: `/api/v1/namespaces/${ns}/pods`,
      params: { labelSelector: "app.kubernetes.io/name=ledger" },
      event: "pods",
    },
    {
      path: `/api/v1/namespaces/${ns}/events`,
      params: {},
      event: "events",
    },
  ];
}

function clusterTargets(): WatchTarget[] {
  return [
    {
      path: `/apis/${CRD_GROUP}/${CRD_VERSION}/${LEDGER_DEFAULTS_PLURAL}`,
      params: {},
      event: "ledger-defaults",
    },
  ];
}

const app = new Hono();

// SSE stream for a namespace: watches CRDs + pods + events
app.get("/namespaces/:ns", (c) => {
  const ns = c.req.param("ns");
  const targets = [...namespacedTargets(ns), ...clusterTargets()];

  return streamSSE(c, async (stream) => {
    const abortControllers: AbortController[] = [];
    let closed = false;

    // When the client disconnects, clean up all watches
    stream.onAbort(() => {
      closed = true;
      for (const ac of abortControllers) {
        ac.abort();
      }
    });

    // Send initial keepalive
    await stream.writeSSE({ event: "connected", data: JSON.stringify({ namespace: ns }) });

    for (const target of targets) {
      // Per-target debounce timer
      let timer: ReturnType<typeof setTimeout> | null = null;

      const startWatch = () => {
        if (closed) return;

        k8sWatch
          .watch(
            target.path,
            target.params,
            (phase, _apiObj) => {
              if (closed) return;

              // Debounce: reset the timer on each event, only emit after DEBOUNCE_MS of silence
              if (timer) clearTimeout(timer);
              timer = setTimeout(() => {
                if (closed) return;
                stream
                  .writeSSE({
                    event: target.event,
                    data: JSON.stringify({ phase, timestamp: Date.now() }),
                  })
                  .catch(() => {
                    // Client disconnected, ignore write errors
                  });
              }, DEBOUNCE_MS);
            },
            (err) => {
              if (closed) return;
              if (err) {
                console.error(`Watch ${target.event} error:`, err.message ?? err);
              }
              // K8s watches close periodically (resourceVersion too old, timeout, etc.)
              // Reconnect after a short delay
              setTimeout(startWatch, 2000);
            }
          )
          .then((ac) => {
            abortControllers.push(ac);
          })
          .catch((err) => {
            if (closed) return;
            console.error(`Watch ${target.event} setup failed:`, err.message ?? err);
            setTimeout(startWatch, 5000);
          });
      };

      startWatch();
    }

    // Keep the stream open with a heartbeat every 30s
    while (!closed) {
      await stream.writeSSE({ event: "heartbeat", data: "" });
      await stream.sleep(30_000);
    }
  });
});

export default app;
