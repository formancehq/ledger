import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";

// Maps SSE event names to the TanStack Query keys they should invalidate.
function queryKeysForEvent(
  event: string,
  namespace: string
): string[][] {
  switch (event) {
    case "ledger-services":
      return [
        ["ledger-services", namespace],
        // Also invalidate any open detail page in this namespace
        ["ledger-service"],
      ];
    case "pods":
    case "events":
      // Pod and event changes affect the detail page
      return [["ledger-service"]];
    case "ledger-defaults":
      return [["ledger-defaults"]];
    default:
      return [];
  }
}

/**
 * Opens an SSE connection to the backend watch endpoint for a given namespace.
 * When K8s resources change, it invalidates the relevant TanStack Query caches,
 * triggering a refetch of only the data that changed.
 *
 * This replaces polling (refetchInterval) with push-based updates.
 */
export function useWatch(namespace: string | undefined) {
  const queryClient = useQueryClient();
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!namespace) return;

    const url = `/api/watch/namespaces/${namespace}`;
    const es = new EventSource(url);
    eventSourceRef.current = es;

    const handleEvent = (event: MessageEvent) => {
      const keys = queryKeysForEvent(event.type, namespace);
      for (const key of keys) {
        queryClient.invalidateQueries({ queryKey: key });
      }
    };

    // Listen to all the event types the backend emits
    es.addEventListener("ledger-services", handleEvent);
    es.addEventListener("pods", handleEvent);
    es.addEventListener("events", handleEvent);
    es.addEventListener("ledger-defaults", handleEvent);

    es.onerror = () => {
      // EventSource will auto-reconnect. Nothing to do here.
    };

    return () => {
      es.close();
      eventSourceRef.current = null;
    };
  }, [namespace, queryClient]);
}
