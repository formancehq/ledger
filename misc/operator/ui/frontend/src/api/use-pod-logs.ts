import { useState, useRef, useCallback, useEffect } from "react";

const MAX_LINES = 10_000;

export interface UsePodLogsOptions {
  namespace: string;
  pod: string;
  container?: string;
  follow?: boolean;
  tailLines?: number;
  timestamps?: boolean;
  previous?: boolean;
}

export function usePodLogs(options: UsePodLogsOptions) {
  const [lines, setLines] = useState<string[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  const stop = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setIsStreaming(false);
  }, []);

  const clear = useCallback(() => {
    setLines([]);
  }, []);

  const start = useCallback(() => {
    stop();

    const ac = new AbortController();
    abortRef.current = ac;
    setIsStreaming(true);

    const params = new URLSearchParams();
    if (options.container) params.set("container", options.container);
    if (options.follow) params.set("follow", "true");
    if (options.tailLines != null) params.set("tailLines", String(options.tailLines));
    if (options.timestamps) params.set("timestamps", "true");
    if (options.previous) params.set("previous", "true");

    const url = `/api/namespaces/${options.namespace}/pods/${options.pod}/logs?${params}`;

    fetch(url, { signal: ac.signal })
      .then(async (res) => {
        if (!res.ok || !res.body) {
          setIsStreaming(false);
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const parts = buffer.split("\n");
          // Keep the last incomplete chunk in the buffer
          buffer = parts.pop() ?? "";

          if (parts.length > 0) {
            setLines((prev) => {
              const next = [...prev, ...parts];
              return next.length > MAX_LINES ? next.slice(-MAX_LINES) : next;
            });
          }
        }

        // Flush remaining buffer
        if (buffer) {
          setLines((prev) => {
            const next = [...prev, buffer];
            return next.length > MAX_LINES ? next.slice(-MAX_LINES) : next;
          });
        }

        setIsStreaming(false);
      })
      .catch((err) => {
        if (err.name !== "AbortError") {
          console.error("Pod logs fetch error:", err);
        }
        setIsStreaming(false);
      });
  }, [options.namespace, options.pod, options.container, options.follow, options.tailLines, options.timestamps, options.previous, stop]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  return { lines, isStreaming, start, stop, clear };
}
