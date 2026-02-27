import { Hono } from "hono";
import { stream } from "hono/streaming";
import { PassThrough } from "node:stream";
import { streamPodLogs } from "../k8s/pod-logs.js";

const app = new Hono();

app.get("/namespaces/:ns/pods/:pod/logs", (c) => {
  const ns = c.req.param("ns");
  const pod = c.req.param("pod");
  const container = c.req.query("container") || undefined;
  const follow = c.req.query("follow") === "true";
  const tailLines = parseInt(c.req.query("tailLines") ?? "1000", 10);
  const timestamps = c.req.query("timestamps") === "true";
  const previous = c.req.query("previous") === "true";

  return stream(c, async (honoStream) => {
    const passthrough = new PassThrough();

    honoStream.onAbort(() => {
      passthrough.destroy();
    });

    streamPodLogs(
      { namespace: ns, podName: pod, container, follow, tailLines, timestamps, previous },
      passthrough
    );

    passthrough.on("data", async (chunk: Buffer) => {
      try {
        await honoStream.write(chunk);
      } catch {
        // Client disconnected
        passthrough.destroy();
      }
    });

    await new Promise<void>((resolve) => {
      passthrough.on("end", resolve);
      passthrough.on("close", resolve);
      passthrough.on("error", resolve);
    });
  });
});

export default app;
