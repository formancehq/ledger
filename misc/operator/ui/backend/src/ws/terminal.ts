import { WebSocketServer, WebSocket } from "ws";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { PassThrough } from "node:stream";
import * as k8s from "@kubernetes/client-node";
import { kubeConfig } from "../k8s/client.js";

// Binary protocol over browser WebSocket:
// Byte 0 = channel: 0=stdin, 1=stdout, 2=stderr, 3=resize
// Bytes 1.. = payload
const CH_STDIN = 0;
const CH_STDOUT = 1;
const CH_STDERR = 2;
const CH_RESIZE = 3;

const wss = new WebSocketServer({ noServer: true });

// Parse URL pattern: /api/ws/namespaces/:ns/pods/:pod/exec?container=xxx
function parseExecUrl(url: string): { namespace: string; pod: string; container?: string } | null {
  const parsed = new URL(url, "http://localhost");
  const match = parsed.pathname.match(
    /^\/api\/ws\/namespaces\/([^/]+)\/pods\/([^/]+)\/exec$/
  );
  if (!match) return null;
  return {
    namespace: match[1],
    pod: match[2],
    container: parsed.searchParams.get("container") ?? undefined,
  };
}

wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
  const params = parseExecUrl(req.url ?? "");
  if (!params) {
    ws.close(1008, "Invalid URL");
    return;
  }

  const exec = new k8s.Exec(kubeConfig);

  const stdinStream = new PassThrough();
  const stdoutStream = new PassThrough();
  const stderrStream = new PassThrough();

  let execWebSocket: WebSocket | null = null;

  // Forward stdout from K8s → browser
  stdoutStream.on("data", (data: Buffer) => {
    if (ws.readyState === WebSocket.OPEN) {
      const msg = Buffer.alloc(1 + data.length);
      msg[0] = CH_STDOUT;
      data.copy(msg, 1);
      ws.send(msg);
    }
  });

  // Forward stderr from K8s → browser
  stderrStream.on("data", (data: Buffer) => {
    if (ws.readyState === WebSocket.OPEN) {
      const msg = Buffer.alloc(1 + data.length);
      msg[0] = CH_STDERR;
      data.copy(msg, 1);
      ws.send(msg);
    }
  });

  // Handle messages from browser → K8s
  ws.on("message", (rawData: Buffer) => {
    const data = Buffer.isBuffer(rawData) ? rawData : Buffer.from(rawData as ArrayBuffer);
    if (data.length < 1) return;

    const channel = data[0];
    const payload = data.subarray(1);

    switch (channel) {
      case CH_STDIN:
        stdinStream.write(payload);
        break;
      case CH_RESIZE: {
        try {
          const size = JSON.parse(payload.toString());
          if (execWebSocket && typeof size.cols === "number" && typeof size.rows === "number") {
            // K8s exec resize: channel 4, JSON payload
            const resizeMsg = JSON.stringify({ Width: size.cols, Height: size.rows });
            const buf = Buffer.alloc(1 + resizeMsg.length);
            buf[0] = 4; // K8s resize channel
            buf.write(resizeMsg, 1);
            if (execWebSocket.readyState === WebSocket.OPEN) {
              execWebSocket.send(buf);
            }
          }
        } catch {
          // Ignore malformed resize
        }
        break;
      }
    }
  });

  const cleanup = () => {
    stdinStream.destroy();
    stdoutStream.destroy();
    stderrStream.destroy();
    if (execWebSocket && execWebSocket.readyState === WebSocket.OPEN) {
      execWebSocket.close();
    }
  };

  ws.on("close", cleanup);
  ws.on("error", cleanup);

  exec
    .exec(
      params.namespace,
      params.pod,
      params.container ?? "",
      ["/bin/sh"],
      stdoutStream,
      stderrStream,
      stdinStream,
      true /* tty */
    )
    .then((wsConn) => {
      execWebSocket = wsConn as unknown as WebSocket;
      (wsConn as any).on("close", () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1000, "Session closed");
        }
      });
      (wsConn as any).on("error", () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1011, "Exec error");
        }
      });
    })
    .catch((err: Error) => {
      console.error("K8s exec failed:", err.message ?? err);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1011, `Exec failed: ${err.message ?? "unknown error"}`);
      }
    });
});

export function handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): boolean {
  const url = req.url ?? "";
  if (!url.startsWith("/api/ws/")) return false;

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
  return true;
}
