import { useEffect, useRef } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { WebLinksAddon } from "@xterm/addon-web-links";
import "@xterm/xterm/css/xterm.css";

// Binary protocol matching backend channels
const CH_STDIN = 0;
const CH_STDOUT = 1;
const CH_STDERR = 2;
const CH_RESIZE = 3;

interface PodTerminalProps {
  namespace: string;
  pod: string;
  container: string;
}

export function PodTerminal({ namespace, pod, container }: PodTerminalProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const termRef = useRef<Terminal | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const term = new Terminal({
      cursorBlink: true,
      fontFamily: "monospace",
      fontSize: 14,
      theme: {
        background: "#1e1e1e",
        foreground: "#d4d4d4",
      },
    });
    termRef.current = term;

    const fitAddon = new FitAddon();
    fitAddonRef.current = fitAddon;
    term.loadAddon(fitAddon);
    term.loadAddon(new WebLinksAddon());

    term.open(containerRef.current);
    fitAddon.fit();

    // Connect WebSocket
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const params = new URLSearchParams();
    if (container) params.set("container", container);
    const wsUrl = `${protocol}//${window.location.host}/api/ws/namespaces/${namespace}/pods/${pod}/exec?${params}`;
    const ws = new WebSocket(wsUrl);
    ws.binaryType = "arraybuffer";
    wsRef.current = ws;

    ws.onopen = () => {
      // Send initial resize
      const resizePayload = JSON.stringify({ cols: term.cols, rows: term.rows });
      const buf = new Uint8Array(1 + resizePayload.length);
      buf[0] = CH_RESIZE;
      new TextEncoder().encodeInto(resizePayload, buf.subarray(1));
      ws.send(buf);
    };

    ws.onmessage = (event: MessageEvent) => {
      const data = new Uint8Array(event.data as ArrayBuffer);
      if (data.length < 1) return;
      const channel = data[0];
      const payload = data.subarray(1);
      if (channel === CH_STDOUT || channel === CH_STDERR) {
        term.write(payload);
      }
    };

    ws.onclose = () => {
      term.write("\r\n\x1b[90m--- Session closed ---\x1b[0m\r\n");
    };

    ws.onerror = () => {
      term.write("\r\n\x1b[31m--- Connection error ---\x1b[0m\r\n");
    };

    // Send keystrokes via stdin channel
    term.onData((data) => {
      if (ws.readyState === WebSocket.OPEN) {
        const encoded = new TextEncoder().encode(data);
        const buf = new Uint8Array(1 + encoded.length);
        buf[0] = CH_STDIN;
        buf.set(encoded, 1);
        ws.send(buf);
      }
    });

    // Send resize on terminal resize
    term.onResize(({ cols, rows }) => {
      if (ws.readyState === WebSocket.OPEN) {
        const resizePayload = JSON.stringify({ cols, rows });
        const buf = new Uint8Array(1 + resizePayload.length);
        buf[0] = CH_RESIZE;
        new TextEncoder().encodeInto(resizePayload, buf.subarray(1));
        ws.send(buf);
      }
    });

    // ResizeObserver for container size changes
    const observer = new ResizeObserver(() => {
      fitAddon.fit();
    });
    observer.observe(containerRef.current);

    return () => {
      observer.disconnect();
      ws.close();
      term.dispose();
      termRef.current = null;
      wsRef.current = null;
      fitAddonRef.current = null;
    };
  }, [namespace, pod, container]);

  return (
    <div
      ref={containerRef}
      className="w-full h-full"
      style={{ minHeight: "300px" }}
    />
  );
}
