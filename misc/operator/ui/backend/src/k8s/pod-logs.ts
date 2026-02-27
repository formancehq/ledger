import * as k8s from "@kubernetes/client-node";
import type { Writable } from "node:stream";
import { kubeConfig } from "./client.js";

export interface PodLogParams {
  namespace: string;
  podName: string;
  container?: string;
  follow?: boolean;
  tailLines?: number;
  timestamps?: boolean;
  previous?: boolean;
}

export function streamPodLogs(params: PodLogParams, output: Writable): void {
  const log = new k8s.Log(kubeConfig);
  log.log(
    params.namespace,
    params.podName,
    params.container ?? "",
    output,
    {
      follow: params.follow ?? false,
      tailLines: params.tailLines ?? 1000,
      timestamps: params.timestamps ?? false,
      previous: params.previous ?? false,
    }
  );
}
