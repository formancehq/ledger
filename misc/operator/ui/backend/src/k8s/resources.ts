import type { PodSummary, PvcSummary, ServiceSummary, EventSummary } from "shared";
import { coreApi } from "./client.js";
import type { V1Pod, V1ContainerStatus } from "@kubernetes/client-node";

const LABEL_NAME = "app.kubernetes.io/name";
const LABEL_INSTANCE = "app.kubernetes.io/instance";

function labelSelector(name: string): string {
  return `${LABEL_NAME}=ledger,${LABEL_INSTANCE}=${name}`;
}

// Compute the effective pod status the way kubectl does:
// instead of just showing the phase ("Pending"), surface the container-level
// reason such as "ImagePullBackOff", "CrashLoopBackOff", "ErrImagePull", etc.
function computePodStatus(pod: V1Pod): {
  status: string;
  reason?: string;
  message?: string;
} {
  const phase = pod.status?.phase ?? "Unknown";

  // Check init containers first (they run before regular containers)
  for (const cs of pod.status?.initContainerStatuses ?? []) {
    const waiting = cs.state?.waiting;
    if (waiting?.reason && waiting.reason !== "PodInitializing") {
      return {
        status: `Init:${waiting.reason}`,
        reason: waiting.reason,
        message: waiting.message,
      };
    }
    const terminated = cs.state?.terminated;
    if (terminated && terminated.exitCode !== 0) {
      return {
        status: `Init:Error`,
        reason: terminated.reason ?? "Error",
        message: terminated.message,
      };
    }
  }

  // Check regular container statuses
  for (const cs of pod.status?.containerStatuses ?? []) {
    const waiting = cs.state?.waiting;
    if (waiting?.reason) {
      return {
        status: waiting.reason,
        reason: waiting.reason,
        message: waiting.message,
      };
    }
    const terminated = cs.state?.terminated;
    if (terminated) {
      if (terminated.reason) {
        return {
          status: terminated.reason,
          reason: terminated.reason,
          message: terminated.message,
        };
      }
      if (terminated.signal) {
        return {
          status: `Signal:${terminated.signal}`,
          reason: `Signal:${terminated.signal}`,
          message: terminated.message,
        };
      }
      if (terminated.exitCode !== 0) {
        return {
          status: `ExitCode:${terminated.exitCode}`,
          reason: `ExitCode:${terminated.exitCode}`,
          message: terminated.message,
        };
      }
    }
  }

  // Check pod-level reason (e.g. Evicted, NodeLost)
  if (pod.status?.reason) {
    return {
      status: pod.status.reason,
      reason: pod.status.reason,
      message: pod.status.message,
    };
  }

  return { status: phase };
}

function countReady(statuses: V1ContainerStatus[]): number {
  let n = 0;
  for (const cs of statuses) {
    if (cs.ready) n++;
  }
  return n;
}

function countRestarts(statuses: V1ContainerStatus[]): number {
  let n = 0;
  for (const cs of statuses) {
    n += cs.restartCount ?? 0;
  }
  return n;
}

export async function listPods(
  namespace: string,
  name: string
): Promise<PodSummary[]> {
  const res = await coreApi.listNamespacedPod({
    namespace,
    labelSelector: labelSelector(name),
  });
  return (res.items ?? []).map((p) => {
    const total = p.spec?.containers?.length ?? 0;
    const containerStatuses = p.status?.containerStatuses ?? [];
    const { status, reason, message } = computePodStatus(p);
    return {
      name: p.metadata?.name ?? "",
      ready: `${countReady(containerStatuses)}/${total}`,
      status,
      reason,
      message,
      restarts: countRestarts(containerStatuses),
      age: p.metadata?.creationTimestamp
        ? new Date(p.metadata.creationTimestamp).toISOString()
        : undefined,
      node: p.spec?.nodeName,
      containers: (p.spec?.containers ?? []).map((c) => c.name),
    };
  });
}

export async function listPvcs(
  namespace: string,
  name: string
): Promise<PvcSummary[]> {
  const res = await coreApi.listNamespacedPersistentVolumeClaim({
    namespace,
    labelSelector: labelSelector(name),
  });
  return (res.items ?? []).map((pvc) => ({
    name: pvc.metadata?.name ?? "",
    status: pvc.status?.phase ?? "Unknown",
    capacity: pvc.status?.capacity?.["storage"] ?? undefined,
    storageClass: pvc.spec?.storageClassName ?? undefined,
    age: pvc.metadata?.creationTimestamp
      ? new Date(pvc.metadata.creationTimestamp).toISOString()
      : undefined,
  }));
}

export async function listServices(
  namespace: string,
  name: string
): Promise<ServiceSummary[]> {
  const res = await coreApi.listNamespacedService({
    namespace,
    labelSelector: labelSelector(name),
  });
  return (res.items ?? []).map((svc) => ({
    name: svc.metadata?.name ?? "",
    type: svc.spec?.type ?? "ClusterIP",
    clusterIP: svc.spec?.clusterIP ?? undefined,
    ports: (svc.spec?.ports ?? [])
      .map((p) => `${p.port}/${p.protocol ?? "TCP"}`)
      .join(", "),
    age: svc.metadata?.creationTimestamp
      ? new Date(svc.metadata.creationTimestamp).toISOString()
      : undefined,
  }));
}

export async function listEvents(
  namespace: string,
  name: string
): Promise<EventSummary[]> {
  // Fetch events related to pods matching this LedgerService's labels,
  // plus events targeting the StatefulSet itself.
  // Using fieldSelector on involvedObject is the most targeted approach,
  // but we also want pod events so we fetch all events and filter.
  const res = await coreApi.listNamespacedEvent({
    namespace,
  });

  const selectorLabels = labelSelector(name);

  // Also fetch pod names to match events to our pods
  const podRes = await coreApi.listNamespacedPod({
    namespace,
    labelSelector: selectorLabels,
  });
  const podNames = new Set(
    (podRes.items ?? []).map((p) => p.metadata?.name).filter(Boolean)
  );

  // Filter events: those involving our pods, the StatefulSet, or PVCs
  const relevant = (res.items ?? []).filter((ev) => {
    const obj = ev.involvedObject;
    if (!obj) return false;
    // Pod events
    if (obj.kind === "Pod" && obj.name && podNames.has(obj.name)) return true;
    // StatefulSet events (same name as the LedgerService)
    if (obj.kind === "StatefulSet" && obj.name === name) return true;
    return false;
  });

  // Sort: most recent first
  relevant.sort((a, b) => {
    const ta = a.lastTimestamp ?? a.eventTime ?? "";
    const tb = b.lastTimestamp ?? b.eventTime ?? "";
    return tb.toString().localeCompare(ta.toString());
  });

  return relevant.map((ev) => ({
    type: ev.type ?? "Normal",
    reason: ev.reason ?? "",
    message: ev.message ?? "",
    count: ev.count ?? 1,
    source: [ev.source?.component, ev.source?.host]
      .filter(Boolean)
      .join("/") || undefined,
    involvedObject: `${ev.involvedObject?.kind ?? ""}/${ev.involvedObject?.name ?? ""}`,
    firstTimestamp: ev.firstTimestamp
      ? new Date(ev.firstTimestamp).toISOString()
      : undefined,
    lastTimestamp: (ev.lastTimestamp ?? ev.eventTime)
      ? new Date((ev.lastTimestamp ?? ev.eventTime)!.toString()).toISOString()
      : undefined,
  }));
}
