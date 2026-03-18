import * as k8s from "@kubernetes/client-node";

const kc = new k8s.KubeConfig();

if (process.env.KUBERNETES_SERVICE_HOST) {
  kc.loadFromCluster();
} else {
  kc.loadFromDefault();
}

export const kubeConfig = kc;
export const coreApi = kc.makeApiClient(k8s.CoreV1Api);
export const customApi = kc.makeApiClient(k8s.CustomObjectsApi);
export const k8sWatch = new k8s.Watch(kc);

export const CRD_GROUP = "ledger.formance.com";
export const CRD_VERSION = "v1alpha1";
export const LEDGER_SERVICE_PLURAL = "ledgerservices";
export const LEDGER_DEFAULTS_PLURAL = "ledgerdefaults";
export const LEDGER_AGENT_PLURAL = "ledgeragents";
export const LEDGER_CLUSTER_AGENT_PLURAL = "ledgerclusteragents";
