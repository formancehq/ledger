import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import type {
  LedgerService,
  LedgerServiceDetail,
  LedgerDefaultsListItem,
  LedgerDefaultsDetail,
} from "shared";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: { message: res.statusText } }));
    throw new Error(body.error?.message ?? res.statusText);
  }
  return res.json();
}

// --- Namespaces ---

export function useNamespaces() {
  return useQuery({
    queryKey: ["namespaces"],
    queryFn: () =>
      fetchJson<Array<{ name: string; status: string }>>("/api/namespaces"),
    staleTime: 60_000,
  });
}

// --- LedgerServices ---

export function useLedgerServices(namespace: string) {
  return useQuery({
    queryKey: ["ledger-services", namespace],
    queryFn: () =>
      fetchJson<LedgerService[]>(
        `/api/namespaces/${namespace}/ledger-services`
      ),
    enabled: !!namespace,
  });
}

export function useLedgerServiceDetail(namespace: string, name: string) {
  return useQuery({
    queryKey: ["ledger-service", namespace, name],
    queryFn: () =>
      fetchJson<LedgerServiceDetail>(
        `/api/namespaces/${namespace}/ledger-services/${name}`
      ),
    enabled: !!namespace && !!name,
  });
}

export function useCreateLedgerService(namespace: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      fetchJson<LedgerService>(
        `/api/namespaces/${namespace}/ledger-services`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger-services", namespace] });
    },
  });
}

export function useUpdateLedgerService(namespace: string, name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      fetchJson<LedgerService>(
        `/api/namespaces/${namespace}/ledger-services/${name}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }
      ),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: ["ledger-service", namespace, name],
      });
      qc.invalidateQueries({ queryKey: ["ledger-services", namespace] });
    },
  });
}

export function useDeleteLedgerService(namespace: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      fetchJson<{ ok: boolean }>(
        `/api/namespaces/${namespace}/ledger-services/${name}`,
        { method: "DELETE" }
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger-services", namespace] });
    },
  });
}

export function useScaleLedgerService(namespace: string, name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (replicas: number) =>
      fetchJson<LedgerService>(
        `/api/namespaces/${namespace}/ledger-services/${name}/scale`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ replicas }),
        }
      ),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: ["ledger-service", namespace, name],
      });
      qc.invalidateQueries({ queryKey: ["ledger-services", namespace] });
    },
  });
}

export function useRestartLedgerService(namespace: string, name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      fetchJson<LedgerService>(
        `/api/namespaces/${namespace}/ledger-services/${name}/restart`,
        { method: "POST" }
      ),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: ["ledger-service", namespace, name],
      });
    },
  });
}

// --- LedgerDefaults ---

export function useLedgerDefaults() {
  return useQuery({
    queryKey: ["ledger-defaults"],
    queryFn: () =>
      fetchJson<LedgerDefaultsListItem[]>("/api/ledger-defaults"),
  });
}

export function useLedgerDefaultsDetail(name: string) {
  return useQuery({
    queryKey: ["ledger-defaults", name],
    queryFn: () =>
      fetchJson<LedgerDefaultsDetail>(`/api/ledger-defaults/${name}`),
    enabled: !!name,
  });
}

export function useCreateLedgerDefaults() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      fetchJson("/api/ledger-defaults", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger-defaults"] });
    },
  });
}

export function useUpdateLedgerDefaults(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      fetchJson(`/api/ledger-defaults/${name}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger-defaults", name] });
      qc.invalidateQueries({ queryKey: ["ledger-defaults"] });
    },
  });
}

export function useDeleteLedgerDefaults() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      fetchJson<{ ok: boolean }>(`/api/ledger-defaults/${name}`, {
        method: "DELETE",
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger-defaults"] });
    },
  });
}
