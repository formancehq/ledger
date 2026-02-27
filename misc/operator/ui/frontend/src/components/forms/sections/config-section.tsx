import type { LedgerServiceConfig } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";

interface ConfigSectionProps {
  value?: LedgerServiceConfig;
  onChange: (value: LedgerServiceConfig) => void;
}

function numVal(v: string): number | undefined {
  if (v === "") return undefined;
  const n = Number(v);
  return isNaN(n) ? undefined : n;
}

export function ConfigSection({ value = {}, onChange }: ConfigSectionProps) {
  const update = (patch: Partial<LedgerServiceConfig>) =>
    onChange({ ...value, ...patch });

  return (
    <FormSection value="config" title="Core Config" description="Cluster ID, ports, directories, and flags">
      <FormField
        label="Cluster ID"
        description="Logical identifier for the Raft cluster. All nodes in the same cluster must share this value."
        htmlFor="cfg-cluster-id"
      >
        <Input
          id="cfg-cluster-id"
          value={value.clusterID ?? ""}
          onChange={(e) => update({ clusterID: e.target.value || undefined })}
          placeholder="default"
        />
      </FormField>
      <FormField
        label="Bind Address"
        description="Network address the server listens on. Use 0.0.0.0 to listen on all interfaces."
        htmlFor="cfg-bind"
      >
        <Input
          id="cfg-bind"
          value={value.bindAddr ?? ""}
          onChange={(e) => update({ bindAddr: e.target.value || undefined })}
          placeholder="0.0.0.0"
        />
      </FormField>
      <FormField
        label="gRPC Port"
        description="Port for the primary gRPC API, used by clients and inter-node Raft communication."
        htmlFor="cfg-grpc-port"
      >
        <Input
          id="cfg-grpc-port"
          type="number"
          value={value.grpcPort ?? ""}
          onChange={(e) => update({ grpcPort: numVal(e.target.value) })}
          placeholder="8888"
        />
      </FormField>
      <FormField
        label="HTTP Port"
        description="Port for the REST-compatible HTTP API. Used by dashboards and HTTP clients."
        htmlFor="cfg-http-port"
      >
        <Input
          id="cfg-http-port"
          type="number"
          value={value.httpPort ?? ""}
          onChange={(e) => update({ httpPort: numVal(e.target.value) })}
          placeholder="9000"
        />
      </FormField>
      <FormField
        label="WAL Directory"
        description="Filesystem path where the Write-Ahead Log is stored. Should be on fast, low-latency storage."
        htmlFor="cfg-wal-dir"
      >
        <Input
          id="cfg-wal-dir"
          value={value.walDir ?? ""}
          onChange={(e) => update({ walDir: e.target.value || undefined })}
          placeholder="/data/wal"
        />
      </FormField>
      <FormField
        label="Data Directory"
        description="Filesystem path for the Pebble database files (SSTables, manifests). Can be on slower storage than WAL."
        htmlFor="cfg-data-dir"
      >
        <Input
          id="cfg-data-dir"
          value={value.dataDir ?? ""}
          onChange={(e) => update({ dataDir: e.target.value || undefined })}
          placeholder="/data/db"
        />
      </FormField>
      <FormField
        label="Debug Mode"
        description="Enable verbose logging and additional diagnostic endpoints. Not recommended in production."
        htmlFor="cfg-debug"
      >
        <Switch
          id="cfg-debug"
          checked={value.debug ?? false}
          onCheckedChange={(checked) => update({ debug: checked || undefined })}
        />
      </FormField>
      <FormField
        label="Restore Mode"
        description="Start the node in restore mode to rebuild state from a snapshot. Use only for disaster recovery."
        htmlFor="cfg-restore"
      >
        <Switch
          id="cfg-restore"
          checked={value.restore ?? false}
          onCheckedChange={(checked) => update({ restore: checked || undefined })}
        />
      </FormField>
      <FormField
        label="Admission Metrics"
        description="Emit per-request metrics for admission control (latency, queue depth). Adds some overhead."
        htmlFor="cfg-admission"
      >
        <Switch
          id="cfg-admission"
          checked={value.admissionMetrics ?? false}
          onCheckedChange={(checked) => update({ admissionMetrics: checked || undefined })}
        />
      </FormField>
      <FormField
        label="Cache Rotation Threshold"
        description="Number of Raft entries after which the in-memory read cache is rotated. Lower values use less memory but cause more cache misses."
        htmlFor="cfg-cache-rot"
      >
        <Input
          id="cfg-cache-rot"
          type="number"
          value={value.cache?.rotationThreshold ?? ""}
          onChange={(e) =>
            update({
              cache: { ...value.cache, rotationThreshold: numVal(e.target.value) },
            })
          }
        />
      </FormField>
      <FormField
        label="Audit Enabled"
        description="Record an audit trail of all state-changing operations for compliance and debugging."
        htmlFor="cfg-audit"
      >
        <Switch
          id="cfg-audit"
          checked={value.audit?.enabled ?? false}
          onCheckedChange={(checked) =>
            update({ audit: { ...value.audit, enabled: checked || undefined } })
          }
        />
      </FormField>
    </FormSection>
  );
}
