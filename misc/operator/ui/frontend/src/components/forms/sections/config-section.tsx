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
      <FormField label="Cluster ID" htmlFor="cfg-cluster-id">
        <Input
          id="cfg-cluster-id"
          value={value.clusterID ?? ""}
          onChange={(e) => update({ clusterID: e.target.value || undefined })}
          placeholder="default"
        />
      </FormField>
      <FormField label="Bind Address" htmlFor="cfg-bind">
        <Input
          id="cfg-bind"
          value={value.bindAddr ?? ""}
          onChange={(e) => update({ bindAddr: e.target.value || undefined })}
          placeholder="0.0.0.0"
        />
      </FormField>
      <FormField label="gRPC Port" htmlFor="cfg-grpc-port">
        <Input
          id="cfg-grpc-port"
          type="number"
          value={value.grpcPort ?? ""}
          onChange={(e) => update({ grpcPort: numVal(e.target.value) })}
          placeholder="8888"
        />
      </FormField>
      <FormField label="HTTP Port" htmlFor="cfg-http-port">
        <Input
          id="cfg-http-port"
          type="number"
          value={value.httpPort ?? ""}
          onChange={(e) => update({ httpPort: numVal(e.target.value) })}
          placeholder="9000"
        />
      </FormField>
      <FormField label="WAL Directory" htmlFor="cfg-wal-dir">
        <Input
          id="cfg-wal-dir"
          value={value.walDir ?? ""}
          onChange={(e) => update({ walDir: e.target.value || undefined })}
          placeholder="/data/wal"
        />
      </FormField>
      <FormField label="Data Directory" htmlFor="cfg-data-dir">
        <Input
          id="cfg-data-dir"
          value={value.dataDir ?? ""}
          onChange={(e) => update({ dataDir: e.target.value || undefined })}
          placeholder="/data/db"
        />
      </FormField>
      <FormField label="Debug Mode" htmlFor="cfg-debug">
        <Switch
          id="cfg-debug"
          checked={value.debug ?? false}
          onCheckedChange={(checked) => update({ debug: checked || undefined })}
        />
      </FormField>
      <FormField label="Restore Mode" htmlFor="cfg-restore">
        <Switch
          id="cfg-restore"
          checked={value.restore ?? false}
          onCheckedChange={(checked) => update({ restore: checked || undefined })}
        />
      </FormField>
      <FormField label="Admission Metrics" htmlFor="cfg-admission">
        <Switch
          id="cfg-admission"
          checked={value.admissionMetrics ?? false}
          onCheckedChange={(checked) => update({ admissionMetrics: checked || undefined })}
        />
      </FormField>
      <FormField label="Cache Rotation Threshold" htmlFor="cfg-cache-rot">
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
      <FormField label="Audit Enabled" htmlFor="cfg-audit">
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
