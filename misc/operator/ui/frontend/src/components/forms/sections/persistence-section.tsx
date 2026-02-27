import type { PersistenceSpec, VolumeSpec, RetentionPolicySpec } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface PersistenceSectionProps {
  value?: PersistenceSpec;
  onChange: (value: PersistenceSpec) => void;
}

export function PersistenceSection({ value = {}, onChange }: PersistenceSectionProps) {
  const updateWal = (patch: Partial<VolumeSpec>) =>
    onChange({ ...value, wal: { ...value.wal, ...patch } });
  const updateData = (patch: Partial<VolumeSpec>) =>
    onChange({ ...value, data: { ...value.data, ...patch } });
  const updateRetention = (patch: Partial<RetentionPolicySpec>) =>
    onChange({ ...value, retentionPolicy: { ...value.retentionPolicy, ...patch } });

  return (
    <FormSection value="persistence" title="Persistence" description="WAL and data volume configuration">
      {/* WAL Volume */}
      <FormField
        label="WAL Storage Class"
        description="Kubernetes StorageClass for the WAL volume. Use fast, low-latency storage (e.g. gp3, local-path) for best write performance."
        htmlFor="wal-sc"
      >
        <Input
          id="wal-sc"
          value={value.wal?.storageClass ?? ""}
          onChange={(e) => updateWal({ storageClass: e.target.value || undefined })}
          placeholder="gp3"
        />
      </FormField>
      <FormField
        label="WAL Access Mode"
        description="PVC access mode. ReadWriteOnce (single node) is typical. ReadWriteMany allows multiple nodes but requires a compatible StorageClass."
        htmlFor="wal-am"
      >
        <Select
          value={value.wal?.accessMode ?? ""}
          onValueChange={(v) => updateWal({ accessMode: v || undefined })}
        >
          <SelectTrigger id="wal-am">
            <SelectValue placeholder="Default" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="ReadWriteOnce">ReadWriteOnce</SelectItem>
            <SelectItem value="ReadWriteMany">ReadWriteMany</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
      <FormField
        label="WAL Size"
        description="Storage capacity for the WAL volume (e.g. 10Gi). Size it based on expected write throughput and snapshot frequency."
        htmlFor="wal-size"
      >
        <Input
          id="wal-size"
          value={value.wal?.size ?? ""}
          onChange={(e) => updateWal({ size: e.target.value || undefined })}
          placeholder="10Gi"
        />
      </FormField>

      {/* Data Volume */}
      <FormField
        label="Data Storage Class"
        description="Kubernetes StorageClass for the Pebble data volume. Can use cheaper storage than WAL since reads are cached."
        htmlFor="data-sc"
      >
        <Input
          id="data-sc"
          value={value.data?.storageClass ?? ""}
          onChange={(e) => updateData({ storageClass: e.target.value || undefined })}
          placeholder="gp3"
        />
      </FormField>
      <FormField
        label="Data Access Mode"
        description="PVC access mode for the data volume. ReadWriteOnce is standard for single-node attachment."
        htmlFor="data-am"
      >
        <Select
          value={value.data?.accessMode ?? ""}
          onValueChange={(v) => updateData({ accessMode: v || undefined })}
        >
          <SelectTrigger id="data-am">
            <SelectValue placeholder="Default" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="ReadWriteOnce">ReadWriteOnce</SelectItem>
            <SelectItem value="ReadWriteMany">ReadWriteMany</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
      <FormField
        label="Data Size"
        description="Storage capacity for the Pebble data volume (e.g. 50Gi). Should accommodate the full dataset plus compaction headroom."
        htmlFor="data-size"
      >
        <Input
          id="data-size"
          value={value.data?.size ?? ""}
          onChange={(e) => updateData({ size: e.target.value || undefined })}
          placeholder="50Gi"
        />
      </FormField>

      {/* Retention Policy */}
      <FormField
        label="Retention When Scaled"
        description="What to do with PVCs when replicas are scaled down. 'Retain' keeps data for scale-up, 'Delete' frees storage."
        htmlFor="ret-scaled"
      >
        <Select
          value={value.retentionPolicy?.whenScaled ?? ""}
          onValueChange={(v) => updateRetention({ whenScaled: v || undefined })}
        >
          <SelectTrigger id="ret-scaled">
            <SelectValue placeholder="Default" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="Retain">Retain</SelectItem>
            <SelectItem value="Delete">Delete</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
      <FormField
        label="Retention When Deleted"
        description="What to do with PVCs when the LedgerService is deleted. 'Retain' preserves data for recovery, 'Delete' cleans up everything."
        htmlFor="ret-deleted"
      >
        <Select
          value={value.retentionPolicy?.whenDeleted ?? ""}
          onValueChange={(v) => updateRetention({ whenDeleted: v || undefined })}
        >
          <SelectTrigger id="ret-deleted">
            <SelectValue placeholder="Default" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="Retain">Retain</SelectItem>
            <SelectItem value="Delete">Delete</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
    </FormSection>
  );
}
