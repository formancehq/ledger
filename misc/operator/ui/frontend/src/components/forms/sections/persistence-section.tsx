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
      <FormField label="WAL Storage Class" htmlFor="wal-sc">
        <Input
          id="wal-sc"
          value={value.wal?.storageClass ?? ""}
          onChange={(e) => updateWal({ storageClass: e.target.value || undefined })}
          placeholder="gp3"
        />
      </FormField>
      <FormField label="WAL Access Mode" htmlFor="wal-am">
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
      <FormField label="WAL Size" htmlFor="wal-size">
        <Input
          id="wal-size"
          value={value.wal?.size ?? ""}
          onChange={(e) => updateWal({ size: e.target.value || undefined })}
          placeholder="10Gi"
        />
      </FormField>

      {/* Data Volume */}
      <FormField label="Data Storage Class" htmlFor="data-sc">
        <Input
          id="data-sc"
          value={value.data?.storageClass ?? ""}
          onChange={(e) => updateData({ storageClass: e.target.value || undefined })}
          placeholder="gp3"
        />
      </FormField>
      <FormField label="Data Access Mode" htmlFor="data-am">
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
      <FormField label="Data Size" htmlFor="data-size">
        <Input
          id="data-size"
          value={value.data?.size ?? ""}
          onChange={(e) => updateData({ size: e.target.value || undefined })}
          placeholder="50Gi"
        />
      </FormField>

      {/* Retention Policy */}
      <FormField label="Retention When Scaled" htmlFor="ret-scaled">
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
      <FormField label="Retention When Deleted" htmlFor="ret-deleted">
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
