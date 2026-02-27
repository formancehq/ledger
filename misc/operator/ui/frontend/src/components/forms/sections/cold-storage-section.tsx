import type { ColdStorageConfig, S3Config } from "shared";
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
import { defaultPlaceholder } from "@/lib/defaults";

interface ColdStorageSectionProps {
  value?: ColdStorageConfig;
  defaults?: ColdStorageConfig;
  onChange: (value: ColdStorageConfig) => void;
}

export function ColdStorageSection({ value = {}, defaults, onChange }: ColdStorageSectionProps) {
  const update = (patch: Partial<ColdStorageConfig>) =>
    onChange({ ...value, ...patch });
  const updateS3 = (patch: Partial<S3Config>) =>
    onChange({ ...value, s3: { ...value.s3, ...patch } });

  return (
    <FormSection value="cold-storage" title="Cold Storage" description="Snapshot cold storage backend">
      <FormField
        label="Driver"
        description="Storage backend for Raft snapshots. Use 'filesystem' for local disk or 's3' for S3-compatible object storage."
        htmlFor="cs-driver"
        hint={defaults?.driver ? `Default: ${defaults.driver}` : undefined}
      >
        <Select
          value={value.driver ?? ""}
          onValueChange={(v) => update({ driver: v || undefined })}
        >
          <SelectTrigger id="cs-driver">
            <SelectValue placeholder={defaults?.driver ? `${defaults.driver} (default)` : "None"} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="filesystem">Filesystem</SelectItem>
            <SelectItem value="s3">S3</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
      <FormField
        label="Path"
        description="Local filesystem path for snapshot storage. Only used with the 'filesystem' driver."
        htmlFor="cs-path"
      >
        <Input
          id="cs-path"
          value={value.path ?? ""}
          onChange={(e) => update({ path: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.path, "/data/cold")}
        />
      </FormField>
      <FormField
        label="Bucket ID"
        description="Logical identifier for partitioning snapshots within the storage backend. Useful for multi-tenant setups."
        htmlFor="cs-bucket-id"
      >
        <Input
          id="cs-bucket-id"
          value={value.bucketId ?? ""}
          onChange={(e) => update({ bucketId: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.bucketId)}
        />
      </FormField>
      <FormField
        label="S3 Bucket"
        description="Name of the S3 bucket where snapshots are stored. The bucket must already exist."
        htmlFor="cs-s3-bucket"
      >
        <Input
          id="cs-s3-bucket"
          value={value.s3?.bucket ?? ""}
          onChange={(e) => updateS3({ bucket: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.s3?.bucket, "my-bucket")}
        />
      </FormField>
      <FormField
        label="S3 Region"
        description="AWS region of the S3 bucket (e.g. us-east-1, eu-west-1)."
        htmlFor="cs-s3-region"
      >
        <Input
          id="cs-s3-region"
          value={value.s3?.region ?? ""}
          onChange={(e) => updateS3({ region: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.s3?.region, "us-east-1")}
        />
      </FormField>
      <FormField
        label="S3 Endpoint"
        description="Custom S3 endpoint URL. Use this for MinIO, Ceph, or other S3-compatible services."
        htmlFor="cs-s3-endpoint"
      >
        <Input
          id="cs-s3-endpoint"
          value={value.s3?.endpoint ?? ""}
          onChange={(e) => updateS3({ endpoint: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.s3?.endpoint, "https://s3.amazonaws.com")}
        />
      </FormField>
    </FormSection>
  );
}
