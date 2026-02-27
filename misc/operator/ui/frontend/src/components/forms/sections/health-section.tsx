import type { HealthConfig } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { defaultPlaceholder } from "@/lib/defaults";

interface HealthSectionProps {
  value?: HealthConfig;
  defaults?: HealthConfig;
  onChange: (value: HealthConfig) => void;
}

export function HealthSection({ value = {}, defaults, onChange }: HealthSectionProps) {
  const update = (patch: Partial<HealthConfig>) =>
    onChange({ ...value, ...patch });

  return (
    <FormSection value="health" title="Health Checks" description="Health monitoring configuration">
      <FormField
        label="Interval"
        description="How often the node runs internal health checks (e.g. 10s). Shorter intervals detect issues faster but add CPU overhead."
        htmlFor="health-interval"
      >
        <Input
          id="health-interval"
          value={value.interval ?? ""}
          onChange={(e) => update({ interval: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.interval, "e.g. 10s")}
        />
      </FormField>
      <FormField
        label="WAL Threshold"
        description="Maximum acceptable delay for WAL writes (e.g. 5s). If WAL writes take longer than this, the node is marked unhealthy."
        htmlFor="health-wal"
      >
        <Input
          id="health-wal"
          value={value.walThreshold ?? ""}
          onChange={(e) => update({ walThreshold: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.walThreshold, "e.g. 5s")}
        />
      </FormField>
      <FormField
        label="Data Threshold"
        description="Maximum acceptable delay for Pebble data operations (e.g. 5s). Detects slow storage or compaction stalls."
        htmlFor="health-data"
      >
        <Input
          id="health-data"
          value={value.dataThreshold ?? ""}
          onChange={(e) => update({ dataThreshold: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.dataThreshold, "e.g. 5s")}
        />
      </FormField>
      <FormField
        label="Clock Skew Threshold"
        description="Maximum allowed clock difference between nodes (e.g. 1s). Excessive skew can cause Raft issues and incorrect timestamps."
        htmlFor="health-clock"
      >
        <Input
          id="health-clock"
          value={value.clockSkewThreshold ?? ""}
          onChange={(e) => update({ clockSkewThreshold: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.clockSkewThreshold, "e.g. 1s")}
        />
      </FormField>
    </FormSection>
  );
}
