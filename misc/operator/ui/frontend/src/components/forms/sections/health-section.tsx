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
      <FormField label="Interval" description="e.g. 10s" htmlFor="health-interval">
        <Input
          id="health-interval"
          value={value.interval ?? ""}
          onChange={(e) => update({ interval: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.interval, "e.g. 10s")}
        />
      </FormField>
      <FormField label="WAL Threshold" description="e.g. 5s" htmlFor="health-wal">
        <Input
          id="health-wal"
          value={value.walThreshold ?? ""}
          onChange={(e) => update({ walThreshold: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.walThreshold, "e.g. 5s")}
        />
      </FormField>
      <FormField label="Data Threshold" description="e.g. 5s" htmlFor="health-data">
        <Input
          id="health-data"
          value={value.dataThreshold ?? ""}
          onChange={(e) => update({ dataThreshold: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.dataThreshold, "e.g. 5s")}
        />
      </FormField>
      <FormField label="Clock Skew Threshold" description="e.g. 1s" htmlFor="health-clock">
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
