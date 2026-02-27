import type { ImageSpec } from "shared";
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

interface ImageSectionProps {
  value?: ImageSpec;
  defaults?: ImageSpec;
  onChange: (value: ImageSpec) => void;
}

export function ImageSection({ value = {}, defaults, onChange }: ImageSectionProps) {
  const update = (patch: Partial<ImageSpec>) =>
    onChange({ ...value, ...patch });

  return (
    <FormSection value="image" title="Image" description="Container image settings">
      <FormField label="Repository" htmlFor="image-repo">
        <Input
          id="image-repo"
          value={value.repository ?? ""}
          onChange={(e) => update({ repository: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.repository, "ghcr.io/formancehq/ledger")}
        />
      </FormField>
      <FormField label="Tag" htmlFor="image-tag">
        <Input
          id="image-tag"
          value={value.tag ?? ""}
          onChange={(e) => update({ tag: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.tag, "latest")}
        />
      </FormField>
      <FormField
        label="Pull Policy"
        htmlFor="image-pull-policy"
        hint={defaults?.pullPolicy ? `Default: ${defaults.pullPolicy}` : undefined}
      >
        <Select
          value={value.pullPolicy ?? ""}
          onValueChange={(v) => update({ pullPolicy: v || undefined })}
        >
          <SelectTrigger id="image-pull-policy">
            <SelectValue placeholder={defaults?.pullPolicy ? `${defaults.pullPolicy} (default)` : "Default"} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="Always">Always</SelectItem>
            <SelectItem value="IfNotPresent">IfNotPresent</SelectItem>
            <SelectItem value="Never">Never</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
    </FormSection>
  );
}
