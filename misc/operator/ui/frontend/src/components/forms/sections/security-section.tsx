import type { TLSConfig, ResponseSigningConfig } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { defaultPlaceholder, defaultHint } from "@/lib/defaults";

interface SecuritySectionProps {
  tls?: TLSConfig;
  responseSigning?: ResponseSigningConfig;
  defaultTls?: TLSConfig;
  defaultResponseSigning?: ResponseSigningConfig;
  onTlsChange: (value: TLSConfig) => void;
  onResponseSigningChange: (value: ResponseSigningConfig) => void;
}

export function SecuritySection({
  tls = {},
  responseSigning = {},
  defaultTls,
  defaultResponseSigning,
  onTlsChange,
  onResponseSigningChange,
}: SecuritySectionProps) {
  return (
    <FormSection value="security" title="Security" description="TLS and response signing">
      <FormField
        label="TLS Enabled"
        htmlFor="tls-enabled"
        hint={defaultHint(defaultTls?.enabled)}
      >
        <Switch
          id="tls-enabled"
          checked={tls.enabled ?? false}
          onCheckedChange={(checked) =>
            onTlsChange({ ...tls, enabled: checked || undefined })
          }
        />
      </FormField>
      <FormField label="TLS Secret Name" htmlFor="tls-secret">
        <Input
          id="tls-secret"
          value={tls.secretName ?? ""}
          onChange={(e) =>
            onTlsChange({ ...tls, secretName: e.target.value || undefined })
          }
          placeholder={defaultPlaceholder(defaultTls?.secretName, "tls-secret")}
        />
      </FormField>
      <FormField label="TLS CA Secret Key" htmlFor="tls-ca-key">
        <Input
          id="tls-ca-key"
          value={tls.caSecretKey ?? ""}
          onChange={(e) =>
            onTlsChange({ ...tls, caSecretKey: e.target.value || undefined })
          }
          placeholder={defaultPlaceholder(defaultTls?.caSecretKey, "ca.crt")}
        />
      </FormField>
      <FormField
        label="Response Signing Enabled"
        htmlFor="signing-enabled"
        hint={defaultHint(defaultResponseSigning?.enabled)}
      >
        <Switch
          id="signing-enabled"
          checked={responseSigning.enabled ?? false}
          onCheckedChange={(checked) =>
            onResponseSigningChange({
              ...responseSigning,
              enabled: checked || undefined,
            })
          }
        />
      </FormField>
      <FormField label="Signing Secret Name" htmlFor="signing-secret">
        <Input
          id="signing-secret"
          value={responseSigning.secretName ?? ""}
          onChange={(e) =>
            onResponseSigningChange({
              ...responseSigning,
              secretName: e.target.value || undefined,
            })
          }
          placeholder={defaultPlaceholder(defaultResponseSigning?.secretName, "signing-secret")}
        />
      </FormField>
      <FormField label="Signing Secret Key" htmlFor="signing-key">
        <Input
          id="signing-key"
          value={responseSigning.secretKey ?? ""}
          onChange={(e) =>
            onResponseSigningChange({
              ...responseSigning,
              secretKey: e.target.value || undefined,
            })
          }
          placeholder={defaultPlaceholder(defaultResponseSigning?.secretKey, "signing.key")}
        />
      </FormField>
    </FormSection>
  );
}
