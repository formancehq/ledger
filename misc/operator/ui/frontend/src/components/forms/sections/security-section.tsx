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
        description="Encrypt gRPC and HTTP traffic with TLS. Requires a Kubernetes Secret containing the certificate and key."
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
      <FormField
        label="TLS Secret Name"
        description="Name of the Kubernetes Secret (type kubernetes.io/tls) containing tls.crt and tls.key."
        htmlFor="tls-secret"
      >
        <Input
          id="tls-secret"
          value={tls.secretName ?? ""}
          onChange={(e) =>
            onTlsChange({ ...tls, secretName: e.target.value || undefined })
          }
          placeholder={defaultPlaceholder(defaultTls?.secretName, "tls-secret")}
        />
      </FormField>
      <FormField
        label="TLS CA Secret Key"
        description="Key within the TLS Secret that contains the CA certificate, used for mTLS client verification."
        htmlFor="tls-ca-key"
      >
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
        description="Sign API responses with Ed25519 for authenticity and integrity verification by clients."
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
      <FormField
        label="Signing Secret Name"
        description="Name of the Kubernetes Secret containing the Ed25519 private key for response signing."
        htmlFor="signing-secret"
      >
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
      <FormField
        label="Signing Secret Key"
        description="Key within the Secret that contains the Ed25519 private key file."
        htmlFor="signing-key"
      >
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
