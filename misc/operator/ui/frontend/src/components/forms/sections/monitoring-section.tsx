import type { MonitoringConfig, TracesConfig, MetricsConfig, LogsConfig, PyroscopeConfig } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { defaultPlaceholder, defaultHint } from "@/lib/defaults";

interface MonitoringSectionProps {
  value?: MonitoringConfig;
  defaults?: MonitoringConfig;
  onChange: (value: MonitoringConfig) => void;
}

export function MonitoringSection({ value = {}, defaults, onChange }: MonitoringSectionProps) {
  const updateTraces = (patch: Partial<TracesConfig>) =>
    onChange({ ...value, traces: { ...value.traces, ...patch } });
  const updateMetrics = (patch: Partial<MetricsConfig>) =>
    onChange({ ...value, metrics: { ...value.metrics, ...patch } });
  const updateLogs = (patch: Partial<LogsConfig>) =>
    onChange({ ...value, logs: { ...value.logs, ...patch } });
  const updatePyroscope = (patch: Partial<PyroscopeConfig>) =>
    onChange({ ...value, pyroscope: { ...value.pyroscope, ...patch } });

  return (
    <FormSection value="monitoring" title="Monitoring" description="Traces, metrics, logs, and profiling">
      {/* Traces */}
      <FormField
        label="Traces Enabled"
        htmlFor="traces-enabled"
        hint={defaultHint(defaults?.traces?.enabled)}
      >
        <Switch
          id="traces-enabled"
          checked={value.traces?.enabled ?? false}
          onCheckedChange={(checked) => updateTraces({ enabled: checked || undefined })}
        />
      </FormField>
      <FormField label="Traces Exporter" htmlFor="traces-exporter">
        <Input
          id="traces-exporter"
          value={value.traces?.exporter ?? ""}
          onChange={(e) => updateTraces({ exporter: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.traces?.exporter, "otlp")}
        />
      </FormField>
      <FormField label="Traces Endpoint" htmlFor="traces-endpoint">
        <Input
          id="traces-endpoint"
          value={value.traces?.endpoint ?? ""}
          onChange={(e) => updateTraces({ endpoint: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.traces?.endpoint, "localhost:4317")}
        />
      </FormField>

      {/* Metrics */}
      <FormField
        label="Metrics Enabled"
        htmlFor="metrics-enabled"
        hint={defaultHint(defaults?.metrics?.enabled)}
      >
        <Switch
          id="metrics-enabled"
          checked={value.metrics?.enabled ?? false}
          onCheckedChange={(checked) => updateMetrics({ enabled: checked || undefined })}
        />
      </FormField>
      <FormField label="Metrics Exporter" htmlFor="metrics-exporter">
        <Input
          id="metrics-exporter"
          value={value.metrics?.exporter ?? ""}
          onChange={(e) => updateMetrics({ exporter: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.metrics?.exporter, "otlp")}
        />
      </FormField>
      <FormField label="Metrics Endpoint" htmlFor="metrics-endpoint">
        <Input
          id="metrics-endpoint"
          value={value.metrics?.endpoint ?? ""}
          onChange={(e) => updateMetrics({ endpoint: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.metrics?.endpoint, "localhost:4317")}
        />
      </FormField>

      {/* Logs */}
      <FormField
        label="Logs Enabled"
        htmlFor="logs-enabled"
        hint={defaultHint(defaults?.logs?.enabled)}
      >
        <Switch
          id="logs-enabled"
          checked={value.logs?.enabled ?? false}
          onCheckedChange={(checked) => updateLogs({ enabled: checked || undefined })}
        />
      </FormField>
      <FormField label="Logs Level" htmlFor="logs-level">
        <Input
          id="logs-level"
          value={value.logs?.level ?? ""}
          onChange={(e) => updateLogs({ level: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.logs?.level, "info")}
        />
      </FormField>
      <FormField label="Logs Exporter" htmlFor="logs-exporter">
        <Input
          id="logs-exporter"
          value={value.logs?.exporter ?? ""}
          onChange={(e) => updateLogs({ exporter: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.logs?.exporter, "otlp")}
        />
      </FormField>

      {/* Pyroscope */}
      <FormField
        label="Pyroscope Enabled"
        htmlFor="pyro-enabled"
        hint={defaultHint(defaults?.pyroscope?.enabled)}
      >
        <Switch
          id="pyro-enabled"
          checked={value.pyroscope?.enabled ?? false}
          onCheckedChange={(checked) => updatePyroscope({ enabled: checked || undefined })}
        />
      </FormField>
      <FormField label="Pyroscope Server" htmlFor="pyro-server">
        <Input
          id="pyro-server"
          value={value.pyroscope?.serverAddress ?? ""}
          onChange={(e) => updatePyroscope({ serverAddress: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.pyroscope?.serverAddress, "http://pyroscope:4040")}
        />
      </FormField>
      <FormField label="Pyroscope App Name" htmlFor="pyro-app">
        <Input
          id="pyro-app"
          value={value.pyroscope?.applicationName ?? ""}
          onChange={(e) => updatePyroscope({ applicationName: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.pyroscope?.applicationName, "ledger")}
        />
      </FormField>
    </FormSection>
  );
}
