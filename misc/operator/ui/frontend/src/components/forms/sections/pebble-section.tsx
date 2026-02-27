import type { PebbleConfig } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { defaultPlaceholder, defaultHint } from "@/lib/defaults";

interface PebbleSectionProps {
  value?: PebbleConfig;
  defaults?: PebbleConfig;
  onChange: (value: PebbleConfig) => void;
}

function numVal(v: string): number | undefined {
  if (v === "") return undefined;
  const n = Number(v);
  return isNaN(n) ? undefined : n;
}

export function PebbleSection({ value = {}, defaults, onChange }: PebbleSectionProps) {
  const update = (patch: Partial<PebbleConfig>) =>
    onChange({ ...value, ...patch });

  return (
    <FormSection value="pebble" title="Pebble" description="Embedded storage engine tuning">
      <FormField label="MemTable Size" description="Bytes" htmlFor="pebble-memtable">
        <Input
          id="pebble-memtable"
          type="number"
          value={value.memTableSize ?? ""}
          onChange={(e) => update({ memTableSize: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.memTableSize)}
        />
      </FormField>
      <FormField label="MemTable Stop Writes Threshold" htmlFor="pebble-memtable-stop">
        <Input
          id="pebble-memtable-stop"
          type="number"
          value={value.memTableStopWritesThreshold ?? ""}
          onChange={(e) => update({ memTableStopWritesThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.memTableStopWritesThreshold)}
        />
      </FormField>
      <FormField label="Cache Size" description="Block cache size in bytes" htmlFor="pebble-cache">
        <Input
          id="pebble-cache"
          type="number"
          value={value.cacheSize ?? ""}
          onChange={(e) => update({ cacheSize: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.cacheSize)}
        />
      </FormField>
      <FormField label="L0 Compaction Threshold" htmlFor="pebble-l0-compact">
        <Input
          id="pebble-l0-compact"
          type="number"
          value={value.l0CompactionThreshold ?? ""}
          onChange={(e) => update({ l0CompactionThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.l0CompactionThreshold)}
        />
      </FormField>
      <FormField label="L0 Stop Writes Threshold" htmlFor="pebble-l0-stop">
        <Input
          id="pebble-l0-stop"
          type="number"
          value={value.l0StopWritesThreshold ?? ""}
          onChange={(e) => update({ l0StopWritesThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.l0StopWritesThreshold)}
        />
      </FormField>
      <FormField label="LBase Max Bytes" htmlFor="pebble-lbase">
        <Input
          id="pebble-lbase"
          type="number"
          value={value.lBaseMaxBytes ?? ""}
          onChange={(e) => update({ lBaseMaxBytes: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.lBaseMaxBytes)}
        />
      </FormField>
      <FormField label="Target File Size" htmlFor="pebble-target-file">
        <Input
          id="pebble-target-file"
          type="number"
          value={value.targetFileSize ?? ""}
          onChange={(e) => update({ targetFileSize: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.targetFileSize)}
        />
      </FormField>
      <FormField label="Bytes Per Sync" htmlFor="pebble-bytes-sync">
        <Input
          id="pebble-bytes-sync"
          type="number"
          value={value.bytesPerSync ?? ""}
          onChange={(e) => update({ bytesPerSync: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.bytesPerSync)}
        />
      </FormField>
      <FormField label="WAL Bytes Per Sync" htmlFor="pebble-wal-bytes-sync">
        <Input
          id="pebble-wal-bytes-sync"
          type="number"
          value={value.walBytesPerSync ?? ""}
          onChange={(e) => update({ walBytesPerSync: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.walBytesPerSync)}
        />
      </FormField>
      <FormField label="Max Concurrent Compactions" htmlFor="pebble-max-compact">
        <Input
          id="pebble-max-compact"
          type="number"
          value={value.maxConcurrentCompactions ?? ""}
          onChange={(e) => update({ maxConcurrentCompactions: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.maxConcurrentCompactions)}
        />
      </FormField>
      <FormField label="WAL Min Sync Interval" description="e.g. 30ms" htmlFor="pebble-wal-sync">
        <Input
          id="pebble-wal-sync"
          value={value.walMinSyncInterval ?? ""}
          onChange={(e) => update({ walMinSyncInterval: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.walMinSyncInterval, "e.g. 30ms")}
        />
      </FormField>
      <FormField
        label="Disable WAL"
        htmlFor="pebble-disable-wal"
        hint={defaultHint(defaults?.disableWAL)}
      >
        <Switch
          id="pebble-disable-wal"
          checked={value.disableWAL ?? false}
          onCheckedChange={(checked) => update({ disableWAL: checked || undefined })}
        />
      </FormField>
    </FormSection>
  );
}
