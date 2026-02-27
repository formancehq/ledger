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
      <FormField
        label="MemTable Size"
        description="Size in bytes of each in-memory write buffer. Larger values improve write throughput but use more RAM."
        htmlFor="pebble-memtable"
      >
        <Input
          id="pebble-memtable"
          type="number"
          value={value.memTableSize ?? ""}
          onChange={(e) => update({ memTableSize: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.memTableSize)}
        />
      </FormField>
      <FormField
        label="MemTable Stop Writes Threshold"
        description="Number of MemTables allowed in memory before writes stall. Prevents unbounded memory growth under heavy write load."
        htmlFor="pebble-memtable-stop"
      >
        <Input
          id="pebble-memtable-stop"
          type="number"
          value={value.memTableStopWritesThreshold ?? ""}
          onChange={(e) => update({ memTableStopWritesThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.memTableStopWritesThreshold)}
        />
      </FormField>
      <FormField
        label="Cache Size"
        description="Size in bytes of the block cache for SSTable data. A larger cache reduces disk reads for frequently accessed data."
        htmlFor="pebble-cache"
      >
        <Input
          id="pebble-cache"
          type="number"
          value={value.cacheSize ?? ""}
          onChange={(e) => update({ cacheSize: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.cacheSize)}
        />
      </FormField>
      <FormField
        label="L0 Compaction Threshold"
        description="Number of L0 files that trigger a compaction. Lower values keep read amplification down but cause more compaction work."
        htmlFor="pebble-l0-compact"
      >
        <Input
          id="pebble-l0-compact"
          type="number"
          value={value.l0CompactionThreshold ?? ""}
          onChange={(e) => update({ l0CompactionThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.l0CompactionThreshold)}
        />
      </FormField>
      <FormField
        label="L0 Stop Writes Threshold"
        description="Number of L0 files at which writes stall until compaction catches up. Safety valve against unbounded L0 growth."
        htmlFor="pebble-l0-stop"
      >
        <Input
          id="pebble-l0-stop"
          type="number"
          value={value.l0StopWritesThreshold ?? ""}
          onChange={(e) => update({ l0StopWritesThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.l0StopWritesThreshold)}
        />
      </FormField>
      <FormField
        label="LBase Max Bytes"
        description="Target size for the base level of the LSM tree. Controls when data moves from L0 to deeper levels."
        htmlFor="pebble-lbase"
      >
        <Input
          id="pebble-lbase"
          type="number"
          value={value.lBaseMaxBytes ?? ""}
          onChange={(e) => update({ lBaseMaxBytes: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.lBaseMaxBytes)}
        />
      </FormField>
      <FormField
        label="Target File Size"
        description="Target size for individual SSTable files. Smaller files speed up compaction but increase file count."
        htmlFor="pebble-target-file"
      >
        <Input
          id="pebble-target-file"
          type="number"
          value={value.targetFileSize ?? ""}
          onChange={(e) => update({ targetFileSize: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.targetFileSize)}
        />
      </FormField>
      <FormField
        label="Bytes Per Sync"
        description="How many bytes to write before syncing data files to disk. Lower values are safer but slower."
        htmlFor="pebble-bytes-sync"
      >
        <Input
          id="pebble-bytes-sync"
          type="number"
          value={value.bytesPerSync ?? ""}
          onChange={(e) => update({ bytesPerSync: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.bytesPerSync)}
        />
      </FormField>
      <FormField
        label="WAL Bytes Per Sync"
        description="How many bytes to write to the WAL before syncing to disk. Lower values reduce data loss risk on crash."
        htmlFor="pebble-wal-bytes-sync"
      >
        <Input
          id="pebble-wal-bytes-sync"
          type="number"
          value={value.walBytesPerSync ?? ""}
          onChange={(e) => update({ walBytesPerSync: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.walBytesPerSync)}
        />
      </FormField>
      <FormField
        label="Max Concurrent Compactions"
        description="Maximum number of compactions running in parallel. More parallelism speeds up compaction but uses more CPU and I/O."
        htmlFor="pebble-max-compact"
      >
        <Input
          id="pebble-max-compact"
          type="number"
          value={value.maxConcurrentCompactions ?? ""}
          onChange={(e) => update({ maxConcurrentCompactions: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.maxConcurrentCompactions)}
        />
      </FormField>
      <FormField
        label="WAL Min Sync Interval"
        description="Minimum interval between WAL syncs (e.g. 30ms). Batches small writes together for better throughput at the cost of slightly higher latency."
        htmlFor="pebble-wal-sync"
      >
        <Input
          id="pebble-wal-sync"
          value={value.walMinSyncInterval ?? ""}
          onChange={(e) => update({ walMinSyncInterval: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.walMinSyncInterval, "e.g. 30ms")}
        />
      </FormField>
      <FormField
        label="Disable WAL"
        description="Skip write-ahead logging entirely. Dramatically faster but data will be lost on crash. Only for ephemeral/testing workloads."
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
