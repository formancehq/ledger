import type { RaftConfig } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { defaultPlaceholder } from "@/lib/defaults";

interface RaftSectionProps {
  value?: RaftConfig;
  defaults?: RaftConfig;
  onChange: (value: RaftConfig) => void;
}

function numVal(v: string): number | undefined {
  if (v === "") return undefined;
  const n = Number(v);
  return isNaN(n) ? undefined : n;
}

export function RaftSection({ value = {}, defaults, onChange }: RaftSectionProps) {
  const update = (patch: Partial<RaftConfig>) =>
    onChange({ ...value, ...patch });

  return (
    <FormSection value="raft" title="Raft" description="Consensus protocol tuning">
      <FormField
        label="Snapshot Threshold"
        description="Number of Raft log entries before a snapshot is taken. Snapshots allow slow followers to catch up faster."
        htmlFor="raft-snap-threshold"
      >
        <Input
          id="raft-snap-threshold"
          type="number"
          value={value.snapshotThreshold ?? ""}
          onChange={(e) => update({ snapshotThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.snapshotThreshold)}
        />
      </FormField>
      <FormField
        label="Compaction Margin"
        description="Number of extra log entries to keep after a snapshot, as a safety buffer before compacting old entries."
        htmlFor="raft-compact-margin"
      >
        <Input
          id="raft-compact-margin"
          type="number"
          value={value.compactionMargin ?? ""}
          onChange={(e) => update({ compactionMargin: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.compactionMargin)}
        />
      </FormField>
      <FormField
        label="Snapshot Interval"
        description="Time-based interval for periodic snapshots (e.g. 5m). Ensures snapshots happen even under low write load."
        htmlFor="raft-snap-interval"
      >
        <Input
          id="raft-snap-interval"
          value={value.snapshotInterval ?? ""}
          onChange={(e) => update({ snapshotInterval: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.snapshotInterval, "e.g. 5m")}
        />
      </FormField>
      <FormField
        label="Election Tick"
        description="Number of tick intervals before a follower starts an election if it hasn't heard from the leader. Higher values reduce false elections."
        htmlFor="raft-election-tick"
      >
        <Input
          id="raft-election-tick"
          type="number"
          value={value.electionTick ?? ""}
          onChange={(e) => update({ electionTick: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.electionTick)}
        />
      </FormField>
      <FormField
        label="Heartbeat Tick"
        description="Number of tick intervals between leader heartbeats. Must be less than Election Tick. Lower values detect failures faster."
        htmlFor="raft-heartbeat-tick"
      >
        <Input
          id="raft-heartbeat-tick"
          type="number"
          value={value.heartbeatTick ?? ""}
          onChange={(e) => update({ heartbeatTick: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.heartbeatTick)}
        />
      </FormField>
      <FormField
        label="Max Size Per Msg"
        description="Maximum size in bytes of a single Raft message. Limits memory usage when replicating large batches."
        htmlFor="raft-max-msg"
      >
        <Input
          id="raft-max-msg"
          type="number"
          value={value.maxSizePerMsg ?? ""}
          onChange={(e) => update({ maxSizePerMsg: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.maxSizePerMsg)}
        />
      </FormField>
      <FormField
        label="Max Inflight Msgs"
        description="Maximum number of in-flight append messages (messages sent but not yet acknowledged). Controls replication pipeline depth."
        htmlFor="raft-inflight"
      >
        <Input
          id="raft-inflight"
          type="number"
          value={value.maxInflightMsgs ?? ""}
          onChange={(e) => update({ maxInflightMsgs: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.maxInflightMsgs)}
        />
      </FormField>
      <FormField
        label="Tick Interval"
        description="Duration of a single Raft tick (e.g. 100ms). This is the base time unit for election and heartbeat timeouts."
        htmlFor="raft-tick"
      >
        <Input
          id="raft-tick"
          value={value.tickInterval ?? ""}
          onChange={(e) => update({ tickInterval: e.target.value || undefined })}
          placeholder={defaultPlaceholder(defaults?.tickInterval, "e.g. 100ms")}
        />
      </FormField>
      <FormField
        label="Propose Queue Capacity"
        description="Maximum number of pending proposals queued before back-pressure is applied. Prevents memory exhaustion under burst writes."
        htmlFor="raft-propose-cap"
      >
        <Input
          id="raft-propose-cap"
          type="number"
          value={value.proposeQueueCapacity ?? ""}
          onChange={(e) => update({ proposeQueueCapacity: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.proposeQueueCapacity)}
        />
      </FormField>
      <FormField
        label="Learner Promotion Threshold"
        description="Number of entries a learner node must replicate before being promoted to a full voting member."
        htmlFor="raft-learner"
      >
        <Input
          id="raft-learner"
          type="number"
          value={value.learnerPromotionThreshold ?? ""}
          onChange={(e) => update({ learnerPromotionThreshold: numVal(e.target.value) })}
          placeholder={defaultPlaceholder(defaults?.learnerPromotionThreshold)}
        />
      </FormField>
    </FormSection>
  );
}
