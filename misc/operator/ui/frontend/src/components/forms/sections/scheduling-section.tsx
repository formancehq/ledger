import type {
  ResourceRequirements,
  PodAntiAffinitySpec,
  PodDisruptionBudgetSpec,
} from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { defaultPlaceholder, defaultHint } from "@/lib/defaults";

interface SchedulingSectionProps {
  resources?: ResourceRequirements;
  podAntiAffinity?: PodAntiAffinitySpec;
  podDisruptionBudget?: PodDisruptionBudgetSpec;
  defaultResources?: ResourceRequirements;
  defaultAntiAffinity?: PodAntiAffinitySpec;
  defaultPdb?: PodDisruptionBudgetSpec;
  onResourcesChange: (value: ResourceRequirements) => void;
  onAntiAffinityChange: (value: PodAntiAffinitySpec) => void;
  onPdbChange: (value: PodDisruptionBudgetSpec) => void;
}

function numVal(v: string): number | undefined {
  if (v === "") return undefined;
  const n = Number(v);
  return isNaN(n) ? undefined : n;
}

export function SchedulingSection({
  resources = {},
  podAntiAffinity = {},
  podDisruptionBudget = {},
  defaultResources,
  defaultAntiAffinity,
  defaultPdb,
  onResourcesChange,
  onAntiAffinityChange,
  onPdbChange,
}: SchedulingSectionProps) {
  const updateRequests = (key: string, val: string) => {
    const requests = { ...resources.requests };
    if (val) requests[key] = val;
    else delete requests[key];
    onResourcesChange({ ...resources, requests });
  };
  const updateLimits = (key: string, val: string) => {
    const limits = { ...resources.limits };
    if (val) limits[key] = val;
    else delete limits[key];
    onResourcesChange({ ...resources, limits });
  };

  return (
    <FormSection value="scheduling" title="Scheduling" description="Resources, anti-affinity, and disruption budgets">
      {/* Resources */}
      <FormField
        label="CPU Request"
        description="Guaranteed CPU allocation per pod (e.g. 500m = 0.5 cores). Kubernetes uses this for scheduling decisions."
        htmlFor="res-cpu-req"
      >
        <Input
          id="res-cpu-req"
          value={resources.requests?.cpu ?? ""}
          onChange={(e) => updateRequests("cpu", e.target.value)}
          placeholder={defaultPlaceholder(defaultResources?.requests?.cpu, "500m")}
        />
      </FormField>
      <FormField
        label="Memory Request"
        description="Guaranteed memory allocation per pod (e.g. 512Mi). Set based on cache size + working set. Pods are evicted if they exceed limits."
        htmlFor="res-mem-req"
      >
        <Input
          id="res-mem-req"
          value={resources.requests?.memory ?? ""}
          onChange={(e) => updateRequests("memory", e.target.value)}
          placeholder={defaultPlaceholder(defaultResources?.requests?.memory, "512Mi")}
        />
      </FormField>
      <FormField
        label="CPU Limit"
        description="Maximum CPU the pod can use (e.g. 1000m = 1 core). Pod is throttled if it exceeds this. Leave empty for no limit."
        htmlFor="res-cpu-lim"
      >
        <Input
          id="res-cpu-lim"
          value={resources.limits?.cpu ?? ""}
          onChange={(e) => updateLimits("cpu", e.target.value)}
          placeholder={defaultPlaceholder(defaultResources?.limits?.cpu, "1000m")}
        />
      </FormField>
      <FormField
        label="Memory Limit"
        description="Maximum memory the pod can use (e.g. 1Gi). Pod is OOM-killed if it exceeds this. Should be >= memory request."
        htmlFor="res-mem-lim"
      >
        <Input
          id="res-mem-lim"
          value={resources.limits?.memory ?? ""}
          onChange={(e) => updateLimits("memory", e.target.value)}
          placeholder={defaultPlaceholder(defaultResources?.limits?.memory, "1Gi")}
        />
      </FormField>

      {/* Pod Anti-Affinity */}
      <FormField
        label="Anti-Affinity Enabled"
        description="Spread Raft replicas across different nodes to survive node failures. Strongly recommended for production."
        htmlFor="aa-enabled"
        hint={defaultHint(defaultAntiAffinity?.enabled)}
      >
        <Switch
          id="aa-enabled"
          checked={podAntiAffinity.enabled ?? false}
          onCheckedChange={(checked) =>
            onAntiAffinityChange({ ...podAntiAffinity, enabled: checked || undefined })
          }
        />
      </FormField>
      <FormField
        label="Anti-Affinity Type"
        description="'Soft' tries to spread pods but allows co-location if necessary. 'Hard' strictly prevents co-location (may block scheduling)."
        htmlFor="aa-type"
        hint={defaultAntiAffinity?.type ? `Default: ${defaultAntiAffinity.type}` : undefined}
      >
        <Select
          value={podAntiAffinity.type ?? ""}
          onValueChange={(v) =>
            onAntiAffinityChange({ ...podAntiAffinity, type: v || undefined })
          }
        >
          <SelectTrigger id="aa-type">
            <SelectValue placeholder={defaultAntiAffinity?.type ? `${defaultAntiAffinity.type} (default)` : "Default"} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="soft">Soft (preferred)</SelectItem>
            <SelectItem value="hard">Hard (required)</SelectItem>
          </SelectContent>
        </Select>
      </FormField>

      {/* PDB */}
      <FormField
        label="PDB Enabled"
        description="Create a PodDisruptionBudget to protect against voluntary disruptions (node drains, cluster upgrades)."
        htmlFor="pdb-enabled"
        hint={defaultHint(defaultPdb?.enabled)}
      >
        <Switch
          id="pdb-enabled"
          checked={podDisruptionBudget.enabled ?? false}
          onCheckedChange={(checked) =>
            onPdbChange({ ...podDisruptionBudget, enabled: checked || undefined })
          }
        />
      </FormField>
      <FormField
        label="PDB Min Available"
        description="Minimum number of pods that must stay running during disruptions. Ensures Raft quorum is maintained."
        htmlFor="pdb-min"
      >
        <Input
          id="pdb-min"
          type="number"
          value={podDisruptionBudget.minAvailable ?? ""}
          onChange={(e) =>
            onPdbChange({ ...podDisruptionBudget, minAvailable: numVal(e.target.value) })
          }
          placeholder={defaultPlaceholder(defaultPdb?.minAvailable)}
        />
      </FormField>
      <FormField
        label="PDB Max Unavailable"
        description="Maximum number of pods that can be down simultaneously. Alternative to Min Available. Use one or the other, not both."
        htmlFor="pdb-max"
      >
        <Input
          id="pdb-max"
          type="number"
          value={podDisruptionBudget.maxUnavailable ?? ""}
          onChange={(e) =>
            onPdbChange({ ...podDisruptionBudget, maxUnavailable: numVal(e.target.value) })
          }
          placeholder={defaultPlaceholder(defaultPdb?.maxUnavailable)}
        />
      </FormField>
    </FormSection>
  );
}
