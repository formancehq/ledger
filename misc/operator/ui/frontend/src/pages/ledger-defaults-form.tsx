import { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import type { LedgerDefaultsSpec, LedgerDefaultsConfig } from "shared";
import {
  useLedgerDefaultsDetail,
  useCreateLedgerDefaults,
  useUpdateLedgerDefaults,
} from "@/api/hooks";
import { Accordion } from "@/components/ui/accordion";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { FormField } from "@/components/forms/form-field";
import { FormSection } from "@/components/forms/form-section";
import { ImageSection } from "@/components/forms/sections/image-section";
import { PebbleSection } from "@/components/forms/sections/pebble-section";
import { RaftSection } from "@/components/forms/sections/raft-section";
import { HealthSection } from "@/components/forms/sections/health-section";
import { SecuritySection } from "@/components/forms/sections/security-section";
import { MonitoringSection } from "@/components/forms/sections/monitoring-section";
import { SchedulingSection } from "@/components/forms/sections/scheduling-section";
import { ColdStorageSection } from "@/components/forms/sections/cold-storage-section";
import { computeDiff, formatValue } from "@/lib/diff";
import { toast } from "@/lib/use-toast";
import { ArrowLeft, Save } from "lucide-react";
import { PageWithInfo, InfoSection } from "@/components/info-panel";

export function LedgerDefaultsFormPage() {
  const { name } = useParams<{ name?: string }>();
  const navigate = useNavigate();
  const isEdit = !!name;

  const { data: detailData, isLoading } = useLedgerDefaultsDetail(name ?? "");
  const createMutation = useCreateLedgerDefaults();
  const updateMutation = useUpdateLedgerDefaults(name ?? "");

  const [formName, setFormName] = useState("");
  const [spec, setSpec] = useState<Partial<LedgerDefaultsSpec>>({});
  const [initialized, setInitialized] = useState(false);
  const [diffOpen, setDiffOpen] = useState(false);

  // Initialize form from existing data in edit mode
  if (isEdit && detailData && !initialized) {
    setSpec(structuredClone(detailData.ledgerDefaults.spec));
    setInitialized(true);
  }

  const updateConfig = (patch: Partial<LedgerDefaultsConfig>) =>
    setSpec((prev) => ({ ...prev, config: { ...prev.config, ...patch } }));

  const handleSave = () => {
    if (isEdit) {
      setDiffOpen(true);
    } else {
      doCreate();
    }
  };

  const doCreate = () => {
    if (!formName) return;
    createMutation.mutate(
      { metadata: { name: formName }, spec },
      {
        onSuccess: () => {
          toast({ title: "LedgerDefaults created", description: formName });
          navigate(`/ledger-defaults/${formName}`);
        },
        onError: (err) => {
          toast({
            title: "Failed to create",
            description: err.message,
            variant: "destructive",
          });
        },
      }
    );
  };

  const doUpdate = () => {
    updateMutation.mutate(
      { spec },
      {
        onSuccess: () => {
          toast({ title: "LedgerDefaults updated", description: name });
          setDiffOpen(false);
          navigate(`/ledger-defaults/${name}`);
        },
        onError: (err) => {
          toast({
            title: "Failed to update",
            description: err.message,
            variant: "destructive",
          });
        },
      }
    );
  };

  if (isEdit && isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  const originalSpec = detailData?.ledgerDefaults.spec ?? {};
  const diffs = isEdit
    ? computeDiff(
        originalSpec as unknown as Record<string, unknown>,
        spec as unknown as Record<string, unknown>
      )
    : [];

  const isPending = createMutation.isPending || updateMutation.isPending;

  return (
    <PageWithInfo
      info={
        <>
          <InfoSection title={isEdit ? "Editing" : "Creating"}>
            <p>
              {isEdit
                ? "Modify this shared configuration. Changes will affect all LedgerServices that reference it. A diff preview will be shown before applying."
                : "Define a shared configuration template. LedgerServices can reference it to inherit these settings."}
            </p>
          </InfoSection>
          <InfoSection title="Sections">
            <p><strong>Image</strong> &mdash; container image and pull policy.</p>
            <p><strong>Pebble</strong> &mdash; storage engine tuning (cache, compaction, write buffer).</p>
            <p><strong>Raft</strong> &mdash; consensus protocol timeouts and snapshotting.</p>
            <p><strong>Health</strong> &mdash; liveness/readiness probe configuration.</p>
            <p><strong>Security</strong> &mdash; TLS and response signing.</p>
            <p><strong>Monitoring</strong> &mdash; OpenTelemetry traces and metrics.</p>
            <p><strong>Cold Storage</strong> &mdash; offloading older data to cheaper storage.</p>
            <p><strong>Scheduling</strong> &mdash; resource limits, anti-affinity, PDB.</p>
          </InfoSection>
          <InfoSection title="How it works">
            <p>
              Leave fields empty to use built-in defaults. LedgerServices that reference this
              configuration inherit all values, but can override individual settings.
            </p>
          </InfoSection>
        </>
      }
    >
    <div className="max-w-4xl">
      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <Button
          variant="ghost"
          size="icon"
          onClick={() =>
            navigate(isEdit ? `/ledger-defaults/${name}` : "/ledger-defaults")
          }
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1">
          <h1 className="text-2xl font-bold">
            {isEdit ? `Edit ${name}` : "Create Configuration"}
          </h1>
          <p className="text-sm text-muted-foreground">
            Cluster-scoped default configuration
          </p>
        </div>
        <Button onClick={handleSave} disabled={isPending || (!isEdit && !formName)}>
          <Save className="h-4 w-4" />
          {isPending ? "Saving..." : isEdit ? "Save Changes" : "Create"}
        </Button>
      </div>

      {/* Form */}
      <Accordion type="multiple" defaultValue={["general"]}>
        {/* General (name for create only) */}
        {!isEdit && (
          <FormSection value="general" title="General" description="A unique name for this configuration. LedgerServices reference it by this name (e.g. &quot;default&quot;, &quot;production&quot;, &quot;staging&quot;).">
            <FormField label="Name" htmlFor="ld-name">
              <Input
                id="ld-name"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                placeholder="default"
              />
            </FormField>
          </FormSection>
        )}

        <ImageSection
          value={spec.image}
          onChange={(image) => setSpec((prev) => ({ ...prev, image }))}
        />
        <PebbleSection
          value={spec.config?.pebble}
          onChange={(pebble) => updateConfig({ pebble })}
        />
        <RaftSection
          value={spec.config?.raft}
          onChange={(raft) => updateConfig({ raft })}
        />
        <HealthSection
          value={spec.config?.health}
          onChange={(health) => updateConfig({ health })}
        />
        <SecuritySection
          tls={spec.config?.tls}
          responseSigning={spec.config?.responseSigning}
          onTlsChange={(tls) => updateConfig({ tls })}
          onResponseSigningChange={(responseSigning) =>
            updateConfig({ responseSigning })
          }
        />
        <MonitoringSection
          value={spec.config?.monitoring}
          onChange={(monitoring) => updateConfig({ monitoring })}
        />
        <ColdStorageSection
          value={spec.config?.coldStorage}
          onChange={(coldStorage) => updateConfig({ coldStorage })}
        />
        <SchedulingSection
          resources={spec.resources}
          podAntiAffinity={spec.podAntiAffinity}
          podDisruptionBudget={spec.podDisruptionBudget}
          onResourcesChange={(resources) =>
            setSpec((prev) => ({ ...prev, resources }))
          }
          onAntiAffinityChange={(podAntiAffinity) =>
            setSpec((prev) => ({ ...prev, podAntiAffinity }))
          }
          onPdbChange={(podDisruptionBudget) =>
            setSpec((prev) => ({ ...prev, podDisruptionBudget }))
          }
        />
      </Accordion>

      {/* Diff Preview Dialog */}
      <Dialog open={diffOpen} onOpenChange={setDiffOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Review Changes</DialogTitle>
            <DialogDescription>
              {diffs.length === 0
                ? "No changes detected."
                : `${diffs.length} field${diffs.length !== 1 ? "s" : ""} changed.`}
            </DialogDescription>
          </DialogHeader>
          {diffs.length > 0 && (
            <div className="max-h-80 overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Field</TableHead>
                    <TableHead>Old</TableHead>
                    <TableHead>New</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {diffs.map((d) => (
                    <TableRow key={d.path}>
                      <TableCell className="font-mono text-xs">{d.path}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {formatValue(d.oldValue)}
                      </TableCell>
                      <TableCell className="text-xs font-medium">
                        {formatValue(d.newValue)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDiffOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={doUpdate}
              disabled={diffs.length === 0 || updateMutation.isPending}
            >
              {updateMutation.isPending ? "Applying..." : "Apply Changes"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
    </PageWithInfo>
  );
}
