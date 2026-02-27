import { useMemo, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import type { LedgerServiceSpec, LedgerServiceConfig, LedgerDefaultsSpec } from "shared";
import {
  useLedgerServiceDetail,
  useCreateLedgerService,
  useUpdateLedgerService,
  useLedgerDefaults,
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import { ConfigSection } from "@/components/forms/sections/config-section";
import { PersistenceSection } from "@/components/forms/sections/persistence-section";
import { ServiceSection } from "@/components/forms/sections/service-section";
import { IngressSection } from "@/components/forms/sections/ingress-section";
import { computeDiff, formatValue } from "@/lib/diff";
import { toast } from "@/lib/use-toast";
import { ArrowLeft, Save } from "lucide-react";

export function LedgerServiceFormPage() {
  const { ns, name } = useParams<{ ns: string; name?: string }>();
  const navigate = useNavigate();
  const isEdit = !!name;

  const { data: detailData, isLoading } = useLedgerServiceDetail(
    ns ?? "",
    name ?? ""
  );
  const { data: defaultsList } = useLedgerDefaults();
  const createMutation = useCreateLedgerService(ns ?? "");
  const updateMutation = useUpdateLedgerService(ns ?? "", name ?? "");

  const [formName, setFormName] = useState("");
  const [spec, setSpec] = useState<Partial<LedgerServiceSpec>>({});
  const [initialized, setInitialized] = useState(false);
  const [diffOpen, setDiffOpen] = useState(false);

  // Initialize form from existing data in edit mode
  if (isEdit && detailData && !initialized) {
    setSpec(structuredClone(detailData.ledgerService.spec));
    setInitialized(true);
  }

  // Resolve the LedgerDefaults spec from the selected defaultsRef
  const defaultsSpec: LedgerDefaultsSpec | undefined = useMemo(() => {
    const ref = spec.defaultsRef;
    if (!ref || !defaultsList) return undefined;
    const match = defaultsList.find((d) => d.ledgerDefaults.metadata.name === ref);
    return match?.ledgerDefaults.spec;
  }, [spec.defaultsRef, defaultsList]);

  const updateConfig = (patch: Partial<LedgerServiceConfig>) =>
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
      { metadata: { name: formName, namespace: ns }, spec },
      {
        onSuccess: () => {
          toast({ title: "LedgerService created", description: formName });
          navigate(`/namespaces/${ns}/ledger-services/${formName}`);
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
          toast({ title: "LedgerService updated", description: name });
          setDiffOpen(false);
          navigate(`/namespaces/${ns}/ledger-services/${name}`);
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

  const originalSpec = detailData?.ledgerService.spec ?? {};
  const diffs = isEdit
    ? computeDiff(
        originalSpec as unknown as Record<string, unknown>,
        spec as unknown as Record<string, unknown>
      )
    : [];

  const isPending = createMutation.isPending || updateMutation.isPending;

  return (
    <div className="max-w-4xl">
      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <Button
          variant="ghost"
          size="icon"
          onClick={() =>
            navigate(
              isEdit
                ? `/namespaces/${ns}/ledger-services/${name}`
                : `/namespaces/${ns}/ledger-services`
            )
          }
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1">
          <h1 className="text-2xl font-bold">
            {isEdit ? `Edit ${name}` : "Create LedgerService"}
          </h1>
          <p className="text-sm text-muted-foreground">Namespace: {ns}</p>
        </div>
        <Button onClick={handleSave} disabled={isPending || (!isEdit && !formName)}>
          <Save className="h-4 w-4" />
          {isPending ? "Saving..." : isEdit ? "Save Changes" : "Create"}
        </Button>
      </div>

      {/* Form */}
      <Accordion type="multiple" defaultValue={["general"]}>
        {/* General (always visible) */}
        <FormSection value="general" title="General" description="Name, replicas, and defaults reference">
          {!isEdit && (
            <FormField label="Name" htmlFor="ls-name">
              <Input
                id="ls-name"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                placeholder="my-ledger"
              />
            </FormField>
          )}
          <FormField label="Replicas" htmlFor="ls-replicas">
            <Select
              value={String(spec.replicas ?? "")}
              onValueChange={(v) =>
                setSpec((prev) => ({
                  ...prev,
                  replicas: v ? parseInt(v, 10) : undefined,
                }))
              }
            >
              <SelectTrigger id="ls-replicas">
                <SelectValue placeholder="Default (3)" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1">1</SelectItem>
                <SelectItem value="3">3</SelectItem>
                <SelectItem value="5">5</SelectItem>
                <SelectItem value="7">7</SelectItem>
              </SelectContent>
            </Select>
          </FormField>
          <FormField label="Defaults Reference" htmlFor="ls-defaults-ref">
            <Select
              value={spec.defaultsRef ?? "__none__"}
              onValueChange={(v) =>
                setSpec((prev) => ({ ...prev, defaultsRef: v === "__none__" ? undefined : v }))
              }
            >
              <SelectTrigger id="ls-defaults-ref">
                <SelectValue placeholder="None" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__">None</SelectItem>
                {defaultsList?.map(({ ledgerDefaults: d }) => (
                  <SelectItem key={d.metadata.name} value={d.metadata.name}>
                    {d.metadata.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </FormField>
        </FormSection>

        <ImageSection
          value={spec.image}
          defaults={defaultsSpec?.image}
          onChange={(image) => setSpec((prev) => ({ ...prev, image }))}
        />
        <ConfigSection
          value={spec.config}
          onChange={(config) => setSpec((prev) => ({ ...prev, config }))}
        />
        <PebbleSection
          value={spec.config?.pebble}
          defaults={defaultsSpec?.config?.pebble}
          onChange={(pebble) => updateConfig({ pebble })}
        />
        <RaftSection
          value={spec.config?.raft}
          defaults={defaultsSpec?.config?.raft}
          onChange={(raft) => updateConfig({ raft })}
        />
        <HealthSection
          value={spec.config?.health}
          defaults={defaultsSpec?.config?.health}
          onChange={(health) => updateConfig({ health })}
        />
        <SecuritySection
          tls={spec.config?.tls}
          responseSigning={spec.config?.responseSigning}
          defaultTls={defaultsSpec?.config?.tls}
          defaultResponseSigning={defaultsSpec?.config?.responseSigning}
          onTlsChange={(tls) => updateConfig({ tls })}
          onResponseSigningChange={(responseSigning) =>
            updateConfig({ responseSigning })
          }
        />
        <MonitoringSection
          value={spec.config?.monitoring}
          defaults={defaultsSpec?.config?.monitoring}
          onChange={(monitoring) => updateConfig({ monitoring })}
        />
        <ColdStorageSection
          value={spec.config?.coldStorage}
          defaults={defaultsSpec?.config?.coldStorage}
          onChange={(coldStorage) => updateConfig({ coldStorage })}
        />
        <PersistenceSection
          value={spec.persistence}
          onChange={(persistence) => setSpec((prev) => ({ ...prev, persistence }))}
        />
        <ServiceSection
          service={spec.service}
          headlessService={spec.headlessService}
          onServiceChange={(service) => setSpec((prev) => ({ ...prev, service }))}
          onHeadlessChange={(headlessService) =>
            setSpec((prev) => ({ ...prev, headlessService }))
          }
        />
        <IngressSection
          ingress={spec.ingress}
          ingressGrpc={spec.ingressGrpc}
          onIngressChange={(ingress) => setSpec((prev) => ({ ...prev, ingress }))}
          onIngressGrpcChange={(ingressGrpc) =>
            setSpec((prev) => ({ ...prev, ingressGrpc }))
          }
        />
        <SchedulingSection
          resources={spec.resources}
          podAntiAffinity={spec.podAntiAffinity}
          podDisruptionBudget={spec.podDisruptionBudget}
          defaultResources={defaultsSpec?.resources}
          defaultAntiAffinity={defaultsSpec?.podAntiAffinity}
          defaultPdb={defaultsSpec?.podDisruptionBudget}
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
  );
}
