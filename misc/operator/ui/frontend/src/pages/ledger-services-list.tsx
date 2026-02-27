import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useLedgerServices, useDeleteLedgerService } from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { formatAge, formatImage, phaseColor } from "@/lib/utils";
import { toast } from "@/lib/use-toast";
import { Plus, Trash2 } from "lucide-react";
import { PageWithInfo, InfoSection } from "@/components/info-panel";

export function LedgerServicesListPage() {
  const { ns } = useParams<{ ns: string }>();
  const { data, isLoading } = useLedgerServices(ns ?? "");
  const deleteMutation = useDeleteLedgerService(ns ?? "");

  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const handleDelete = () => {
    if (!deleteTarget) return;
    deleteMutation.mutate(deleteTarget, {
      onSuccess: () => {
        toast({ title: "LedgerService deleted", description: deleteTarget });
        setDeleteTarget(null);
      },
      onError: (err) => {
        toast({
          title: "Failed to delete",
          description: err.message,
          variant: "destructive",
        });
      },
    });
  };

  const infoContent = (
    <>
      <InfoSection title="What is a LedgerService?">
        <p>
          A LedgerService is a running instance of the Formance Ledger in your Kubernetes cluster.
          Each one manages its own data, Raft consensus group, and storage.
        </p>
        <p>
          You can have multiple LedgerServices in the same namespace (e.g. one for staging, one for production).
        </p>
      </InfoSection>
      <InfoSection title="Namespaces">
        <p>
          LedgerServices are namespace-scoped resources &mdash; each namespace can contain
          its own independent set of Ledger instances. Use the namespace selector in the sidebar
          to switch between namespaces.
        </p>
      </InfoSection>
      <InfoSection title="Table columns">
        <p><strong>Replicas</strong> &mdash; ready pods / desired pods in the Raft cluster.</p>
        <p><strong>Phase</strong> &mdash; current lifecycle state (Pending, Running, Failed).</p>
        <p><strong>Defaults</strong> &mdash; the LedgerDefaults configuration this service inherits from.</p>
        <p><strong>Owner</strong> &mdash; the user who created this service (from OIDC auth). Shows "-" when auth is disabled.</p>
      </InfoSection>
    </>
  );

  if (!ns) {
    return (
      <PageWithInfo info={infoContent}>
        <div className="text-muted-foreground space-y-2">
          <p className="text-lg font-medium">No namespace selected</p>
          <p>
            Use the namespace selector in the sidebar to pick a Kubernetes namespace.
          </p>
        </div>
      </PageWithInfo>
    );
  }

  return (
    <PageWithInfo info={infoContent}>
      <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">LedgerServices</h1>
          <p className="text-xs text-muted-foreground mt-1">Namespace: {ns}</p>
        </div>
        <Button asChild>
          <Link to={`/namespaces/${ns}/ledger-services/new`}>
            <Plus className="h-4 w-4" />
            Create
          </Link>
        </Button>
      </div>

      {isLoading ? (
        <div className="space-y-2">
          {[...Array(3)].map((_, i) => (
            <Skeleton key={i} className="h-12 w-full" />
          ))}
        </div>
      ) : !data?.length ? (
        <div className="text-center py-12 text-muted-foreground space-y-2">
          <p>No LedgerServices found in namespace &ldquo;{ns}&rdquo;.</p>
          <p className="text-xs">
            Click &ldquo;Create&rdquo; to deploy a new Ledger instance, or select a different namespace from the sidebar.
          </p>
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Replicas</TableHead>
              <TableHead>Phase</TableHead>
              <TableHead>Image</TableHead>
              <TableHead>Defaults</TableHead>
              <TableHead>Owner</TableHead>
              <TableHead>Age</TableHead>
              <TableHead className="w-12"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.map((svc) => (
              <TableRow key={svc.metadata.name}>
                <TableCell>
                  <Link
                    to={`/namespaces/${ns}/ledger-services/${svc.metadata.name}`}
                    className="font-medium text-primary hover:underline"
                  >
                    {svc.metadata.name}
                  </Link>
                </TableCell>
                <TableCell>
                  {svc.status?.readyReplicas ?? 0}/{svc.spec.replicas ?? 3}
                </TableCell>
                <TableCell>
                  <Badge
                    variant="secondary"
                    className={phaseColor(svc.status?.phase)}
                  >
                    {svc.status?.phase || "Pending"}
                  </Badge>
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {formatImage(svc.spec.image)}
                </TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {svc.spec.defaultsRef || "-"}
                </TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {svc.metadata.annotations?.["ledger.formance.com/created-by-email"]
                    ?? svc.metadata.annotations?.["ledger.formance.com/created-by"]
                    ?? "-"}
                </TableCell>
                <TableCell>
                  {formatAge(svc.metadata.creationTimestamp)}
                </TableCell>
                <TableCell>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setDeleteTarget(svc.metadata.name)}
                  >
                    <Trash2 className="h-4 w-4 text-destructive" />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      {/* Delete Dialog */}
      <Dialog
        open={deleteTarget !== null}
        onOpenChange={(open) => !open && setDeleteTarget(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete LedgerService</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete "{deleteTarget}"? This action
              cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? "Deleting..." : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      </div>
    </PageWithInfo>
  );
}
