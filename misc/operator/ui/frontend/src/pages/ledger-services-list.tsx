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

  if (!ns) {
    return (
      <div className="text-muted-foreground">
        Select a namespace from the sidebar to view LedgerServices.
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">LedgerServices</h1>
          <p className="text-sm text-muted-foreground">Namespace: {ns}</p>
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
        <div className="text-center py-12 text-muted-foreground">
          No LedgerServices found in namespace "{ns}".
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
  );
}
