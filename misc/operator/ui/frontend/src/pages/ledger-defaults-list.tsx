import { useState } from "react";
import { Link } from "react-router-dom";
import { useLedgerDefaults, useDeleteLedgerDefaults } from "@/api/hooks";
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
import { formatAge, formatImage } from "@/lib/utils";
import { toast } from "@/lib/use-toast";
import { Plus, Trash2 } from "lucide-react";

export function LedgerDefaultsListPage() {
  const { data, isLoading } = useLedgerDefaults();
  const deleteMutation = useDeleteLedgerDefaults();

  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const handleDelete = () => {
    if (!deleteTarget) return;
    deleteMutation.mutate(deleteTarget, {
      onSuccess: () => {
        toast({ title: "LedgerDefaults deleted", description: deleteTarget });
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

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">LedgerDefaults</h1>
          <p className="text-sm text-muted-foreground">
            Cluster-scoped default configurations
          </p>
        </div>
        <Button asChild>
          <Link to="/ledger-defaults/new">
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
          No LedgerDefaults found.
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Image</TableHead>
              <TableHead>Resources</TableHead>
              <TableHead>Monitoring</TableHead>
              <TableHead>TLS</TableHead>
              <TableHead>Referenced By</TableHead>
              <TableHead>Age</TableHead>
              <TableHead className="w-12"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.map(({ ledgerDefaults: d, referencedByCount }) => (
              <TableRow key={d.metadata.name}>
                <TableCell>
                  <Link
                    to={`/ledger-defaults/${d.metadata.name}`}
                    className="font-medium text-primary hover:underline"
                  >
                    {d.metadata.name}
                  </Link>
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {formatImage(d.spec.image)}
                </TableCell>
                <TableCell className="text-xs">
                  {d.spec.resources?.requests?.cpu || d.spec.resources?.requests?.memory
                    ? `${d.spec.resources?.requests?.cpu ?? "-"} / ${d.spec.resources?.requests?.memory ?? "-"}`
                    : "-"}
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">
                    {d.spec.config?.monitoring?.traces?.enabled
                      ? "Traces"
                      : "-"}
                  </Badge>
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">
                    {d.spec.config?.tls?.enabled ? "Enabled" : "Off"}
                  </Badge>
                </TableCell>
                <TableCell>
                  <Badge variant="outline">{referencedByCount}</Badge>
                </TableCell>
                <TableCell>
                  {formatAge(d.metadata.creationTimestamp)}
                </TableCell>
                <TableCell>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setDeleteTarget(d.metadata.name)}
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
            <DialogTitle>Delete LedgerDefaults</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete "{deleteTarget}"? LedgerServices
              referencing this defaults will lose their inherited configuration.
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
