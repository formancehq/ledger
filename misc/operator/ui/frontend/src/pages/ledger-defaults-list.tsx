import { useState, useEffect } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useLedgerDefaults, useDeleteLedgerDefaults } from "@/api/hooks";
import { useRole } from "@/auth/use-auth";
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
import { PageWithInfo, InfoSection } from "@/components/info-panel";

export function LedgerDefaultsListPage() {
  const navigate = useNavigate();
  const role = useRole();
  const { data, isLoading } = useLedgerDefaults();
  const deleteMutation = useDeleteLedgerDefaults();

  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  useEffect(() => {
    if (role === "guest") {
      toast({ title: "Access denied", description: "Configurations require admin access.", variant: "destructive" });
      navigate("/", { replace: true });
    }
  }, [role, navigate]);

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
    <PageWithInfo
      info={
        <>
          <InfoSection title="What are Configurations?">
            <p>
              Configurations (LedgerDefaults) are cluster-wide templates that define shared settings
              for your Ledger instances &mdash; container image, resource limits, monitoring, TLS,
              storage engine tuning, and more.
            </p>
            <p>
              When a LedgerService references a configuration, it inherits all its settings automatically.
              Change it once here, and all linked services pick it up.
            </p>
          </InfoSection>
          <InfoSection title="Table columns">
            <p><strong>Referenced By</strong> &mdash; how many LedgerServices currently use this configuration. Click through to see which ones.</p>
            <p><strong>TLS</strong> &mdash; whether TLS encryption is enabled for gRPC/HTTP traffic.</p>
            <p><strong>Monitoring</strong> &mdash; whether OpenTelemetry traces are enabled.</p>
          </InfoSection>
          <InfoSection title="Inheritance">
            <p>
              Configurations are like CSS classes: a LedgerService can reference one to inherit all its settings,
              then override individual values as needed.
            </p>
          </InfoSection>
        </>
      }
    >
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Configurations</h1>
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
        <div className="text-center py-12 text-muted-foreground space-y-2">
          <p>No configurations found yet.</p>
          <p className="text-xs">
            Click &ldquo;Create&rdquo; to define a shared configuration template.
            You can then reference it from any LedgerService to inherit its settings.
          </p>
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
    </PageWithInfo>
  );
}
