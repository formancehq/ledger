import { useState } from "react";
import { Link, useParams, useNavigate } from "react-router-dom";
import { useLedgerDefaultsDetail, useCreateLedgerDefaults, useDeleteLedgerDefaults } from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { formatAge, formatImage } from "@/lib/utils";
import { toast } from "@/lib/use-toast";
import { ArrowLeft, Copy, Pencil, Trash2 } from "lucide-react";

export function LedgerDefaultsDetailPage() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const { data, isLoading } = useLedgerDefaultsDetail(name ?? "");
  const createMutation = useCreateLedgerDefaults();
  const deleteMutation = useDeleteLedgerDefaults();

  const [duplicateOpen, setDuplicateOpen] = useState(false);
  const [duplicateName, setDuplicateName] = useState("");
  const [deleteOpen, setDeleteOpen] = useState(false);

  const handleDuplicate = () => {
    if (!duplicateName || !data) return;
    createMutation.mutate(
      { metadata: { name: duplicateName }, spec: structuredClone(data.ledgerDefaults.spec) },
      {
        onSuccess: () => {
          toast({ title: "Configuration duplicated", description: duplicateName });
          setDuplicateOpen(false);
          setDuplicateName("");
          navigate(`/ledger-defaults/${duplicateName}`);
        },
        onError: (err) => {
          toast({
            title: "Failed to duplicate",
            description: err.message,
            variant: "destructive",
          });
        },
      }
    );
  };

  const handleDelete = () => {
    deleteMutation.mutate(name ?? "", {
      onSuccess: () => {
        toast({ title: "Deleted", description: name });
        navigate("/ledger-defaults");
      },
      onError: (err) => {
        toast({
          title: "Delete failed",
          description: err.message,
          variant: "destructive",
        });
      },
    });
  };

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (!data) {
    return <div className="text-muted-foreground">LedgerDefaults not found.</div>;
  }

  const { ledgerDefaults: d, referencedBy } = data;

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => navigate("/ledger-defaults")}
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1">
          <h1 className="text-2xl font-bold">{d.metadata.name}</h1>
          <p className="text-sm text-muted-foreground">
            Cluster-scoped &middot; Age: {formatAge(d.metadata.creationTimestamp)}
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" asChild>
            <Link to={`/ledger-defaults/${name}/edit`}>
              <Pencil className="h-4 w-4" />
              Edit
            </Link>
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setDuplicateName(`${name}-copy`);
              setDuplicateOpen(true);
            }}
          >
            <Copy className="h-4 w-4" />
            Duplicate
          </Button>
          <Button
            variant="destructive"
            size="sm"
            onClick={() => setDeleteOpen(true)}
          >
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
        </div>
      </div>

      <div className="space-y-4">
        {/* Overview */}
        <Card>
          <CardHeader>
            <CardTitle>Overview</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <dt className="text-muted-foreground">Image</dt>
                <dd className="font-mono text-xs">
                  {formatImage(d.spec.image)}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Resources (requests)</dt>
                <dd>
                  {d.spec.resources?.requests
                    ? `CPU: ${d.spec.resources.requests.cpu ?? "-"}, Memory: ${d.spec.resources.requests.memory ?? "-"}`
                    : "-"}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">TLS</dt>
                <dd>{d.spec.config?.tls?.enabled ? "Enabled" : "Disabled"}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Monitoring</dt>
                <dd>
                  {d.spec.config?.monitoring?.traces?.enabled ? "Traces " : ""}
                  {d.spec.config?.monitoring?.metrics?.enabled ? "Metrics " : ""}
                  {!d.spec.config?.monitoring?.traces?.enabled &&
                  !d.spec.config?.monitoring?.metrics?.enabled
                    ? "-"
                    : ""}
                </dd>
              </div>
            </dl>
          </CardContent>
        </Card>

        {/* Full Spec */}
        <Card>
          <CardHeader>
            <CardTitle>Full Spec</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="text-xs bg-muted p-4 rounded-md overflow-auto max-h-96">
              {JSON.stringify(d.spec, null, 2)}
            </pre>
          </CardContent>
        </Card>

        {/* Referencing LedgerServices */}
        <Card>
          <CardHeader>
            <CardTitle>
              Referenced By ({referencedBy.length} LedgerService
              {referencedBy.length !== 1 ? "s" : ""})
            </CardTitle>
          </CardHeader>
          <CardContent>
            {!referencedBy.length ? (
              <p className="text-muted-foreground">
                No LedgerServices reference this defaults.
              </p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Namespace</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {referencedBy.map((ref) => (
                    <TableRow key={`${ref.namespace}/${ref.name}`}>
                      <TableCell>
                        <Link
                          to={`/namespaces/${ref.namespace}/ledger-services/${ref.name}`}
                          className="text-primary hover:underline"
                        >
                          {ref.name}
                        </Link>
                      </TableCell>
                      <TableCell>{ref.namespace}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Duplicate Dialog */}
      <Dialog open={duplicateOpen} onOpenChange={setDuplicateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Duplicate {name}</DialogTitle>
            <DialogDescription>
              Create a new configuration with the same spec.
            </DialogDescription>
          </DialogHeader>
          <div>
            <Label htmlFor="dup-name">New name</Label>
            <Input
              id="dup-name"
              value={duplicateName}
              onChange={(e) => setDuplicateName(e.target.value)}
              placeholder="my-config-copy"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDuplicateOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={handleDuplicate}
              disabled={!duplicateName || createMutation.isPending}
            >
              {createMutation.isPending ? "Duplicating..." : "Duplicate"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Dialog */}
      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete {name}</DialogTitle>
            <DialogDescription>
              This will delete the LedgerDefaults. {referencedBy.length}{" "}
              LedgerService{referencedBy.length !== 1 ? "s" : ""} currently
              reference this defaults and will lose inherited configuration.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteOpen(false)}>
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
