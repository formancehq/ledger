import { useState } from "react";
import { Link, useParams, useNavigate } from "react-router-dom";
import {
  useLedgerServiceDetail,
  useCreateLedgerService,
  useScaleLedgerService,
  useRestartLedgerService,
  useDeleteLedgerService,
} from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
import { formatAge, formatImage, phaseColor, podStatusColor, eventTypeColor } from "@/lib/utils";
import { toast } from "@/lib/use-toast";
import {
  ArrowLeft,
  Copy,
  Pencil,
  RefreshCw,
  Scaling,
  ScrollText,
  TerminalSquare,
  Trash2,
} from "lucide-react";
import { PodLogsPanel } from "@/components/pod-logs-panel";
import { PodTerminalDialog } from "@/components/pod-terminal-dialog";
import type { PodSummary } from "shared";

export function LedgerServiceDetailPage() {
  const { ns, name } = useParams<{ ns: string; name: string }>();
  const navigate = useNavigate();
  const { data, isLoading } = useLedgerServiceDetail(ns ?? "", name ?? "");
  const createMutation = useCreateLedgerService(ns ?? "");
  const scaleMutation = useScaleLedgerService(ns ?? "", name ?? "");
  const restartMutation = useRestartLedgerService(ns ?? "", name ?? "");
  const deleteMutation = useDeleteLedgerService(ns ?? "");

  const [scaleOpen, setScaleOpen] = useState(false);
  const [scaleValue, setScaleValue] = useState("");
  const [duplicateOpen, setDuplicateOpen] = useState(false);
  const [duplicateName, setDuplicateName] = useState("");
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [logsOpen, setLogsOpen] = useState(false);
  const [terminalOpen, setTerminalOpen] = useState(false);
  const [selectedPod, setSelectedPod] = useState<PodSummary | null>(null);

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (!data) {
    return <div className="text-muted-foreground">LedgerService not found.</div>;
  }

  const { ledgerService: svc, pods, pvcs, services, events } = data;

  const warningEventCount = events.filter((e) => e.type === "Warning").length;

  const handleScale = () => {
    const replicas = parseInt(scaleValue, 10);
    if (isNaN(replicas) || replicas < 1) return;
    scaleMutation.mutate(replicas, {
      onSuccess: () => {
        toast({ title: "Scaled", description: `Replicas set to ${replicas}` });
        setScaleOpen(false);
      },
      onError: (err) => {
        toast({
          title: "Scale failed",
          description: err.message,
          variant: "destructive",
        });
      },
    });
  };

  const handleDuplicate = () => {
    if (!duplicateName || !data) return;
    createMutation.mutate(
      { metadata: { name: duplicateName, namespace: ns }, spec: structuredClone(svc.spec) },
      {
        onSuccess: () => {
          toast({ title: "LedgerService duplicated", description: duplicateName });
          setDuplicateOpen(false);
          setDuplicateName("");
          navigate(`/namespaces/${ns}/ledger-services/${duplicateName}`);
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

  const handleRestart = () => {
    restartMutation.mutate(undefined, {
      onSuccess: () => {
        toast({ title: "Restart initiated", description: name });
      },
      onError: (err) => {
        toast({
          title: "Restart failed",
          description: err.message,
          variant: "destructive",
        });
      },
    });
  };

  const handleDelete = () => {
    deleteMutation.mutate(name ?? "", {
      onSuccess: () => {
        toast({ title: "Deleted", description: name });
        navigate(`/namespaces/${ns}/ledger-services`);
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

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => navigate(`/namespaces/${ns}/ledger-services`)}
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1">
          <h1 className="text-2xl font-bold">{svc.metadata.name}</h1>
          <p className="text-sm text-muted-foreground">
            {ns} &middot;{" "}
            <Badge
              variant="secondary"
              className={phaseColor(svc.status?.phase)}
            >
              {svc.status?.phase || "Pending"}
            </Badge>
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" asChild>
            <Link to={`/namespaces/${ns}/ledger-services/${name}/edit`}>
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
            variant="outline"
            size="sm"
            onClick={() => {
              setScaleValue(String(svc.spec.replicas ?? 3));
              setScaleOpen(true);
            }}
          >
            <Scaling className="h-4 w-4" />
            Scale
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleRestart}
            disabled={restartMutation.isPending}
          >
            <RefreshCw className="h-4 w-4" />
            Restart
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

      {/* Tabs */}
      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="pods">Pods ({pods.length})</TabsTrigger>
          <TabsTrigger value="events">
            Events ({events.length})
            {warningEventCount > 0 && (
              <span className="ml-1 inline-flex items-center rounded-full bg-yellow-200 px-1.5 py-0.5 text-[10px] font-bold text-yellow-900">
                {warningEventCount}
              </span>
            )}
          </TabsTrigger>
          <TabsTrigger value="storage">Storage</TabsTrigger>
          <TabsTrigger value="conditions">Conditions</TabsTrigger>
          <TabsTrigger value="config">Config</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <Card>
            <CardContent className="pt-6">
              <dl className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <dt className="text-muted-foreground">Replicas</dt>
                  <dd className="font-medium">
                    {svc.status?.readyReplicas ?? 0}/{svc.spec.replicas ?? 3}
                  </dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Image</dt>
                  <dd className="font-mono text-xs">
                    {formatImage(svc.spec.image)}
                  </dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Defaults Ref</dt>
                  <dd>{svc.spec.defaultsRef || "-"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Cluster ID</dt>
                  <dd>{svc.spec.config?.clusterID || "default"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">gRPC Port</dt>
                  <dd>{svc.spec.config?.grpcPort ?? 8888}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">HTTP Port</dt>
                  <dd>{svc.spec.config?.httpPort ?? 9000}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Age</dt>
                  <dd>{formatAge(svc.metadata.creationTimestamp)}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Generation</dt>
                  <dd>{svc.metadata.generation ?? "-"}</dd>
                </div>
              </dl>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="pods">
          <Card>
            <CardContent className="pt-6">
              {!pods.length ? (
                <p className="text-muted-foreground">No pods found.</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Ready</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Restarts</TableHead>
                      <TableHead>Node</TableHead>
                      <TableHead>Age</TableHead>
                      <TableHead className="w-24">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pods.map((pod) => (
                      <TableRow key={pod.name}>
                        <TableCell className="font-mono text-xs">
                          {pod.name}
                        </TableCell>
                        <TableCell>{pod.ready}</TableCell>
                        <TableCell>
                          <div className="flex flex-col gap-1">
                            <Badge
                              variant="secondary"
                              className={podStatusColor(pod.status)}
                            >
                              {pod.status}
                            </Badge>
                            {pod.message && (
                              <span className="text-xs text-muted-foreground max-w-xs truncate" title={pod.message}>
                                {pod.message}
                              </span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>{pod.restarts}</TableCell>
                        <TableCell className="text-xs">
                          {pod.node ?? "-"}
                        </TableCell>
                        <TableCell>{formatAge(pod.age)}</TableCell>
                        <TableCell>
                          <div className="flex gap-1">
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              title="View logs"
                              onClick={() => {
                                setSelectedPod(pod);
                                setLogsOpen(true);
                              }}
                            >
                              <ScrollText className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              title="Open terminal"
                              onClick={() => {
                                setSelectedPod(pod);
                                setTerminalOpen(true);
                              }}
                            >
                              <TerminalSquare className="h-4 w-4" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="events">
          <Card>
            <CardContent className="pt-6">
              {!events.length ? (
                <p className="text-muted-foreground">No recent events.</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-20">Type</TableHead>
                      <TableHead>Reason</TableHead>
                      <TableHead>Object</TableHead>
                      <TableHead className="min-w-64">Message</TableHead>
                      <TableHead className="w-16">Count</TableHead>
                      <TableHead>Source</TableHead>
                      <TableHead>Last Seen</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {events.map((ev, i) => (
                      <TableRow key={`${ev.involvedObject}-${ev.reason}-${i}`}>
                        <TableCell>
                          <Badge
                            variant="secondary"
                            className={eventTypeColor(ev.type)}
                          >
                            {ev.type}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-medium">
                          {ev.reason}
                        </TableCell>
                        <TableCell className="font-mono text-xs">
                          {ev.involvedObject}
                        </TableCell>
                        <TableCell className="text-xs max-w-md">
                          {ev.message}
                        </TableCell>
                        <TableCell className="text-center">
                          {ev.count > 1 && (
                            <Badge variant="outline">{ev.count}</Badge>
                          )}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {ev.source ?? "-"}
                        </TableCell>
                        <TableCell>{formatAge(ev.lastTimestamp)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="storage">
          <div className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle>PersistentVolumeClaims</CardTitle>
              </CardHeader>
              <CardContent>
                {!pvcs.length ? (
                  <p className="text-muted-foreground">No PVCs found.</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Name</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Capacity</TableHead>
                        <TableHead>Storage Class</TableHead>
                        <TableHead>Age</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {pvcs.map((pvc) => (
                        <TableRow key={pvc.name}>
                          <TableCell className="font-mono text-xs">
                            {pvc.name}
                          </TableCell>
                          <TableCell>
                            <Badge variant="secondary">{pvc.status}</Badge>
                          </TableCell>
                          <TableCell>{pvc.capacity ?? "-"}</TableCell>
                          <TableCell>{pvc.storageClass ?? "-"}</TableCell>
                          <TableCell>{formatAge(pvc.age)}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Services</CardTitle>
              </CardHeader>
              <CardContent>
                {!services.length ? (
                  <p className="text-muted-foreground">No services found.</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Name</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Cluster IP</TableHead>
                        <TableHead>Ports</TableHead>
                        <TableHead>Age</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {services.map((svc) => (
                        <TableRow key={svc.name}>
                          <TableCell className="font-mono text-xs">
                            {svc.name}
                          </TableCell>
                          <TableCell>{svc.type}</TableCell>
                          <TableCell>{svc.clusterIP ?? "-"}</TableCell>
                          <TableCell>{svc.ports ?? "-"}</TableCell>
                          <TableCell>{formatAge(svc.age)}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="conditions">
          <Card>
            <CardContent className="pt-6">
              {!svc.status?.conditions?.length ? (
                <p className="text-muted-foreground">No conditions reported.</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Type</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Reason</TableHead>
                      <TableHead>Message</TableHead>
                      <TableHead>Last Transition</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {svc.status.conditions.map((cond) => (
                      <TableRow key={cond.type}>
                        <TableCell className="font-medium">
                          {cond.type}
                        </TableCell>
                        <TableCell>
                          <Badge
                            variant="secondary"
                            className={
                              cond.status === "True"
                                ? "bg-green-100 text-green-800"
                                : "bg-gray-100 text-gray-600"
                            }
                          >
                            {cond.status}
                          </Badge>
                        </TableCell>
                        <TableCell>{cond.reason ?? "-"}</TableCell>
                        <TableCell className="max-w-xs truncate">
                          {cond.message ?? "-"}
                        </TableCell>
                        <TableCell>
                          {formatAge(cond.lastTransitionTime)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="config">
          <Card>
            <CardContent className="pt-6">
              <pre className="text-xs bg-muted p-4 rounded-md overflow-auto max-h-96">
                {JSON.stringify(svc.spec, null, 2)}
              </pre>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Scale Dialog */}
      <Dialog open={scaleOpen} onOpenChange={setScaleOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Scale {name}</DialogTitle>
            <DialogDescription>Set the desired replica count.</DialogDescription>
          </DialogHeader>
          <div>
            <Label htmlFor="replicas">Replicas</Label>
            <Input
              id="replicas"
              type="number"
              min={1}
              value={scaleValue}
              onChange={(e) => setScaleValue(e.target.value)}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setScaleOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={handleScale}
              disabled={scaleMutation.isPending}
            >
              {scaleMutation.isPending ? "Scaling..." : "Scale"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Duplicate Dialog */}
      <Dialog open={duplicateOpen} onOpenChange={setDuplicateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Duplicate {name}</DialogTitle>
            <DialogDescription>
              Create a new LedgerService with the same spec.
            </DialogDescription>
          </DialogHeader>
          <div>
            <Label htmlFor="dup-name">New name</Label>
            <Input
              id="dup-name"
              value={duplicateName}
              onChange={(e) => setDuplicateName(e.target.value)}
              placeholder="my-ledger-copy"
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
              This will delete the LedgerService and all its resources. This
              cannot be undone.
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

      {/* Pod Logs Dialog */}
      {selectedPod && (
        <PodLogsPanel
          open={logsOpen}
          onOpenChange={(open) => {
            setLogsOpen(open);
            if (!open) setSelectedPod(null);
          }}
          namespace={ns ?? ""}
          podName={selectedPod.name}
          containers={selectedPod.containers}
        />
      )}

      {/* Pod Terminal Dialog */}
      {selectedPod && (
        <PodTerminalDialog
          open={terminalOpen}
          onOpenChange={(open) => {
            setTerminalOpen(open);
            if (!open) setSelectedPod(null);
          }}
          namespace={ns ?? ""}
          podName={selectedPod.name}
          containers={selectedPod.containers}
        />
      )}
    </div>
  );
}
