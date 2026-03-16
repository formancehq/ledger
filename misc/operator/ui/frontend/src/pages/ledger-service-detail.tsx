import { useState } from "react";
import { Link, useParams, useNavigate } from "react-router-dom";
import {
  useLedgerServiceDetail,
  useCreateLedgerService,
  useScaleLedgerService,
  useRestartLedgerService,
  useDeleteLedgerService,
  useConnectInfo,
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
  Check,
  Copy,
  Download,
  Pencil,
  Plug,
  RefreshCw,
  Scaling,
  ScrollText,
  TerminalSquare,
  Trash2,
} from "lucide-react";
import { PodLogsPanel } from "@/components/pod-logs-panel";
import { PodTerminalDialog } from "@/components/pod-terminal-dialog";
import { PageWithInfo, InfoSection } from "@/components/info-panel";
import { useAuth, useRole } from "@/auth/use-auth";
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
  const [copiedField, setCopiedField] = useState<string | null>(null);
  const role = useRole();
  const isGuest = role === "guest";
  const { data: authStatus } = useAuth();
  const connectQuery = useConnectInfo(ns ?? "", name ?? "");

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
    <PageWithInfo
      info={
        <>
          <InfoSection title="LedgerService">
            <p>
              A running Ledger instance managed by the operator. Each service
              runs a Raft consensus cluster with the configured number of replicas.
            </p>
          </InfoSection>
          <InfoSection title="Connect tab">
            <p>
              Shows how to connect to this cluster using <code>ledgerctl</code>.
              When authentication is enabled, you can download your key bundle
              and use it to sign requests.
            </p>
          </InfoSection>
          <InfoSection title="Overview tab">
            <p>
              Key properties of this LedgerService. Replicas are the number of pods in the Raft cluster.
              The "Defaults Ref" links to a LedgerDefaults configuration for inherited settings.
            </p>
          </InfoSection>
          <InfoSection title="Pods tab">
            <p>
              Each pod is one node in the Raft consensus cluster. You can view its logs
              or open an interactive terminal shell directly from here.
            </p>
          </InfoSection>
          <InfoSection title="Events tab">
            <p>
              Kubernetes events for this service and its pods. "Normal" events are
              informational (pod scheduled, image pulled). "Warning" events indicate
              problems (crash loops, failed mounts). Events are cleaned up after ~1 hour.
            </p>
          </InfoSection>
          <InfoSection title="Storage tab">
            <p>
              <strong>PVCs</strong> &mdash; each pod gets its own PVC to store Pebble database
              files and WAL data. The storage class determines where data lives (local SSD, EBS, etc.).
            </p>
            <p>
              <strong>Services</strong> &mdash; stable network endpoints to reach the Ledger pods.
              The headless service is used for Raft peer discovery between nodes.
            </p>
          </InfoSection>
          <InfoSection title="Conditions tab">
            <p>
              The operator's view of this service's health. Each condition tracks a specific aspect
              (e.g. "Available", "Progressing"). Status=True means the condition is met.
            </p>
          </InfoSection>
          <InfoSection title="Scaling">
            <p>
              The Raft consensus protocol requires an odd number of replicas (1, 3, 5, 7)
              for clean leader election. More replicas improve fault tolerance and read throughput.
            </p>
          </InfoSection>
        </>
      }
    >
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
          {!isGuest && (
            <Button variant="outline" size="sm" asChild>
              <Link to={`/namespaces/${ns}/ledger-services/${name}/edit`}>
                <Pencil className="h-4 w-4" />
                Edit
              </Link>
            </Button>
          )}
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
      <Tabs defaultValue="connect">
        <TabsList>
          <TabsTrigger value="connect">
            <Plug className="h-3.5 w-3.5 mr-1" />
            Connect
          </TabsTrigger>
          <TabsTrigger value="overview">General</TabsTrigger>
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
          {!isGuest && <TabsTrigger value="config">Config</TabsTrigger>}
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
                <div>
                  <dt className="text-muted-foreground">Owner</dt>
                  <dd>
                    {svc.metadata.annotations?.["ledger.formance.com/created-by-email"]
                      ?? svc.metadata.annotations?.["ledger.formance.com/created-by"]
                      ?? "-"}
                  </dd>
                </div>
              </dl>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="connect">
          <ConnectPanel
            connectQuery={connectQuery}
            authEnabled={authStatus?.enabled ?? false}
            serviceName={name ?? ""}
            namespace={ns ?? ""}
            copiedField={copiedField}
            onCopy={(field, text) => {
              navigator.clipboard.writeText(text);
              setCopiedField(field);
              setTimeout(() => setCopiedField(null), 2000);
            }}
          />
        </TabsContent>

        <TabsContent value="pods">
          <Card>
            <CardContent className="pt-6">
              {!pods.length ? (
                <p className="text-muted-foreground">No pods found. The operator may still be creating them.</p>
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
                <p className="text-muted-foreground">No recent events &mdash; everything looks normal.</p>
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
                  <p className="text-muted-foreground">No PVCs found. Persistence may be disabled for this service.</p>
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
                <p className="text-muted-foreground">No conditions reported yet. The operator may still be reconciling.</p>
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

        {!isGuest && (
          <TabsContent value="config">
            <Card>
              <CardContent className="pt-6">
                <pre className="text-xs bg-muted p-4 rounded-md overflow-auto max-h-96">
                  {JSON.stringify(svc.spec, null, 2)}
                </pre>
              </CardContent>
            </Card>
          </TabsContent>
        )}
      </Tabs>

      {/* Scale Dialog */}
      <Dialog open={scaleOpen} onOpenChange={setScaleOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Scale {name}</DialogTitle>
            <DialogDescription>
              Set the number of Raft cluster nodes. Use an odd number (1, 3, 5, 7)
              for clean leader election. Changes take effect immediately.
            </DialogDescription>
          </DialogHeader>
          <div>
            <Label htmlFor="replicas">Replicas</Label>
            <Input
              id="replicas"
              type="number"
              min={1}
              max={isGuest ? 5 : undefined}
              value={scaleValue}
              onChange={(e) => setScaleValue(e.target.value)}
            />
            {isGuest && (
              <p className="text-xs text-muted-foreground mt-1">
                Guest accounts can scale up to 5 replicas. Contact an admin for more.
              </p>
            )}
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
              Create a new independent LedgerService with the same configuration.
              It will have its own data and storage.
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
              This will permanently delete the LedgerService, its pods, and services.
              PVCs may be retained depending on the reclaim policy. This cannot be undone.
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
    </PageWithInfo>
  );
}

/* ------------------------------------------------------------------ */
/*  Connect Panel                                                      */
/* ------------------------------------------------------------------ */

function CopyButton({
  field,
  text,
  copiedField,
  onCopy,
}: {
  field: string;
  text: string;
  copiedField: string | null;
  onCopy: (field: string, text: string) => void;
}) {
  const copied = copiedField === field;
  return (
    <Button
      variant="ghost"
      size="icon"
      className="h-6 w-6 shrink-0"
      title="Copy to clipboard"
      onClick={() => onCopy(field, text)}
    >
      {copied ? <Check className="h-3.5 w-3.5 text-green-600" /> : <Copy className="h-3.5 w-3.5" />}
    </Button>
  );
}

function CodeBlock({
  id,
  code,
  copiedField,
  onCopy,
}: {
  id: string;
  code: string;
  copiedField: string | null;
  onCopy: (field: string, text: string) => void;
}) {
  return (
    <div className="relative group">
      <pre className="text-xs bg-muted p-3 rounded-md overflow-x-auto whitespace-pre-wrap break-all">
        {code}
      </pre>
      <div className="absolute top-2 right-2">
        <CopyButton field={id} text={code} copiedField={copiedField} onCopy={onCopy} />
      </div>
    </div>
  );
}

const RELEASES_URL = "https://github.com/formancehq/ledger/releases";

const INSTALL_SCRIPT_GH = `# Install via GitHub CLI (recommended for private repos)
ARCH=$(uname -m); [ "$ARCH" = "x86_64" ] && ARCH="amd64"
gh release download --repo formancehq/ledger -p "ledger_$(uname -s | tr '[:upper:]' '[:lower:]')-$ARCH.tar.gz" -O - | tar xz
sudo mv ledgerctl /usr/local/bin/`;

const INSTALL_SCRIPT_CURL = `# Or via curl (requires a GitHub token for private repos)
ARCH=$(uname -m); [ "$ARCH" = "x86_64" ] && ARCH="amd64"
curl -sSfL -H "Authorization: token \${GITHUB_TOKEN}" \\
  "https://github.com/formancehq/ledger/releases/latest/download/ledger_$(uname -s | tr '[:upper:]' '[:lower:]')-$ARCH.tar.gz" | tar xz
sudo mv ledgerctl /usr/local/bin/`;

function ConnectPanel({
  connectQuery,
  authEnabled,
  serviceName,
  namespace,
  copiedField,
  onCopy,
}: {
  connectQuery: ReturnType<typeof useConnectInfo>;
  authEnabled: boolean;
  serviceName: string;
  namespace: string;
  copiedField: string | null;
  onCopy: (field: string, text: string) => void;
}) {
  const { data: info, isLoading } = connectQuery;

  if (isLoading) {
    return (
      <Card>
        <CardContent className="pt-6">
          <Skeleton className="h-48 w-full" />
        </CardContent>
      </Card>
    );
  }

  const grpcEndpoint = info?.endpoints?.grpc ?? `${serviceName}.${namespace}.svc.cluster.local:8888`;
  const httpEndpoint = info?.endpoints?.http ?? `http://${serviceName}.${namespace}.svc.cluster.local:9000`;
  const isExternal = info?.endpoints?.external ?? false;

  // When using an Ingress the gRPC endpoint is TLS-terminated — no --insecure.
  // For in-cluster access we skip TLS verification.
  const profileFlags = isExternal
    ? `--server ${grpcEndpoint}`
    : `--server ${grpcEndpoint} --insecure`;

  return (
    <div className="space-y-4">
      {/* Step 1: Install ledgerctl */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">1. Install ledgerctl</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-sm text-muted-foreground">
            Download the CLI for your platform from the{" "}
            <a
              href={RELEASES_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary underline underline-offset-4 hover:text-primary/80"
            >
              GitHub releases page
            </a>.
          </p>
          <CodeBlock id="install-gh" code={INSTALL_SCRIPT_GH} copiedField={copiedField} onCopy={onCopy} />
          <CodeBlock id="install-curl" code={INSTALL_SCRIPT_CURL} copiedField={copiedField} onCopy={onCopy} />
        </CardContent>
      </Card>

      {/* Step 2: Authentication (only when auth enabled) */}
      {authEnabled && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">2. Authenticate with your key bundle</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {!info?.available ? (
              <div className="text-sm text-muted-foreground bg-muted p-3 rounded-md">
                {info?.reason ?? "Key bundle not available yet."}
                {info?.agentPhase && info.agentPhase !== "Ready" && (
                  <span className="block mt-1">
                    Agent status: <Badge variant="secondary">{info.agentPhase}</Badge>
                  </span>
                )}
              </div>
            ) : (
              <>
                <p className="text-sm text-muted-foreground">
                  Download your key bundle and use it to sign API requests.
                </p>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      if (!info.bundle) return;
                      const blob = new Blob([JSON.stringify(info.bundle, null, 2)], { type: "application/json" });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement("a");
                      a.href = url;
                      a.download = `${info.agentName ?? "agent"}-bundle.json`;
                      a.click();
                      URL.revokeObjectURL(url);
                    }}
                  >
                    <Download className="h-4 w-4" />
                    Download key bundle
                  </Button>
                </div>
                <CodeBlock
                  id="auth-login"
                  code={`# Authenticate with your key bundle (token stored in OS keychain)
ledgerctl auth login ${profileFlags} --bundle ${info.agentName ?? "agent"}-bundle.json`}
                  copiedField={copiedField}
                  onCopy={onCopy}
                />
              </>
            )}
          </CardContent>
        </Card>
      )}

      {/* Step 3: Endpoints */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {authEnabled ? "3" : "2"}. Connect to your cluster
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">gRPC endpoint</dt>
              <dd className="font-mono text-xs flex items-center gap-1">
                {grpcEndpoint}
                {isExternal && <Badge variant="secondary" className="ml-1 text-[10px]">ingress</Badge>}
                <CopyButton field="grpc-ep" text={grpcEndpoint} copiedField={copiedField} onCopy={onCopy} />
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground">HTTP endpoint</dt>
              <dd className="font-mono text-xs flex items-center gap-1">
                {httpEndpoint}
                {isExternal && <Badge variant="secondary" className="ml-1 text-[10px]">ingress</Badge>}
                <CopyButton field="http-ep" text={httpEndpoint} copiedField={copiedField} onCopy={onCopy} />
              </dd>
            </div>
          </div>
          {!isExternal && (
            <p className="text-xs text-muted-foreground">
              These are in-cluster addresses. Configure an Ingress on the LedgerService
              for external access.
            </p>
          )}

          <p className="text-sm text-muted-foreground mt-3">
            Use <code>ledgerctl</code> to interact with the cluster:
          </p>
          <CodeBlock
            id="ledgerctl-usage"
            code={`# Create a connection profile
ledgerctl profile create my-cluster ${profileFlags} --use

# Create a ledger
ledgerctl ledgers create my-ledger

# List ledgers
ledgerctl ledgers list`}
            copiedField={copiedField}
            onCopy={onCopy}
          />

          <p className="text-sm text-muted-foreground mt-3">
            Or use <code>curl</code> with the HTTP API:
          </p>
          <CodeBlock
            id="curl-usage"
            code={`# Health check
curl ${httpEndpoint}/_info

# Create a ledger
curl -X POST ${httpEndpoint}/v2/my-ledger -H 'Content-Type: application/json'

# Create a transaction
curl -X POST ${httpEndpoint}/v2/my-ledger/transactions \\
  -H 'Content-Type: application/json' \\
  -d '{
    "postings": [{
      "source": "world",
      "destination": "users:alice",
      "asset": "USD/2",
      "amount": 1000
    }]
  }'`}
            copiedField={copiedField}
            onCopy={onCopy}
          />
        </CardContent>
      </Card>
    </div>
  );
}
