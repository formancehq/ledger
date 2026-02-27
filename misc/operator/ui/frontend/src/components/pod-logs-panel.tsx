import { useState, useRef, useEffect, useCallback } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { usePodLogs } from "@/api/use-pod-logs";
import { Play, Square, Trash2 } from "lucide-react";

interface PodLogsPanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  namespace: string;
  podName: string;
  containers: string[];
}

export function PodLogsPanel({
  open,
  onOpenChange,
  namespace,
  podName,
  containers,
}: PodLogsPanelProps) {
  const [container, setContainer] = useState(containers[0] ?? "");
  const [follow, setFollow] = useState(true);
  const [tailLines, setTailLines] = useState("1000");
  const preRef = useRef<HTMLPreElement>(null);

  const { lines, isStreaming, start, stop, clear } = usePodLogs({
    namespace,
    pod: podName,
    container,
    follow,
    tailLines: parseInt(tailLines, 10) || 1000,
  });

  // Auto-scroll to bottom when new lines arrive and follow is on
  useEffect(() => {
    if (follow && preRef.current) {
      preRef.current.scrollTop = preRef.current.scrollHeight;
    }
  }, [lines, follow]);

  // Reset container when pod changes
  useEffect(() => {
    setContainer(containers[0] ?? "");
  }, [containers]);

  // Stop streaming on close
  const handleOpenChange = useCallback(
    (nextOpen: boolean) => {
      if (!nextOpen) {
        stop();
        clear();
      }
      onOpenChange(nextOpen);
    },
    [onOpenChange, stop, clear]
  );

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-4xl h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Logs: {podName}</DialogTitle>
          <DialogDescription>Streaming pod logs</DialogDescription>
        </DialogHeader>

        {/* Controls */}
        <div className="flex items-center gap-4 flex-wrap">
          <div className="flex items-center gap-2">
            <Label htmlFor="log-container">Container</Label>
            <Select value={container} onValueChange={setContainer}>
              <SelectTrigger id="log-container" className="w-48">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {containers.map((c) => (
                  <SelectItem key={c} value={c}>
                    {c}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center gap-2">
            <Label htmlFor="log-follow">Follow</Label>
            <Switch
              id="log-follow"
              checked={follow}
              onCheckedChange={setFollow}
            />
          </div>

          <div className="flex items-center gap-2">
            <Label htmlFor="log-tail">Tail</Label>
            <Input
              id="log-tail"
              type="number"
              className="w-24"
              value={tailLines}
              onChange={(e) => setTailLines(e.target.value)}
            />
          </div>
        </div>

        {/* Log output */}
        <pre
          ref={preRef}
          className="flex-1 overflow-auto bg-muted p-4 rounded-md text-xs font-mono whitespace-pre-wrap min-h-0"
        >
          {lines.length > 0 ? lines.join("\n") : (
            <span className="text-muted-foreground">
              {isStreaming ? "Waiting for logs..." : "Click Start to begin streaming logs."}
            </span>
          )}
        </pre>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="outline" size="sm" onClick={clear}>
            <Trash2 className="h-4 w-4 mr-1" />
            Clear
          </Button>
          {isStreaming ? (
            <Button variant="outline" size="sm" onClick={stop}>
              <Square className="h-4 w-4 mr-1" />
              Stop
            </Button>
          ) : (
            <Button size="sm" onClick={start}>
              <Play className="h-4 w-4 mr-1" />
              Start
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={() => handleOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
