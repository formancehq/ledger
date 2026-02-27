import { useState, useEffect } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { PodTerminal } from "@/components/pod-terminal";

interface PodTerminalDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  namespace: string;
  podName: string;
  containers: string[];
}

export function PodTerminalDialog({
  open,
  onOpenChange,
  namespace,
  podName,
  containers,
}: PodTerminalDialogProps) {
  const [container, setContainer] = useState(containers[0] ?? "");

  useEffect(() => {
    setContainer(containers[0] ?? "");
  }, [containers]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-none w-[95vw] h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Terminal: {podName}</DialogTitle>
          <DialogDescription>Interactive shell session</DialogDescription>
        </DialogHeader>

        <div className="flex items-center gap-2 mb-2">
          <Label htmlFor="term-container">Container</Label>
          <Select value={container} onValueChange={setContainer}>
            <SelectTrigger id="term-container" className="w-48">
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

        <div className="flex-1 min-h-0 rounded-md overflow-hidden bg-[#1e1e1e]">
          {open && container && (
            <PodTerminal
              namespace={namespace}
              pod={podName}
              container={container}
            />
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
