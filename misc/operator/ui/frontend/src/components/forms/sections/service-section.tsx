import type { ServiceSpec, HeadlessServiceSpec } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface ServiceSectionProps {
  service?: ServiceSpec;
  headlessService?: HeadlessServiceSpec;
  onServiceChange: (value: ServiceSpec) => void;
  onHeadlessChange: (value: HeadlessServiceSpec) => void;
}

function numVal(v: string): number | undefined {
  if (v === "") return undefined;
  const n = Number(v);
  return isNaN(n) ? undefined : n;
}

export function ServiceSection({
  service = {},
  headlessService = {},
  onServiceChange,
  onHeadlessChange,
}: ServiceSectionProps) {
  return (
    <FormSection value="service" title="Service" description="Kubernetes Service and headless service">
      <FormField label="Service Type" htmlFor="svc-type">
        <Select
          value={service.type ?? ""}
          onValueChange={(v) =>
            onServiceChange({ ...service, type: v || undefined })
          }
        >
          <SelectTrigger id="svc-type">
            <SelectValue placeholder="ClusterIP" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="ClusterIP">ClusterIP</SelectItem>
            <SelectItem value="NodePort">NodePort</SelectItem>
            <SelectItem value="LoadBalancer">LoadBalancer</SelectItem>
          </SelectContent>
        </Select>
      </FormField>
      <FormField label="HTTP Port" htmlFor="svc-http-port">
        <Input
          id="svc-http-port"
          type="number"
          value={service.httpPort ?? ""}
          onChange={(e) =>
            onServiceChange({ ...service, httpPort: numVal(e.target.value) })
          }
          placeholder="9000"
        />
      </FormField>
      <FormField label="gRPC Port" htmlFor="svc-grpc-port">
        <Input
          id="svc-grpc-port"
          type="number"
          value={service.grpcPort ?? ""}
          onChange={(e) =>
            onServiceChange({ ...service, grpcPort: numVal(e.target.value) })
          }
          placeholder="8888"
        />
      </FormField>
      <FormField label="Raft Port" htmlFor="svc-raft-port">
        <Input
          id="svc-raft-port"
          type="number"
          value={service.raftPort ?? ""}
          onChange={(e) =>
            onServiceChange({ ...service, raftPort: numVal(e.target.value) })
          }
          placeholder="9100"
        />
      </FormField>
      <FormField label="Headless Service Enabled" htmlFor="headless-enabled">
        <Switch
          id="headless-enabled"
          checked={headlessService.enabled ?? false}
          onCheckedChange={(checked) =>
            onHeadlessChange({ ...headlessService, enabled: checked || undefined })
          }
        />
      </FormField>
    </FormSection>
  );
}
