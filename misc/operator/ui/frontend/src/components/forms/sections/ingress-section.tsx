import type { IngressSpec, IngressGrpcSpec, IngressHost } from "shared";
import { FormSection } from "@/components/forms/form-section";
import { FormField } from "@/components/forms/form-field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { Plus, X } from "lucide-react";

interface IngressSectionProps {
  ingress?: IngressSpec;
  ingressGrpc?: IngressGrpcSpec;
  onIngressChange: (value: IngressSpec) => void;
  onIngressGrpcChange: (value: IngressGrpcSpec) => void;
}

function HostList({
  hosts,
  onChange,
  idPrefix,
}: {
  hosts: IngressHost[];
  onChange: (hosts: IngressHost[]) => void;
  idPrefix: string;
}) {
  return (
    <div className="col-span-full space-y-2">
      {hosts.map((h, i) => (
        <div key={i} className="flex items-center gap-2">
          <Input
            id={`${idPrefix}-host-${i}`}
            value={h.host}
            onChange={(e) => {
              const next = [...hosts];
              next[i] = { ...h, host: e.target.value };
              onChange(next);
            }}
            placeholder="example.com"
            className="flex-1"
          />
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={() => onChange(hosts.filter((_, j) => j !== i))}
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      ))}
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={() => onChange([...hosts, { host: "" }])}
      >
        <Plus className="h-3 w-3 mr-1" />
        Add Host
      </Button>
    </div>
  );
}

export function IngressSection({
  ingress = {},
  ingressGrpc = {},
  onIngressChange,
  onIngressGrpcChange,
}: IngressSectionProps) {
  return (
    <FormSection value="ingress" title="Ingress" description="HTTP and gRPC ingress configuration">
      {/* HTTP Ingress */}
      <FormField label="HTTP Ingress Enabled" htmlFor="ing-http-enabled">
        <Switch
          id="ing-http-enabled"
          checked={ingress.enabled ?? false}
          onCheckedChange={(checked) =>
            onIngressChange({ ...ingress, enabled: checked || undefined })
          }
        />
      </FormField>
      <FormField label="HTTP Ingress Class" htmlFor="ing-http-class">
        <Input
          id="ing-http-class"
          value={ingress.className ?? ""}
          onChange={(e) =>
            onIngressChange({ ...ingress, className: e.target.value || undefined })
          }
          placeholder="nginx"
        />
      </FormField>
      <div className="col-span-full">
        <p className="text-sm font-medium mb-2">HTTP Hosts</p>
        <HostList
          hosts={ingress.hosts ?? []}
          onChange={(hosts) => onIngressChange({ ...ingress, hosts })}
          idPrefix="ing-http"
        />
      </div>

      {/* gRPC Ingress */}
      <FormField label="gRPC Ingress Enabled" htmlFor="ing-grpc-enabled">
        <Switch
          id="ing-grpc-enabled"
          checked={ingressGrpc.enabled ?? false}
          onCheckedChange={(checked) =>
            onIngressGrpcChange({ ...ingressGrpc, enabled: checked || undefined })
          }
        />
      </FormField>
      <FormField label="gRPC Ingress Class" htmlFor="ing-grpc-class">
        <Input
          id="ing-grpc-class"
          value={ingressGrpc.className ?? ""}
          onChange={(e) =>
            onIngressGrpcChange({ ...ingressGrpc, className: e.target.value || undefined })
          }
          placeholder="alb"
        />
      </FormField>
      <div className="col-span-full">
        <p className="text-sm font-medium mb-2">gRPC Hosts</p>
        <HostList
          hosts={ingressGrpc.hosts ?? []}
          onChange={(hosts) => onIngressGrpcChange({ ...ingressGrpc, hosts })}
          idPrefix="ing-grpc"
        />
      </div>
    </FormSection>
  );
}
