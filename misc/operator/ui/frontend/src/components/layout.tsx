import { Link, Outlet, useNavigate, useParams } from "react-router-dom";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { useNamespaces } from "@/api/hooks";
import { useWatch } from "@/api/use-watch";
import { Database, Settings, Box } from "lucide-react";

const LAST_NS_KEY = "ledger-ui-namespace";

export function Layout() {
  const { ns } = useParams<{ ns: string }>();
  const navigate = useNavigate();
  const { data: namespaces } = useNamespaces();

  const currentNs = ns || localStorage.getItem(LAST_NS_KEY) || "";

  // Open SSE connection to watch K8s resources in the selected namespace
  useWatch(currentNs || undefined);

  const handleNamespaceChange = (value: string) => {
    localStorage.setItem(LAST_NS_KEY, value);
    navigate(`/namespaces/${value}/ledger-services`);
  };

  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <aside className="w-64 border-r bg-sidebar-background flex flex-col">
        <div className="p-4">
          <Link to="/" className="flex items-center gap-2 font-semibold text-lg">
            <Database className="h-5 w-5" />
            Ledger Operator
          </Link>
        </div>
        <Separator />

        {/* Namespace selector */}
        <div className="p-4">
          <label className="text-xs font-medium text-muted-foreground mb-2 block">
            Namespace
          </label>
          <Select value={currentNs} onValueChange={handleNamespaceChange}>
            <SelectTrigger>
              <SelectValue placeholder="Select namespace..." />
            </SelectTrigger>
            <SelectContent>
              {(namespaces ?? []).map((ns) => (
                <SelectItem key={ns.name} value={ns.name}>
                  {ns.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <Separator />

        {/* Navigation links */}
        <nav className="flex-1 p-2">
          {currentNs && (
            <Link
              to={`/namespaces/${currentNs}/ledger-services`}
              className="flex items-center gap-2 rounded-md px-3 py-2 text-sm hover:bg-sidebar-accent"
            >
              <Box className="h-4 w-4" />
              Services
            </Link>
          )}
          <Link
            to="/ledger-defaults"
            className="flex items-center gap-2 rounded-md px-3 py-2 text-sm hover:bg-sidebar-accent"
          >
            <Settings className="h-4 w-4" />
            Configurations
          </Link>
        </nav>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto">
        <div className="p-6">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
