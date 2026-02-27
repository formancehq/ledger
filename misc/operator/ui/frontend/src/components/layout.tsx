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
import { useAuth, useLogout } from "@/auth/use-auth";
import { Database, Settings, Box, LogOut, User } from "lucide-react";

const LAST_NS_KEY = "ledger-ui-namespace";

/**
 * UserInfo — shown at the bottom of the sidebar when auth is enabled.
 * Displays the logged-in user's name/email and a logout button.
 * Renders nothing when auth is disabled (the component is invisible).
 */
function UserInfo() {
  const { data: auth } = useAuth();
  const logout = useLogout();

  // Don't render anything when auth is off or user is not logged in
  if (!auth?.enabled || !auth.authenticated || !auth.user) return null;

  return (
    <>
      <Separator />
      <div className="p-3 flex items-center gap-2">
        <User className="h-4 w-4 shrink-0 text-muted-foreground" />
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium truncate">
            {auth.user.name ?? auth.user.email ?? auth.user.id}
          </p>
          {auth.user.name && auth.user.email && (
            <p className="text-xs text-muted-foreground truncate">{auth.user.email}</p>
          )}
        </div>
        <button
          onClick={logout}
          className="shrink-0 rounded-md p-1.5 text-muted-foreground hover:bg-sidebar-accent hover:text-foreground"
          title="Log out"
        >
          <LogOut className="h-4 w-4" />
        </button>
      </div>
    </>
  );
}

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
          <label className="text-xs font-medium text-muted-foreground mb-1 block">
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

        <UserInfo />
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
