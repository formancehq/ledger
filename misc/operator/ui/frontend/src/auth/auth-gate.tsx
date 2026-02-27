import type { ReactNode } from "react";
import { useAuth } from "./use-auth";

export function AuthGate({ children }: { children: ReactNode }) {
  const { data, isLoading, isError } = useAuth();

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
      </div>
    );
  }

  // Auth disabled — render app normally
  if (data && !data.enabled) {
    return <>{children}</>;
  }

  // Auth enabled but not authenticated — redirect to login
  if (!data?.authenticated || isError) {
    window.location.href = "/api/auth/login";
    return (
      <div className="flex h-screen items-center justify-center">
        <p className="text-muted-foreground">Redirecting to login...</p>
      </div>
    );
  }

  // Authenticated
  return <>{children}</>;
}
