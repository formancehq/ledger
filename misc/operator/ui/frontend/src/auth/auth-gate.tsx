/**
 * AuthGate — wraps the entire app and controls access.
 *
 * This component is the first thing that renders. It calls GET /api/auth/me
 * to figure out what to do:
 *
 *  1. Loading       → show a spinner while we check auth status
 *  2. Auth disabled → render the app normally (no login required)
 *  3. Not logged in → redirect the browser to /api/auth/login (OIDC flow)
 *  4. Logged in     → render the app
 *
 * This means when auth is off (the default), users see zero difference.
 */

import type { ReactNode } from "react";
import { useAuth } from "./use-auth";

export function AuthGate({ children }: { children: ReactNode }) {
  const { data, isLoading, isError } = useAuth();

  // Step 1: still checking with the backend
  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
      </div>
    );
  }

  // Step 2: auth is disabled — pass through
  if (data && !data.enabled) {
    return <>{children}</>;
  }

  // Step 3: auth is enabled but user is not logged in — redirect to OIDC login
  if (!data?.authenticated || isError) {
    window.location.href = "/api/auth/login";
    return (
      <div className="flex h-screen items-center justify-center">
        <p className="text-muted-foreground">Redirecting to login...</p>
      </div>
    );
  }

  // Step 4: authenticated — show the app
  return <>{children}</>;
}
