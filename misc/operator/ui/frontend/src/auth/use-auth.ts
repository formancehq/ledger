/**
 * React hooks for authentication and authorization.
 *
 * useAuth()   — fetches the current auth status from the backend (GET /api/auth/me).
 *               Returns { enabled, authenticated, user } so components can adapt:
 *               - enabled=false  → auth is off, show the app normally
 *               - authenticated  → user is logged in, show their info
 *               - !authenticated → redirect to login
 *
 * useRole()   — returns the current user's role ("admin" or "guest").
 *               Defaults to "admin" when auth is disabled (no restrictions).
 *
 * useLogout() — returns an async function that logs the user out (POST /api/auth/logout),
 *               clears the React Query cache, and redirects to the post-logout page.
 */

import { useQuery, useQueryClient } from "@tanstack/react-query";

export type UserRole = "admin" | "guest";

export interface AuthUser {
  id: string;
  email?: string;
  name?: string;
  role?: UserRole;
}

export interface AuthStatus {
  enabled: boolean;
  authenticated: boolean;
  user?: AuthUser;
}

async function fetchAuthMe(): Promise<AuthStatus> {
  const res = await fetch("/api/auth/me");
  if (res.status === 401) {
    const body = await res.json();
    return { enabled: body.enabled ?? true, authenticated: false };
  }
  if (!res.ok) {
    throw new Error("Failed to fetch auth status");
  }
  return res.json();
}

/** Check current authentication status. Cached for 60 seconds. */
export function useAuth() {
  return useQuery({
    queryKey: ["auth", "me"],
    queryFn: fetchAuthMe,
    staleTime: 60_000,
    retry: false,
  });
}

/**
 * Returns the current user's role.
 * When auth is disabled, returns "admin" (no restrictions).
 * When auth is enabled but user data is unavailable, returns "guest" (secure default).
 */
export function useRole(): UserRole {
  const { data } = useAuth();
  if (!data?.enabled) return "admin";
  return data.user?.role ?? "guest";
}

/** Returns an async logout function. Clears cache and redirects. */
export function useLogout() {
  const qc = useQueryClient();
  return async () => {
    const res = await fetch("/api/auth/logout", { method: "POST" });
    if (res.ok) {
      const { redirectTo } = await res.json();
      qc.clear();
      window.location.href = redirectTo ?? "/";
    }
  };
}
