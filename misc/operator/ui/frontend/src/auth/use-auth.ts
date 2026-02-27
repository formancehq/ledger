import { useQuery, useQueryClient } from "@tanstack/react-query";

export interface AuthUser {
  id: string;
  email?: string;
  name?: string;
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

export function useAuth() {
  return useQuery({
    queryKey: ["auth", "me"],
    queryFn: fetchAuthMe,
    staleTime: 60_000,
    retry: false,
  });
}

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
