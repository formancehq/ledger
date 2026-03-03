/**
 * Role-based access control — maps user emails to roles.
 *
 * Two roles exist:
 *  - "admin"  — full access to all features
 *  - "guest"  — can manage ledger lifecycle but cannot edit configurations
 *
 * Role resolution:
 *  1. Exact email match in the mapping
 *  2. Wildcard "*" match
 *  3. Default to "guest" (secure by default)
 */

export type UserRole = "admin" | "guest";

/**
 * Resolve a user's role from their email and the configured mapping.
 *
 * @param email    The user's email from OIDC claims (may be undefined)
 * @param mapping  Email-to-role mapping from AUTH_ROLE_MAPPING
 * @returns        The resolved role ("admin" or "guest")
 */
export function resolveRole(
  email: string | undefined,
  mapping: Record<string, string>
): UserRole {
  if (email) {
    const exact = mapping[email];
    if (exact === "admin" || exact === "guest") return exact;
  }

  const wildcard = mapping["*"];
  if (wildcard === "admin" || wildcard === "guest") return wildcard;

  return "guest";
}
