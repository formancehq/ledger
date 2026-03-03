/**
 * Server-side session store.
 *
 * Sessions live in memory on the backend — the browser never sees tokens.
 * Instead, the browser receives an HttpOnly cookie containing a random session
 * ID that is HMAC-signed (SHA-256) with AUTH_SESSION_SECRET so it can't be
 * forged or tampered with.
 *
 * Cookie format:  operator_sid=<random-hex-id>.<hmac-signature>
 *
 * How it works:
 *  1. After successful OIDC login, createSession() stores user info in a Map
 *     and returns a signed cookie value.
 *  2. On every request, getSession() reads the cookie, verifies the signature,
 *     and looks up the session data.
 *  3. On logout, deleteSessionFromCookie() removes the session from the Map.
 *
 * Note: sessions are lost on server restart (in-memory). This is acceptable
 * for the operator UI — users simply re-authenticate.
 */

import { randomBytes, createHmac, timingSafeEqual } from "node:crypto";

import type { UserRole } from "./roles.js";

export interface SessionData {
  userId: string;
  email?: string;
  name?: string;
  role: UserRole;
  createdAt: number;
}

/** In-memory store: session ID → user data. */
const sessions = new Map<string, SessionData>();

/** HMAC secret used to sign cookie values — set by initSessions(). */
let hmacSecret: string;

const COOKIE_NAME = "operator_sid";

/** Must be called once at startup before any session operations. */
export function initSessions(secret: string): void {
  hmacSecret = secret;
}

/** Sign a session ID: returns "id.hmac_signature". */
function sign(id: string): string {
  const mac = createHmac("sha256", hmacSecret).update(id).digest("base64url");
  return `${id}.${mac}`;
}

/** Verify a signed cookie value. Returns the session ID if valid, null otherwise. */
function verify(signed: string): string | null {
  const dot = signed.lastIndexOf(".");
  if (dot === -1) return null;
  const id = signed.substring(0, dot);
  const expected = createHmac("sha256", hmacSecret).update(id).digest("base64url");
  const actual = signed.substring(dot + 1);
  // Constant-time comparison to prevent timing attacks
  if (expected.length !== actual.length) return null;
  const ok = timingSafeEqual(Buffer.from(expected), Buffer.from(actual));
  return ok ? id : null;
}

/** Create a new session and return the cookie name + signed value to set. */
export function createSession(data: SessionData): { cookieName: string; cookieValue: string } {
  const id = randomBytes(32).toString("hex");
  sessions.set(id, data);
  return {
    cookieName: COOKIE_NAME,
    cookieValue: sign(id),
  };
}

/** Look up a session from the Cookie header. Returns null if missing/invalid. */
export function getSession(cookieHeader: string | undefined): SessionData | null {
  if (!cookieHeader) return null;
  const match = cookieHeader.split(";").map((c) => c.trim()).find((c) => c.startsWith(`${COOKIE_NAME}=`));
  if (!match) return null;
  const signed = match.substring(COOKIE_NAME.length + 1);
  const id = verify(signed);
  if (!id) return null;
  return sessions.get(id) ?? null;
}

/** Delete the session identified by the Cookie header (used on logout). */
export function deleteSessionFromCookie(cookieHeader: string | undefined): void {
  if (!cookieHeader) return;
  const match = cookieHeader.split(";").map((c) => c.trim()).find((c) => c.startsWith(`${COOKIE_NAME}=`));
  if (!match) return;
  const signed = match.substring(COOKIE_NAME.length + 1);
  const id = verify(signed);
  if (id) sessions.delete(id);
}

export function getCookieName(): string {
  return COOKIE_NAME;
}
