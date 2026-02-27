import { randomBytes, createHmac, timingSafeEqual } from "node:crypto";

export interface SessionData {
  userId: string;
  email?: string;
  name?: string;
  createdAt: number;
}

const sessions = new Map<string, SessionData>();
let hmacSecret: string;

const COOKIE_NAME = "operator_sid";

export function initSessions(secret: string): void {
  hmacSecret = secret;
}

function sign(id: string): string {
  const mac = createHmac("sha256", hmacSecret).update(id).digest("base64url");
  return `${id}.${mac}`;
}

function verify(signed: string): string | null {
  const dot = signed.lastIndexOf(".");
  if (dot === -1) return null;
  const id = signed.substring(0, dot);
  const expected = createHmac("sha256", hmacSecret).update(id).digest("base64url");
  const actual = signed.substring(dot + 1);
  if (expected.length !== actual.length) return null;
  const ok = timingSafeEqual(Buffer.from(expected), Buffer.from(actual));
  return ok ? id : null;
}

export function createSession(data: SessionData): { cookieName: string; cookieValue: string } {
  const id = randomBytes(32).toString("hex");
  sessions.set(id, data);
  return {
    cookieName: COOKIE_NAME,
    cookieValue: sign(id),
  };
}

export function getSession(cookieHeader: string | undefined): SessionData | null {
  if (!cookieHeader) return null;
  const match = cookieHeader.split(";").map((c) => c.trim()).find((c) => c.startsWith(`${COOKIE_NAME}=`));
  if (!match) return null;
  const signed = match.substring(COOKIE_NAME.length + 1);
  const id = verify(signed);
  if (!id) return null;
  return sessions.get(id) ?? null;
}

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
