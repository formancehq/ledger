import type { Context } from "hono";

interface K8sError extends Error {
  statusCode?: number;
  body?: unknown;
}

function isK8sError(err: unknown): err is K8sError {
  return err instanceof Error && "statusCode" in err;
}

export async function errorHandler(err: Error, c: Context) {
  console.error(`[${c.req.method} ${c.req.path}]`, err.message);

  if (isK8sError(err)) {
    const status = err.statusCode ?? 500;
    const body =
      typeof err.body === "object" && err.body !== null
        ? err.body
        : { message: err.message };
    return c.json({ error: body }, status as any);
  }

  return c.json({ error: { message: err.message } }, 500);
}
