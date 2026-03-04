import type { Context } from "hono";

interface K8sError extends Error {
  // @kubernetes/client-node v1.x ApiException uses `code`
  code?: number;
  body?: unknown;
}

function isK8sError(err: unknown): err is K8sError {
  return err instanceof Error && "code" in err && typeof (err as any).code === "number";
}

export async function errorHandler(err: Error, c: Context) {
  console.error(`[${c.req.method} ${c.req.path}]`, err.message);

  if (isK8sError(err)) {
    const status = err.code ?? 500;
    const body =
      typeof err.body === "object" && err.body !== null
        ? err.body
        : { message: err.message };
    return c.json({ error: body }, status as any);
  }

  return c.json({ error: { message: err.message } }, 500);
}
