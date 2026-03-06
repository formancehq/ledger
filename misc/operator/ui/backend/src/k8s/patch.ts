import { setHeaderOptions } from "@kubernetes/client-node";

/**
 * Configuration options that set Content-Type to application/merge-patch+json
 * for Kubernetes patch operations via CustomObjectsApi.
 */
export const mergePatchOptions = setHeaderOptions(
  "Content-Type",
  "application/merge-patch+json",
);
