package controller

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// serviceAccountNamespacePath is the standard in-cluster location where kubelet
// mounts the ServiceAccount namespace file. Present in every pod that has an
// automounted ServiceAccount token (the default).
const serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// DiscoverOperatorNamespace resolves the namespace the operator is running in,
// used to place cluster-scoped resources whose canonical location must be
// stable across the lifecycle of any Cluster or Credentials (e.g.
// per-credentials canonical seed Secrets). Resolution order:
//
//  1. POD_NAMESPACE env var (downward API — preferred, easy to inject and mock).
//  2. In-cluster ServiceAccount namespace file (default in every pod).
//
// Returns an explicit error when neither is available so the operator refuses
// to start rather than silently persisting seeds in the wrong location.
func DiscoverOperatorNamespace() (string, error) {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns, nil
	}

	data, err := os.ReadFile(serviceAccountNamespacePath)
	if err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("reading %s: %w", serviceAccountNamespacePath, err)
	}

	return "", errors.New("could not determine operator namespace: set POD_NAMESPACE (downward API) or run inside a pod with a mounted ServiceAccount")
}
