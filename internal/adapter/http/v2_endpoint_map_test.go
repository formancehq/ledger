package http

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// v2EndpointMapPath is the published Ledger v2 → v3 endpoint map. The public
// docs render their migration cheat sheet from a copy of it, and customers
// load it as data, so a v3 target it names must be a route this server
// actually serves.
const v2EndpointMapPath = "../../../docs/technical/contributing/v2-to-v3-endpoint-map.json"

type v2EndpointMap struct {
	Statuses   map[string]string `json:"statuses"`
	Operations map[string]struct {
		OperationID string   `json:"operationId"`
		Status      string   `json:"status"`
		V3          []string `json:"v3"`
		Notes       string   `json:"notes"`
	} `json:"operations"`
}

// TestV2EndpointMap_TargetsExist fails when a route the map points Ledger v2
// callers at is renamed or removed from the router or from openapi.yml. Update
// the map in the same change.
func TestV2EndpointMap_TargetsExist(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(v2EndpointMapPath)
	require.NoError(t, err)

	var endpointMap v2EndpointMap
	require.NoError(t, json.Unmarshal(raw, &endpointMap))
	require.NotEmpty(t, endpointMap.Operations)

	spec, err := os.ReadFile("../../../openapi.yml")
	require.NoError(t, err)

	documentedOps := openAPIOperations(t, spec)
	routes := walkRoutes(t, newScopedHandler(t, nil))

	for key, op := range endpointMap.Operations {
		require.Containsf(t, endpointMap.Statuses, op.Status, "%s: unknown status %q", key, op.Status)
		require.NotEmptyf(t, op.OperationID, "%s: missing operationId", key)

		if op.Status == "removed" {
			require.Emptyf(t, op.V3, "%s: a removed operation has no v3 target", key)
			require.NotEmptyf(t, op.Notes, "%s: a removed operation must say what to do instead", key)

			continue
		}

		require.NotEmptyf(t, op.V3, "%s: status %q needs at least one v3 target", key, op.Status)

		for _, target := range op.V3 {
			method, path, ok := strings.Cut(target, " ")
			require.Truef(t, ok, "%s: target %q is not \"METHOD /path\"", key, target)

			_, served := routes[walkKey{method, path}]
			require.Truef(t, served, "%s: target %q is not registered by NewHandler", key, target)

			_, documented := documentedOps[walkKey{method, path}]
			require.Truef(t, documented, "%s: target %q is missing from openapi.yml", key, target)
		}
	}
}

// openAPIOperations lists the (method, path) pairs openapi.yml declares. It
// walks the node tree rather than decoding into a map, because decoding
// rejects the whole document over a duplicate key anywhere in it, and only the
// method and path keys matter here.
func openAPIOperations(t *testing.T, spec []byte) map[walkKey]struct{} {
	t.Helper()

	var doc yaml.Node
	require.NoError(t, yaml.Unmarshal(spec, &doc))
	require.NotEmpty(t, doc.Content)

	ops := map[walkKey]struct{}{}

	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "paths" {
			continue
		}

		paths := root.Content[i+1]
		for j := 0; j+1 < len(paths.Content); j += 2 {
			path, item := paths.Content[j].Value, paths.Content[j+1]
			for k := 0; k+1 < len(item.Content); k += 2 {
				ops[walkKey{strings.ToUpper(item.Content[k].Value), path}] = struct{}{}
			}
		}
	}

	require.NotEmpty(t, ops, "openapi.yml declares no paths")

	return ops
}
