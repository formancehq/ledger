package http

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// TestOpenAPISpec_DecodesStrictly fails when openapi.yml repeats a mapping key.
// A duplicate is not a style issue: strict parsers reject the whole document,
// and lenient ones silently keep only the last definition, so the published
// contract depends on which tool reads it. POST /v3/{ledgerName}/promote once
// carried two '400' responses.
func TestOpenAPISpec_DecodesStrictly(t *testing.T) {
	t.Parallel()

	spec, err := os.ReadFile("../../../openapi.yml")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(spec, &doc))
	require.Contains(t, doc, "paths")
}
