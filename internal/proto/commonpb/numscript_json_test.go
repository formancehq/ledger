package commonpb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNumscriptInfo_MarshalJSON_CamelCase guards the regression where the
// numscript GET / PUT REST responses emitted `created_at` (the protoc-gen-go
// default tag) instead of `createdAt`, diverging from every other endpoint's
// camelCase contract. Same class of bug as #459.
func TestNumscriptInfo_MarshalJSON_CamelCase(t *testing.T) {
	t.Parallel()

	info := &NumscriptInfo{
		Name:      "my-script",
		Content:   "send [USD/2 1] (source=@world destination=@a)",
		Version:   "1.0.0",
		CreatedAt: &Timestamp{Data: 1_700_000_000_000_000},
		Ledger:    "ledger1",
	}

	data, err := info.MarshalJSON()
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"name":"my-script"`)
	require.Contains(t, out, `"content":"send [USD/2 1] (source=@world destination=@a)"`)
	require.Contains(t, out, `"version":"1.0.0"`)
	require.Contains(t, out, `"createdAt":`)
	require.Contains(t, out, `"ledger":"ledger1"`)
	require.False(t, strings.Contains(out, "created_at"),
		"NumscriptInfo must use camelCase createdAt to match every other endpoint")
}
