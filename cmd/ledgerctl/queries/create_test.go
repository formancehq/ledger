package queries

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestParseCreateFilterAcceptsBothForms guards that `queries create --filter`
// takes the same dual-format input as `queries update` and the list commands:
// the textual grammar and the structured JSON DSL decode to the same filter.
func TestParseCreateFilterAcceptsBothForms(t *testing.T) {
	t.Parallel()

	target := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS

	fromText, err := parseCreateFilter(`reference == $ref`, true, target)
	require.NoError(t, err)
	require.NotNil(t, fromText)

	fromJSON, err := parseCreateFilter(`{"$match":{"reference":{"$param":"ref"}}}`, true, target)
	require.NoError(t, err)
	require.True(t, proto.Equal(fromText, fromJSON))

	omitted, err := parseCreateFilter("", false, target)
	require.NoError(t, err)
	require.Nil(t, omitted, "omitting --filter creates an unfiltered query")

	for _, blank := range []string{"", "   ", "\t\n"} {
		_, err = parseCreateFilter(blank, true, target)
		require.ErrorContains(t, err, "--filter must contain at least one condition", "blank %q", blank)
	}

	_, err = parseCreateFilter(`{"$match":{"bogus":"x"}}`, true, target)
	require.ErrorContains(t, err, "invalid filter expression")
}
