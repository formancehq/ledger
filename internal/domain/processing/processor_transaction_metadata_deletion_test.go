package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestScriptAccountMetadataDeletions pins the logged deletion set: keys are
// sorted, and a key this transaction also sets is not deleted.
func TestScriptAccountMetadataDeletions(t *testing.T) {
	t.Parallel()

	deleted := map[string]map[string]struct{}{
		"users:001": {"c": {}, "a": {}, "b": {}},
		"users:002": {"kept-by-caller": {}},
	}
	accountMetadata := map[string]*commonpb.MetadataMap{
		"users:001": {Values: map[string]*commonpb.MetadataValue{"b": commonpb.NewStringValue("caller")}},
		"users:002": {Values: map[string]*commonpb.MetadataValue{"kept-by-caller": commonpb.NewStringValue("caller")}},
	}

	require.Equal(t, map[string]*commonpb.MetadataKeys{
		"users:001": {Keys: []string{"a", "c"}},
	}, scriptAccountMetadataDeletions(deleted, accountMetadata))

	require.Nil(t, scriptAccountMetadataDeletions(nil, accountMetadata))
}
