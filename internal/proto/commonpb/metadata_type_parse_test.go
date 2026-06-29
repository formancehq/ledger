package commonpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMetadataType_Datetime(t *testing.T) {
	t.Parallel()

	got, err := ParseMetadataType("datetime")
	require.NoError(t, err)
	assert.Equal(t, MetadataType_METADATA_TYPE_DATETIME, got)

	assert.Equal(t, "datetime", MetadataTypeToString(MetadataType_METADATA_TYPE_DATETIME))
	assert.Contains(t, MetadataTypeOptions(), "datetime")
}
