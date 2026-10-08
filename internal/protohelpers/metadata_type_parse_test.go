package protohelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestParseMetadataType_Datetime(t *testing.T) {
	t.Parallel()

	got, err := ParseMetadataType("datetime")
	require.NoError(t, err)
	assert.Equal(t, ledgerpb.MetadataType_METADATA_TYPE_DATETIME, got)

	assert.Equal(t, "datetime", MetadataTypeToString(ledgerpb.MetadataType_METADATA_TYPE_DATETIME))
	assert.Contains(t, MetadataTypeOptions(), "datetime")
}
