package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimestampJSONRoundsBeforeEpochCheck(t *testing.T) {
	t.Parallel()

	var atEpoch Timestamp
	require.NoError(t, atEpoch.UnmarshalJSON([]byte(`"1969-12-31T23:59:59.9999996Z"`)))
	require.Equal(t, uint64(0), atEpoch.GetData())

	var beforeEpoch Timestamp
	require.ErrorIs(t, beforeEpoch.UnmarshalJSON([]byte(`"1969-12-31T23:59:59.9999994Z"`)), ErrTimestampBeforeEpoch)
}
