package protosql

import (
	"testing"

	"github.com/invopop/jsonschema"
	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestVolumesAdapter(t *testing.T) {
	t.Parallel()

	volume := &commonpb.Volumes{Input: "12", Output: "3"}
	value, err := (VolumesAdapter{Volumes: volume}).Value()
	require.NoError(t, err)
	require.Equal(t, "(12, 3)", value)
	value, err = (VolumesAdapter{}).Value()
	require.NoError(t, err)
	require.Nil(t, value)

	destination := &commonpb.Volumes{}
	adapter := VolumesAdapter{Volumes: destination}
	require.NoError(t, adapter.Scan("( 12, 3 )"))
	require.Equal(t, volume, destination)
	require.NoError(t, adapter.Scan(nil))
	require.Equal(t, volume, destination)
	require.ErrorContains(t, adapter.Scan([]byte("(12,3)")), "expected string")
	require.ErrorContains(t, adapter.Scan("12,3"), "invalid volume pair")
}

func TestLogTypeAdapter(t *testing.T) {
	t.Parallel()

	logType := commonpb.OrderSkippedLogType
	adapter := LogTypeAdapter{LogType: &logType}
	value, err := adapter.Value()
	require.NoError(t, err)
	require.Equal(t, "ORDER_SKIPPED", value)
	require.NoError(t, adapter.Scan("NEW_TRANSACTION"))
	require.Equal(t, commonpb.NewTransactionLogType, logType)
	require.ErrorContains(t, adapter.Scan(1), "expected string")
	require.Error(t, adapter.Scan("UNKNOWN"))
}

func TestExtendVolumesJSONSchema(t *testing.T) {
	t.Parallel()

	schema := &jsonschema.Schema{Properties: jsonschema.NewProperties()}
	input := &jsonschema.Schema{Type: "string"}
	schema.Properties.Set("input", input)
	ExtendVolumesJSONSchema(schema)
	balance, ok := schema.Properties.Get("balance")
	require.True(t, ok)
	require.Same(t, input, balance)
}
