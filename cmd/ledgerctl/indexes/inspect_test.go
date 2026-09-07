package indexes

import (
	"io"
	"os"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestFormatMetadataValue_Datetime(t *testing.T) {
	t.Parallel()

	// 2024-01-15T10:00:00Z = 1705312800000000 micros. Datetime index keys share
	// the int64 encoding, so the server returns an int_value; the served
	// binding's type drives RFC3339 rendering.
	intVal := commonpb.NewIntValue(1705312800000000)

	assert.Equal(t,
		"2024-01-15T10:00:00Z",
		formatMetadataValue(intVal, commonpb.MetadataType_METADATA_TYPE_DATETIME),
		"an int_value under a datetime-serving binding must render as RFC3339")

	assert.Equal(t,
		"1705312800000000",
		formatMetadataValue(intVal, commonpb.MetadataType_METADATA_TYPE_INT64),
		"an int_value under an int64-serving binding must render as the raw integer")

	// A self-describing datetime_value renders as RFC3339 regardless of the hint.
	assert.Equal(t,
		"2024-01-15T10:00:00Z",
		formatMetadataValue(commonpb.NewDatetimeValue(1705312800000000), commonpb.MetadataType_METADATA_TYPE_STRING),
		"a datetime_value always renders as RFC3339")
}

// TestRenderInspectResult_UsesServedBinding drives the production render step
// with a discriminating served_type: the type that renders the values comes
// from the response's served binding, not from any schema lookup. Feeding the
// render a constant or the live declared type flips these assertions.
func TestRenderInspectResult_UsesServedBinding(t *testing.T) {
	render := func(t *testing.T, servedType commonpb.MetadataType) string {
		t.Helper()

		resp := &servicepb.InspectIndexResponse{
			ServedType: servedType,
			Result: &servicepb.InspectIndexResponse_DistinctValues{
				DistinctValues: &servicepb.InspectDistinctValues{
					Values: []*commonpb.MetadataValue{commonpb.NewIntValue(1705312800000000)},
				},
			},
		}

		orig := os.Stdout
		r, w, err := os.Pipe()
		require.NoError(t, err)

		os.Stdout = w
		pterm.SetDefaultOutput(w)
		renderInspectResult(resp)
		pterm.SetDefaultOutput(orig)
		os.Stdout = orig
		require.NoError(t, w.Close())

		out, err := io.ReadAll(r)
		require.NoError(t, err)

		return string(out)
	}

	assert.Contains(t, render(t, commonpb.MetadataType_METADATA_TYPE_DATETIME), "2024-01-15T10:00:00Z",
		"a datetime-served binding renders RFC3339")
	assert.Contains(t, render(t, commonpb.MetadataType_METADATA_TYPE_INT64), "1705312800000000",
		"an int64-served binding renders the raw integer")
}
