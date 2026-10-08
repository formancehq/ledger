package indexes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestFormatMetadataValue_Datetime(t *testing.T) {
	t.Parallel()

	// 2024-01-15T10:00:00Z = 1705312800000000 micros. Datetime index keys share
	// the int64 encoding, so the server returns an int_value; the declared type
	// drives RFC3339 rendering.
	intVal := ledgerpb.NewIntValue(1705312800000000)

	assert.Equal(t,
		"2024-01-15T10:00:00Z",
		formatMetadataValue(intVal, ledgerpb.MetadataType_METADATA_TYPE_DATETIME),
		"an int_value declared datetime must render as RFC3339")

	assert.Equal(t,
		"1705312800000000",
		formatMetadataValue(intVal, ledgerpb.MetadataType_METADATA_TYPE_INT64),
		"an int_value declared int64 must render as the raw integer")

	// A self-describing datetime_value renders as RFC3339 regardless of the hint.
	assert.Equal(t,
		"2024-01-15T10:00:00Z",
		formatMetadataValue(ledgerpb.NewDatetimeValue(1705312800000000), ledgerpb.MetadataType_METADATA_TYPE_STRING),
		"a datetime_value always renders as RFC3339")
}
