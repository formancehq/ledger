package auditpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuditEntryUnmarshalRejectsMalformedPackedCapabilities(t *testing.T) {
	t.Parallel()
	// The declared packed payload is one byte; the following valid order_count
	// field must not be consumed as the rest of a fixed32 capability.
	malformed := []byte{0x72, 0x01, 0x01, 0x30, 0x80, 0x01}
	require.Error(t, new(AuditEntry).UnmarshalVT(malformed))

	valid := []byte{0x72, 0x04, 0x07, 0x00, 0x00, 0x00, 0x30, 0x01}
	entry := new(AuditEntry)
	require.NoError(t, entry.UnmarshalVT(valid))
	require.Equal(t, []uint32{7}, entry.GetRequiredCapabilities())
	require.Equal(t, uint32(1), entry.GetOrderCount())
}
