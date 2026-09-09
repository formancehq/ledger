package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// API redaction must not erase the diagnostic identity hashed into audit
// failures. Pin literal messages as well as context to catch accidental edits
// to Error() (the general projection table intentionally derives its message).
func TestPublicErrorAuditIdentityUnchanged(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		err      domain.Describable
		message  string
		metadata map[string]string
	}{
		{
			name:     "coverage",
			err:      &ErrCoverageMiss{Attribute: "volumes", CanonicalHex: "deadbeef", IDHex: "0102", RaftIndex: 42},
			message:  "preload coverage miss (kind=volumes id=0102 raftIndex=42)",
			metadata: map[string]string{"attribute": "volumes", "canonicalHex": "deadbeef", "idHex": "0102", "raftIndex": "42"},
		},
		{
			name:     "index",
			err:      &domain.ErrIndexInconsistent{Index: "private-index", Detail: "reading /private/pebble: secret failure"},
			message:  "index private-index is inconsistent: reading /private/pebble: secret failure",
			metadata: map[string]string{"index": "private-index", "detail": "reading /private/pebble: secret failure"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, metadata, overridden := domain.PublicErrorDetails(tc.err)
			require.True(t, overridden)
			require.Empty(t, metadata)
			failure := buildAuditFailure(tc.err)
			require.Equal(t, tc.message, failure.GetMessage())
			require.Equal(t, tc.metadata, failure.GetContext())
			require.Equal(t, tc.err.Reason(), domain.ReasonString(failure.GetReason()))
		})
	}
}
