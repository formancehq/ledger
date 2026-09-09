package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicErrorDetails(t *testing.T) {
	t.Parallel()

	private := &ErrIndexInconsistent{Index: "private-index", Detail: "private storage detail"}
	ordinary := &ErrLedgerNotFound{Name: "caller-ledger"}
	outer := &ErrStorageOperation{Operation: "loading index", Cause: private}
	for _, tc := range []struct {
		name       string
		err        Describable
		overridden bool
	}{
		{"private", private, true},
		{"business private", &BusinessError{Err: private}, true},
		{"nested business private", &BusinessError{Err: &BusinessError{Err: private}}, true},
		{"ordinary", ordinary, false},
		{"business ordinary", &BusinessError{Err: ordinary}, false},
		{"distinct outer reason", outer, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			message, metadata, overridden := PublicErrorDetails(tc.err)
			require.Equal(t, tc.overridden, overridden)
			if overridden {
				require.Equal(t, "index is inconsistent", message)
				require.Empty(t, metadata)
				require.Equal(t, "index private-index is inconsistent: private storage detail", tc.err.Error())
				require.Equal(t, map[string]string{"index": "private-index", "detail": "private storage detail"}, tc.err.Metadata())
			} else {
				require.Equal(t, tc.err.Error(), message)
				require.Equal(t, tc.err.Metadata(), metadata)
			}
		})
	}
}
