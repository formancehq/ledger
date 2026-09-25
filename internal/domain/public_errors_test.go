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
		// wantMetadata is the context PublicErrorDetails must publish. It is
		// spelled out rather than derived from tc.err so the row for `outer`
		// genuinely pins the no-unwrap rule: a helper that walked the chain
		// would answer the nested private error's context here and the
		// assertion would still hold if both sides walked.
		wantMetadata map[string]string
	}{
		{"private", private, true, nil},
		{"business private", &BusinessError{Err: private}, true, nil},
		{"nested business private", &BusinessError{Err: &BusinessError{Err: private}}, true, nil},
		{"ordinary", ordinary, false, map[string]string{"name": "caller-ledger"}},
		{"business ordinary", &BusinessError{Err: ordinary}, false, map[string]string{"name": "caller-ledger"}},
		{"distinct outer reason", outer, false, map[string]string{"operation": "loading index"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			message, metadata, overridden := PublicErrorDetails(tc.err)
			require.Equal(t, tc.overridden, overridden)
			if overridden {
				require.Equal(t, "index is inconsistent", message)
				require.Empty(t, metadata)
				require.Equal(t, "index private-index is inconsistent: private storage detail", tc.err.Error())
				// The diagnostic identity the override withholds lives on the
				// error itself, not on the BusinessError carrying it — a
				// carrier exposes no Metadata of its own.
				require.Equal(t, map[string]string{"index": "private-index", "detail": "private storage detail"}, MetadataOf(private))
			} else {
				require.Equal(t, tc.err.Error(), message)
				require.Equal(t, tc.wantMetadata, metadata)
			}
		})
	}
}

// TestMetadataOfDoesNotWalkTheChain pins the selection rule MetadataOf exists
// to enforce: it reads the context of the error it is handed and nothing
// deeper. Walking the chain would let a wrapped internal cause substitute its
// diagnostic context for the outer reason's, which is exactly the leak #326 and
// EN-1379 close — ErrStorageOperation publishes only the operation it names,
// never the Pebble-level detail it carries.
func TestMetadataOfDoesNotWalkTheChain(t *testing.T) {
	t.Parallel()

	inner := &ErrIndexInconsistent{Index: "private-index", Detail: "private storage detail"}

	require.Equal(t, map[string]string{"operation": "loading index"},
		MetadataOf(&ErrStorageOperation{Operation: "loading index", Cause: inner}))

	// A carrier declares no Metadata of its own, and MetadataOf must not
	// substitute the carried error's.
	require.Nil(t, MetadataOf(&BusinessError{Err: inner}))
}
