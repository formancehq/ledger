package node

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

func TestParseReadIndexContext_Valid(t *testing.T) {
	t.Parallel()

	// Round-trip: make then parse
	original := uint64(12345)
	rctx := makeReadIndexContext(original)

	id, ok := parseReadIndexContext(rctx)
	require.True(t, ok)
	require.Equal(t, original, id)
}

func TestParseReadIndexContext_MaxValue(t *testing.T) {
	t.Parallel()

	rctx := makeReadIndexContext(^uint64(0))

	id, ok := parseReadIndexContext(rctx)
	require.True(t, ok)
	require.Equal(t, ^uint64(0), id)
}

func TestParseReadIndexContext_Zero(t *testing.T) {
	t.Parallel()

	rctx := makeReadIndexContext(0)

	id, ok := parseReadIndexContext(rctx)
	require.True(t, ok)
	require.Equal(t, uint64(0), id)
}

func TestParseReadIndexContext_WrongLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rctx []byte
	}{
		{name: "nil", rctx: nil},
		{name: "empty", rctx: []byte{}},
		{name: "too short", rctx: []byte{1, 2, 3}},
		{name: "too long", rctx: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{name: "seven bytes", rctx: []byte{1, 2, 3, 4, 5, 6, 7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, ok := parseReadIndexContext(tt.rctx)
			require.False(t, ok)
			require.Equal(t, uint64(0), id)
		})
	}
}

func TestFailAllPendingReads(t *testing.T) {
	t.Parallel()

	n := &Node{
		pendingReads: &SyncMap[uint64, *readIndexRequest]{},
	}

	// Store some pending reads
	f1 := futures.New[uint64]()
	f2 := futures.New[uint64]()
	f3 := futures.New[uint64]()

	n.pendingReads.Store(1, &readIndexRequest{future: f1})
	n.pendingReads.Store(2, &readIndexRequest{future: f2})
	n.pendingReads.Store(3, &readIndexRequest{future: f3})

	testErr := errors.New("leadership lost")
	n.failAllPendingReads(testErr)

	// All futures should be resolved with the error
	_, err := f1.Wait()
	require.ErrorIs(t, err, testErr)

	_, err = f2.Wait()
	require.ErrorIs(t, err, testErr)

	_, err = f3.Wait()
	require.ErrorIs(t, err, testErr)

	// pendingReads should be empty
	count := 0
	n.pendingReads.Range(func(_ uint64, _ *readIndexRequest) bool {
		count++

		return true
	})

	require.Zero(t, count)
}

func TestFailAllPendingReads_Empty(t *testing.T) {
	t.Parallel()

	n := &Node{
		pendingReads: &SyncMap[uint64, *readIndexRequest]{},
	}

	// Should not panic on empty map
	n.failAllPendingReads(errors.New("no pending reads"))
}
