package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type item struct {
	name string
	id   uint64
}

func sliceCursor(items ...item) cursor.Cursor[item] {
	return cursor.NewSliceCursor(items)
}

func collect(t *testing.T, c cursor.Cursor[item]) []item {
	t.Helper()

	out, err := cursor.Collect(c)
	require.NoError(t, err)

	return out
}

func TestPageSizePlusOne(t *testing.T) {
	t.Parallel()

	// Regression pin for the page_size=0 review finding: callers MUST clamp the
	// user-supplied page size BEFORE asking pageSizePlusOne for the fetch slot,
	// otherwise the peek-ahead is short-circuited and x-next-cursor never
	// fires for clients that send the documented "0 = default" value.
	t.Run("zero short-circuits — callers must clamp first", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, uint32(0), pageSizePlusOne(0))
	})

	t.Run("returns pageSize+1 for non-zero inputs", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, uint32(11), pageSizePlusOne(10))
		require.Equal(t, ctrl.MaxPageSize+1, pageSizePlusOne(ctrl.MaxPageSize))
	})

	t.Run("composes safely with ClampPageSize for user input 0", func(t *testing.T) {
		t.Parallel()

		// User sends page_size = 0 → handler clamps to DefaultPageSize, then
		// pageSizePlusOne adds the peek slot. The composition stays within
		// MaxFetchSize, so the controller's ClampFetchSize won't lop off the
		// extra item.
		userInput := uint32(0)
		pageSize := ctrl.ClampPageSize(userInput)
		fetchSize := pageSizePlusOne(pageSize)

		require.Equal(t, ctrl.DefaultPageSize, pageSize)
		require.Equal(t, ctrl.DefaultPageSize+1, fetchSize)
		require.LessOrEqual(t, fetchSize, ctrl.MaxFetchSize)
	})
}

func TestSkipByStringKey(t *testing.T) {
	t.Parallel()

	t.Run("empty cursor returns nil predicate", func(t *testing.T) {
		t.Parallel()

		require.Nil(t, skipByStringKey("", false, func(item) string { return "" }))
	})

	t.Run("forward skips items <= cursor", func(t *testing.T) {
		t.Parallel()

		pred := skipByStringKey("c", false, func(i item) string { return i.name })

		require.True(t, pred(item{name: "a"}))
		require.True(t, pred(item{name: "c"}))
		require.False(t, pred(item{name: "d"}))
	})

	t.Run("reverse skips items >= cursor", func(t *testing.T) {
		t.Parallel()

		pred := skipByStringKey("c", true, func(i item) string { return i.name })

		require.False(t, pred(item{name: "a"}))
		require.True(t, pred(item{name: "c"}))
		require.True(t, pred(item{name: "d"}))
	})
}

func TestSkipByUint64Key(t *testing.T) {
	t.Parallel()

	t.Run("zero cursor returns nil predicate", func(t *testing.T) {
		t.Parallel()

		require.Nil(t, skipByUint64Key(0, false, func(item) uint64 { return 0 }))
	})

	t.Run("forward skips items <= cursor", func(t *testing.T) {
		t.Parallel()

		pred := skipByUint64Key(5, false, func(i item) uint64 { return i.id })

		require.True(t, pred(item{id: 1}))
		require.True(t, pred(item{id: 5}))
		require.False(t, pred(item{id: 6}))
	})

	t.Run("reverse skips items >= cursor", func(t *testing.T) {
		t.Parallel()

		pred := skipByUint64Key(5, true, func(i item) uint64 { return i.id })

		require.False(t, pred(item{id: 1}))
		require.True(t, pred(item{id: 5}))
		require.True(t, pred(item{id: 6}))
	})
}

func TestApplyHandlerPagination(t *testing.T) {
	t.Parallel()

	t.Run("no skip, no reverse passes through", func(t *testing.T) {
		t.Parallel()

		c, err := ApplyHandlerPagination(
			sliceCursor(item{name: "a"}, item{name: "b"}, item{name: "c"}),
			nil, false,
		)
		require.NoError(t, err)

		got := collect(t, c)
		require.Len(t, got, 3)
		require.Equal(t, "a", got[0].name)
	})

	t.Run("reverse applied then skip", func(t *testing.T) {
		t.Parallel()

		// Pipeline ordering matters: reverse first, so the skip predicate sees
		// items in the reversed direction.
		c, err := ApplyHandlerPagination(
			sliceCursor(item{name: "a"}, item{name: "b"}, item{name: "c"}, item{name: "d"}),
			skipByStringKey("c", true, func(i item) string { return i.name }),
			true,
		)
		require.NoError(t, err)

		// Reversed: d, c, b, a. Skip while name >= "c": drop d, c. Pass b, a.
		got := collect(t, c)
		require.Equal(t, []item{{name: "b"}, {name: "a"}}, got)
	})

	t.Run("forward skip honors cursor", func(t *testing.T) {
		t.Parallel()

		c, err := ApplyHandlerPagination(
			sliceCursor(item{name: "a"}, item{name: "b"}, item{name: "c"}, item{name: "d"}),
			skipByStringKey("b", false, func(i item) string { return i.name }),
			false,
		)
		require.NoError(t, err)

		got := collect(t, c)
		require.Equal(t, []item{{name: "c"}, {name: "d"}}, got)
	})
}

// TestValidateListOptions pins the per-endpoint gate that turns unsupported
// ListOptions sub-fields into InvalidArgument instead of letting the request
// flow through and silently produce a result that contradicts the input.
func TestValidateListOptions(t *testing.T) {
	t.Parallel()

	full := ListOptionsSupport{Filter: true, Reverse: true, CheckpointID: true}
	none := ListOptionsSupport{}

	t.Run("nil opts → no error", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateListOptions(nil, none))
	})

	t.Run("filter not supported → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		err := ValidateListOptions(&commonpb.ListOptions{Filter: &commonpb.QueryFilter{}}, none)
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("reverse not supported → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		err := ValidateListOptions(&commonpb.ListOptions{Reverse: true}, none)
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("checkpoint_id not supported → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		err := ValidateListOptions(
			&commonpb.ListOptions{Read: &commonpb.ReadOptions{CheckpointId: 42}},
			none,
		)
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("everything supported → no error even with all fields set", func(t *testing.T) {
		t.Parallel()
		err := ValidateListOptions(&commonpb.ListOptions{
			Filter:  &commonpb.QueryFilter{},
			Reverse: true,
			Read:    &commonpb.ReadOptions{CheckpointId: 42, MinLogSequence: 99},
		}, full)
		require.NoError(t, err)
	})
}
