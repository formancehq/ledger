package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

// fakeServerStream stays as a thin wrapper rather than a pure mockgen mock:
// it embeds the generated MockServerStreamingServer[Res] (so it remains a
// grpc.ServerStreamingServer[Res] callers can pass to handlers) while holding
// the in-test state sendPagedToStream tests assert on (captured items,
// merged trailer, optional Send-error injection). The wrapping pattern keeps
// stateful Send/SetTrailer semantics expressible as DoAndReturn closures
// without needing a separate state struct per test.
type fakeServerStream[Res any] struct {
	*MockServerStreamingServer[Res]

	sent     []*Res
	trailer  metadata.MD
	sendErr  error
	sendStop int // when >0, return sendErr on the Nth Send (1-indexed)
}

func newFakeServerStream[Res any](t *testing.T) *fakeServerStream[Res] {
	t.Helper()

	f := &fakeServerStream[Res]{
		MockServerStreamingServer: NewMockServerStreamingServer[Res](gomock.NewController(t)),
		trailer:                   metadata.MD{},
	}

	f.MockServerStreamingServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	f.MockServerStreamingServer.EXPECT().Send(gomock.Any()).DoAndReturn(func(item *Res) error {
		f.sent = append(f.sent, item)

		if f.sendStop > 0 && len(f.sent) == f.sendStop {
			return f.sendErr
		}

		return nil
	}).AnyTimes()

	f.MockServerStreamingServer.EXPECT().SetTrailer(gomock.Any()).Do(func(md metadata.MD) {
		f.trailer = metadata.Join(f.trailer, md)
	}).AnyTimes()

	f.MockServerStreamingServer.EXPECT().SetHeader(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().SendHeader(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().SendMsg(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().RecvMsg(gomock.Any()).Return(nil).AnyTimes()

	return f
}

// trailerCursor pulls the x-next-cursor token (if any) the helper wrote.
func (f *fakeServerStream[Res]) trailerCursor() string {
	v := f.trailer.Get(NextCursorTrailerKey)
	if len(v) == 0 {
		return ""
	}

	return v[0]
}

// upstreamCursor is a cursor that satisfies both Cursor and UpstreamTrailer.
// It feeds a fixed slice of items, then surfaces a fixed next-cursor token at
// EOF — emulating a routed gRPC client whose leader signaled more pages.
type upstreamCursor[T any] struct {
	items      []*T
	index      int
	nextCursor string
}

func (u *upstreamCursor[T]) Next() (*T, error) {
	if u.index >= len(u.items) {
		return nil, errIOEOF
	}

	out := u.items[u.index]
	u.index++

	return out, nil
}

func (u *upstreamCursor[T]) NextCursor() string { return u.nextCursor }
func (u *upstreamCursor[T]) Close() error       { return nil }

var errIOEOF = errIO("EOF")

type errIO string

func (e errIO) Error() string { return string(e) }
func (e errIO) Is(target error) bool {
	// satisfy errors.Is(err, io.EOF) without importing io into the test's
	// public surface
	return target.Error() == "EOF"
}

type stringItem struct{ name string }

func TestSendPagedToStream(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer carries last-sent cursor", func(t *testing.T) {
		t.Parallel()

		// Source has 4 items, pageSize=3 → peek slot fires on item 4. The
		// helper sends the first 3 and emits a trailer keyed on the 3rd item.
		src := cursor.NewSliceCursor([]*stringItem{{name: "a"}, {name: "b"}, {name: "c"}, {name: "d"}})
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), src, stream, "item", 3,
			func(it *stringItem) string { return it.name },
		)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"},
			[]string{stream.sent[0].name, stream.sent[1].name, stream.sent[2].name})
		require.Equal(t, "c", stream.trailerCursor(),
			"resume-after-cursor is exclusive: the cursor MUST be the last SENT item, not the peeked one")
	})

	t.Run("count == pageSize → no trailer (peek does not fire)", func(t *testing.T) {
		t.Parallel()

		src := cursor.NewSliceCursor([]*stringItem{{name: "a"}, {name: "b"}, {name: "c"}})
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), src, stream, "item", 3,
			func(it *stringItem) string { return it.name },
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 3)
		require.Empty(t, stream.trailerCursor(),
			"trailer must NOT fire on an exactly-full page — clients would issue a spurious round-trip")
	})

	t.Run("empty source → no trailer", func(t *testing.T) {
		t.Parallel()

		src := cursor.NewSliceCursor([]*stringItem(nil))
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), src, stream, "item", 3,
			func(it *stringItem) string { return it.name },
		)
		require.NoError(t, err)
		require.Empty(t, stream.sent)
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("cursorOf returns empty → no trailer even when peek fires", func(t *testing.T) {
		t.Parallel()

		// 4 items, pageSize=3 → peek fires, but the cursorOf is intentionally
		// blind (mimics ListLogs' defensive empty-string return when the
		// payload is not Apply). emitTrailer must short-circuit instead of
		// publishing a bogus token.
		src := cursor.NewSliceCursor([]*stringItem{{name: "a"}, {name: "b"}, {name: "c"}, {name: "d"}})
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), src, stream, "item", 3,
			func(_ *stringItem) string { return "" },
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 3)
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("UpstreamTrailer forwarded verbatim on EOF", func(t *testing.T) {
		t.Parallel()

		// Routed-controller scenario: the local cursor has no peek slot
		// (only N items), so it hits EOF naturally. Upstream advertised a
		// resume token via its own trailer; sendPagedToStream must forward
		// that token to the follower's own trailer — using upstream's value
		// verbatim, NOT re-deriving from lastSent.
		up := &upstreamCursor[stringItem]{
			items:      []*stringItem{{name: "x"}, {name: "y"}},
			nextCursor: "from-upstream",
		}
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), up, stream, "item", 5,
			func(it *stringItem) string { return it.name },
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 2)
		require.Equal(t, "from-upstream", stream.trailerCursor(),
			"upstream cursor wins on EOF: re-deriving from lastSent would lose information when zero items were sent this batch")
	})

	t.Run("UpstreamTrailer empty cursor → no trailer", func(t *testing.T) {
		t.Parallel()

		up := &upstreamCursor[stringItem]{
			items:      []*stringItem{{name: "x"}},
			nextCursor: "",
		}
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), up, stream, "item", 5,
			func(it *stringItem) string { return it.name },
		)
		require.NoError(t, err)
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("pageSize=0 drains without trailer", func(t *testing.T) {
		t.Parallel()

		src := cursor.NewSliceCursor([]*stringItem{{name: "a"}, {name: "b"}})
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), src, stream, "item", 0, nil,
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 2)
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("send error surfaces wrapped", func(t *testing.T) {
		t.Parallel()

		src := cursor.NewSliceCursor([]*stringItem{{name: "a"}, {name: "b"}})
		stream := newFakeServerStream[stringItem](t)
		stream.sendStop = 1
		stream.sendErr = errors.New("network blew up")

		err := sendPagedToStream(
			context.Background(), src, stream, "widget", 5,
			func(it *stringItem) string { return it.name },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sending widget")
	})
}

// TestUpstreamPeekCursor pins the helper that bridges a routed gRPC
// streaming client to the local sendPagedToStream peek-ahead.
func TestUpstreamPeekCursor(t *testing.T) {
	t.Parallel()

	t.Run("test-fake fixture exposes upstream trailer at EOF", func(t *testing.T) {
		t.Parallel()

		c := &upstreamCursor[stringItem]{
			items:      []*stringItem{{name: "1"}, {name: "2"}},
			nextCursor: "more-pages-here",
		}

		// Drain.
		for range 2 {
			_, err := c.Next()
			require.NoError(t, err)
		}

		_, err := c.Next()
		require.Error(t, err)

		require.Equal(t, "more-pages-here", c.NextCursor())
	})

	t.Run("real NewUpstreamPeekCursor surfaces the trailer", func(t *testing.T) {
		t.Parallel()

		// Drive the production upstreamPeekCursor with a mockgen streaming
		// client whose Trailer() carries x-next-cursor.
		ctrl := gomock.NewController(t)
		client := NewMockServerStreamingClient[stringItem](ctrl)

		items := []*stringItem{{name: "a"}, {name: "b"}}
		idx := 0
		client.EXPECT().Recv().DoAndReturn(func() (*stringItem, error) {
			if idx >= len(items) {
				return nil, errIOEOF
			}

			out := items[idx]
			idx++

			return out, nil
		}).AnyTimes()
		client.EXPECT().Trailer().Return(metadata.Pairs(NextCursorTrailerKey, "leader-token")).AnyTimes()

		closed := false
		client.EXPECT().CloseSend().DoAndReturn(func() error {
			closed = true

			return nil
		})

		c := NewUpstreamPeekCursor[stringItem](client)
		ut, ok := c.(UpstreamTrailer)
		require.True(t, ok, "cursor returned by NewUpstreamPeekCursor must satisfy UpstreamTrailer")

		// Drain items.
		for range 2 {
			_, err := c.Next()
			require.NoError(t, err)
		}

		// EOF populates the upstream cursor.
		_, err := c.Next()
		require.Error(t, err)

		require.Equal(t, "leader-token", ut.NextCursor())
		require.NoError(t, c.Close(), "Close delegates to the underlying client's CloseSend")
		require.True(t, closed)
	})
}
