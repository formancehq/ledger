package grpc

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
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
	// sendDelay burns wall-clock inside each Send, standing in for a consumer
	// that is slow to drain the stream. Used by the profile phase-attribution
	// tests to prove back-pressure lands in the delivery phase.
	sendDelay time.Duration
}

func newFakeServerStream[Res any](t *testing.T) *fakeServerStream[Res] {
	t.Helper()

	f := &fakeServerStream[Res]{
		MockServerStreamingServer: NewMockServerStreamingServer[Res](gomock.NewController(t)),
		trailer:                   metadata.MD{},
	}

	f.MockServerStreamingServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	f.MockServerStreamingServer.EXPECT().Send(gomock.Any()).DoAndReturn(func(item *Res) error {
		for deadline := time.Now().Add(f.sendDelay); time.Now().Before(deadline); {
			runtime.Gosched()
		}

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

// next and previous return the page tokens the helper wrote, decoded.
func (f *fakeServerStream[Res]) next(t *testing.T) (pagecursor.Cursor, bool) {
	t.Helper()

	return trailerToken(t, f.trailer, NextCursorTrailerKey)
}

func (f *fakeServerStream[Res]) previous(t *testing.T) (pagecursor.Cursor, bool) {
	t.Helper()

	return trailerToken(t, f.trailer, PreviousCursorTrailerKey)
}

// trailerCursor returns the key of the next-page token the helper wrote, or
// "" when it wrote none.
func (f *fakeServerStream[Res]) trailerCursor() string {
	v := f.trailer.Get(NextCursorTrailerKey)
	if len(v) == 0 {
		return ""
	}

	c, err := pagecursor.Decode(v[0])
	if err != nil {
		return "undecodable:" + v[0]
	}

	return c.Key
}

func trailerToken(t *testing.T, md metadata.MD, key string) (pagecursor.Cursor, bool) {
	t.Helper()

	v := md.Get(key)
	if len(v) == 0 {
		return pagecursor.Cursor{}, false
	}

	require.Len(t, v, 1)

	c, err := pagecursor.Decode(v[0])
	require.NoError(t, err)

	return c, true
}

func (f *fakeServerStream[Res]) sentNames() []string {
	names := make([]string, 0, len(f.sent))
	for _, it := range f.sent {
		names = append(names, any(it).(*stringItem).name)
	}

	return names
}

// upstreamCursor feeds a fixed slice of items, then reports whether its
// source signaled more rows — emulating a routed gRPC client whose leader
// capped its response.
type upstreamCursor[T any] struct {
	items   []*T
	index   int
	hasMore bool
}

func (u *upstreamCursor[T]) Next() (*T, error) {
	if u.index >= len(u.items) {
		return nil, errIOEOF
	}

	out := u.items[u.index]
	u.index++

	return out, nil
}

func (u *upstreamCursor[T]) HasMore() bool { return u.hasMore }
func (u *upstreamCursor[T]) Close() error  { return nil }

var errIOEOF = errIO("EOF")

type errIO string

func (e errIO) Error() string { return string(e) }
func (e errIO) Is(target error) bool {
	// satisfy errors.Is(err, io.EOF) without importing io into the test's
	// public surface
	return target.Error() == "EOF"
}

type stringItem struct{ name string }

func names(ns ...string) []*stringItem {
	items := make([]*stringItem, 0, len(ns))
	for _, n := range ns {
		items = append(items, &stringItem{name: n})
	}

	return items
}

func itemName(it *stringItem) string { return it.name }

func TestSendPagedToStream(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → next is the last sent item", func(t *testing.T) {
		t.Parallel()

		// Source has 4 items, pageSize=3 → peek slot fires on item 4. The
		// helper sends the first 3 and links past the 3rd.
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("a", "b", "c", "d")), stream, "item", 3, pagecursor.Cursor{}, itemName)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"}, stream.sentNames())

		next, ok := stream.next(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Key: "c"}, next,
			"resume is exclusive: the cursor MUST be the last SENT item, not the peeked one")

		_, ok = stream.previous(t)
		require.False(t, ok, "the first page has nothing before it")
	})

	t.Run("count == pageSize → no next (peek does not fire)", func(t *testing.T) {
		t.Parallel()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("a", "b", "c")), stream, "item", 3, pagecursor.Cursor{}, itemName)
		require.NoError(t, err)
		require.Len(t, stream.sent, 3)

		_, ok := stream.next(t)
		require.False(t, ok, "next must NOT fire on an exactly-full page — clients would issue a spurious round-trip")
	})

	t.Run("resumed page links back to its first item", func(t *testing.T) {
		t.Parallel()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("d", "e")), stream, "item", 3, pagecursor.Cursor{Key: "c"}, itemName)
		require.NoError(t, err)

		previous, ok := stream.previous(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Key: "d", Back: true}, previous)
	})

	t.Run("empty resumed page links back to the last page", func(t *testing.T) {
		t.Parallel()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor([]*stringItem(nil)), stream, "item", 3, pagecursor.Cursor{Key: "z"}, itemName)
		require.NoError(t, err)
		require.Empty(t, stream.sent)

		_, ok := stream.next(t)
		require.False(t, ok)

		previous, ok := stream.previous(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Back: true}, previous)
	})

	t.Run("back page is sent in query order", func(t *testing.T) {
		t.Parallel()

		// Back from "e": the source reads the opposite order, d c b a.
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("d", "c", "b", "a")), stream, "item", 3, pagecursor.Cursor{Key: "e", Back: true}, itemName)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "c", "d"}, stream.sentNames())

		next, ok := stream.next(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Key: "d"}, next)

		previous, ok := stream.previous(t)
		require.True(t, ok, "the extra row proves a page before this one")
		require.Equal(t, pagecursor.Cursor{Key: "b", Back: true}, previous)
	})

	t.Run("back page reaching the head has no previous", func(t *testing.T) {
		t.Parallel()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("b", "a")), stream, "item", 3, pagecursor.Cursor{Key: "c", Back: true}, itemName)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, stream.sentNames())

		_, ok := stream.previous(t)
		require.False(t, ok)
	})

	t.Run("cursorOf returns empty → no link through that item", func(t *testing.T) {
		t.Parallel()

		// Mimics ListLogs' defensive empty-string return when the payload is
		// not Apply: the helper must not publish a bogus token.
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("a", "b", "c", "d")), stream, "item", 3, pagecursor.Cursor{Key: "0"},
			func(_ *stringItem) string { return "" })
		require.NoError(t, err)
		require.Len(t, stream.sent, 3)
		require.Empty(t, stream.trailer)
	})

	t.Run("upstream more on EOF links past the last sent item", func(t *testing.T) {
		t.Parallel()

		// Routed-controller scenario: the leader capped its response, so the
		// local cursor hits EOF without a peek slot. The leader's own peek is
		// the proof of a further page.
		up := &upstreamCursor[stringItem]{items: names("x", "y"), hasMore: true}
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), up, stream, "item", 5, pagecursor.Cursor{}, itemName)
		require.NoError(t, err)
		require.Len(t, stream.sent, 2)

		next, ok := stream.next(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Key: "y"}, next)
	})

	t.Run("upstream more on a back page links further back", func(t *testing.T) {
		t.Parallel()

		up := &upstreamCursor[stringItem]{items: names("y", "x"), hasMore: true}
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), up, stream, "item", 5, pagecursor.Cursor{Key: "z", Back: true}, itemName)
		require.NoError(t, err)
		require.Equal(t, []string{"x", "y"}, stream.sentNames())

		previous, ok := stream.previous(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Key: "x", Back: true}, previous)
	})

	t.Run("upstream without more → no next", func(t *testing.T) {
		t.Parallel()

		up := &upstreamCursor[stringItem]{items: names("x")}
		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), up, stream, "item", 5, pagecursor.Cursor{}, itemName)
		require.NoError(t, err)

		_, ok := stream.next(t)
		require.False(t, ok)
	})

	t.Run("pageSize=0 drains without trailer", func(t *testing.T) {
		t.Parallel()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("a", "b")), stream, "item", 0, pagecursor.Cursor{Key: "0"}, nil)
		require.NoError(t, err)
		require.Len(t, stream.sent, 2)
		require.Empty(t, stream.trailer)
	})

	t.Run("send error surfaces wrapped", func(t *testing.T) {
		t.Parallel()

		for _, page := range []pagecursor.Cursor{{}, {Back: true}} {
			stream := newFakeServerStream[stringItem](t)
			stream.sendStop = 1
			stream.sendErr = errors.New("network blew up")

			err := sendPagedToStream(context.Background(), cursor.NewSliceCursor(names("a", "b")), stream, "widget", 5, page, itemName)
			require.ErrorContains(t, err, "sending widget")
		}
	})
}

// TestUpstreamPeekCursor pins the helper that bridges a routed gRPC
// streaming client to the local sendPagedToStream peek-ahead.
func TestUpstreamPeekCursor(t *testing.T) {
	t.Parallel()

	t.Run("real NewUpstreamPeekCursor reports the leader's next page", func(t *testing.T) {
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

		c := NewUpstreamPeekCursor[stringItem](context.Background(), client)
		require.False(t, cursor.SourceHasMore(c), "nothing is known before EOF")

		// Drain items.
		for range 2 {
			_, err := c.Next()
			require.NoError(t, err)
		}

		// EOF reads the leader's trailer.
		_, err := c.Next()
		require.Error(t, err)

		require.True(t, cursor.SourceHasMore(c))
		require.NoError(t, c.Close(), "Close delegates to the underlying client's CloseSend")
		require.True(t, closed)
	})
}
