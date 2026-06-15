package grpc

import (
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// GRPCStreamCursor implements cursor.Cursor[To] by reading from a gRPC server stream.
type GRPCStreamCursor[Res, To any] struct {
	client grpc.ServerStreamingClient[Res]
	mapper func(*Res) (To, error)
}

func (c GRPCStreamCursor[Res, To]) Next() (To, error) {
	next, err := c.client.Recv()
	if err != nil {
		if status.Code(err) == codes.Canceled {
			err = io.EOF
		}

		var zero To

		return zero, err
	}

	return c.mapper(next)
}

func (c GRPCStreamCursor[Res, To]) Close() error {
	return c.client.CloseSend()
}

var _ cursor.Cursor[any] = (*GRPCStreamCursor[any, any])(nil)

// NewGRPCStreamCursor creates a cursor that reads from a gRPC server stream and maps each element.
func NewGRPCStreamCursor[Res, To any](client grpc.ServerStreamingClient[Res], mapper func(*Res) (To, error)) cursor.Cursor[To] {
	return GRPCStreamCursor[Res, To]{client: client, mapper: mapper}
}

// NewGRPCIdentityCursor creates a GRPCStreamCursor with an identity mapper.
func NewGRPCIdentityCursor[T any](client grpc.ServerStreamingClient[T]) cursor.Cursor[*T] {
	return NewGRPCStreamCursor(client, func(res *T) (*T, error) {
		return res, nil
	})
}

// UpstreamTrailer is implemented by cursors that expose an upstream
// x-next-cursor trailer once they're exhausted. sendPagedToStream uses this
// to surface a follower-side trailer when the routed leader signaled more
// pages but the local cursor itself hit EOF (no peek slot available).
//
// Direct cursor consumers (HTTP compatibility shims, drain loops, etc.) just
// see a normal EOF and don't have to know about the trailer — they can
// type-assert when they care.
type UpstreamTrailer interface {
	NextCursor() string
}

// upstreamPeekCursor wraps a streaming gRPC cursor used by a routed
// BucketGrpcClient so the follower-side sendPagedToStream peek-ahead can
// still fire when the leader signaled more pages via x-next-cursor.
//
// The leader caps its response at MaxPageSize and only advertises the extra
// page through the trailer; without this wrapper, the follower-side cursor
// would also hit EOF on a full page and the follower's sendPagedToStream
// would never emit a trailer of its own, breaking pagination for clustered
// deployments. We expose the upstream cursor as a side channel
// (UpstreamTrailer.NextCursor) so generic Cursor[*T] consumers — HTTP
// compatibility reads, drain loops — get a normal EOF and never see a fake
// record.
type upstreamPeekCursor[Res any] struct {
	client     grpc.ServerStreamingClient[Res]
	exhausted  bool
	nextCursor string
}

func (c *upstreamPeekCursor[Res]) Next() (*Res, error) {
	item, err := c.client.Recv()
	if err == nil {
		return item, nil
	}

	if status.Code(err) == codes.Canceled {
		err = io.EOF
	}

	if errors.Is(err, io.EOF) && !c.exhausted {
		c.exhausted = true
		c.nextCursor = nextCursorFromTrailer(c.client.Trailer())
	}

	return nil, err
}

func (c *upstreamPeekCursor[Res]) NextCursor() string {
	return c.nextCursor
}

func (c *upstreamPeekCursor[Res]) Close() error {
	return c.client.CloseSend()
}

// NewUpstreamPeekCursor wraps the streaming gRPC client used by routed
// BucketGrpcClient methods. The returned cursor satisfies cursor.Cursor[*T]
// (so generic consumers see only real items + io.EOF) and additionally
// satisfies UpstreamTrailer so sendPagedToStream can pick up the leader's
// x-next-cursor.
func NewUpstreamPeekCursor[T any](client grpc.ServerStreamingClient[T]) cursor.Cursor[*T] {
	return &upstreamPeekCursor[T]{client: client}
}

// ListOptionsSupport captures, per resource, which ListOptions sub-fields the
// handler honors. Each bool is true when the option is implemented; non-zero
// values on unsupported fields are rejected with InvalidArgument so clients
// don't get successful responses that silently contradict their request.
type ListOptionsSupport struct {
	Filter       bool
	Reverse      bool
	CheckpointID bool
}

// ValidateListOptions returns an InvalidArgument status when opts carries a
// non-default value on a field the handler does not yet implement. min_log_sequence
// and the opaque cursor + page_size are always considered supported (they're
// honored by every list handler).
func ValidateListOptions(opts *commonpb.ListOptions, support ListOptionsSupport) error {
	if opts == nil {
		return nil
	}

	if !support.Filter && opts.GetFilter() != nil {
		return status.Error(codes.InvalidArgument, "options.filter is not supported on this endpoint")
	}

	if !support.Reverse && opts.GetReverse() {
		return status.Error(codes.InvalidArgument, "options.reverse is not supported on this endpoint")
	}

	if !support.CheckpointID && opts.GetRead().GetCheckpointId() != 0 {
		return status.Error(codes.InvalidArgument, "options.read.checkpoint_id is not supported on this endpoint")
	}

	return nil
}

// pageSizePlusOne returns pageSize+1 for callers that ask their source cursor
// for one extra item so sendPagedToStream's peek-ahead can fire. The +1 is
// accepted by ctrl.ClampFetchSize all the way up to MaxFetchSize
// (= MaxPageSize+1), so the trailer correctly surfaces even when the user
// requested the maximum page size.
func pageSizePlusOne(pageSize uint32) uint32 {
	if pageSize == 0 {
		return 0
	}

	return pageSize + 1
}

// skipByStringKey returns a SkipWhileCursor predicate that drops items at the
// start of a sorted stream whose key is at or before the provided cursor
// (ascending) or at or after it (when reverse). Returns nil if no cursor is
// set, so ApplyHandlerPagination can short-circuit the SkipWhile wrap.
func skipByStringKey[T any](cursor string, reverse bool, keyFn func(T) string) func(T) bool {
	if cursor == "" {
		return nil
	}

	if reverse {
		return func(item T) bool { return keyFn(item) >= cursor }
	}

	return func(item T) bool { return keyFn(item) <= cursor }
}

// skipByUint64Key is the uint64-cursor analogue of skipByStringKey. A zero
// cursor disables skipping.
func skipByUint64Key[T any](cursor uint64, reverse bool, keyFn func(T) uint64) func(T) bool {
	if cursor == 0 {
		return nil
	}

	if reverse {
		return func(item T) bool { return keyFn(item) >= cursor }
	}

	return func(item T) bool { return keyFn(item) <= cursor }
}

// ApplyHandlerPagination wraps a controller cursor with the standard
// order/skip semantics applied at the gRPC handler layer:
//   - if reverse is true, the cursor is drained and reversed in memory.
//     This is suitable only for endpoints whose total cardinality is
//     bounded (in-memory raft state, small read-store collections).
//     Callers that need efficient backward iteration over unbounded streams
//     (logs, accounts, transactions) must implement reverse at the controller
//     level — drain-and-reverse here would OOM on those.
//   - if skipPredicate is non-nil, the contiguous prefix of items for which
//     it returns true is dropped (used to honor an opaque --cursor resume value).
//
// Page-size capping is intentionally NOT done here: sendPagedToStream applies
// it via peek-ahead so the x-next-cursor trailer is only set when an actual
// (pageSize+1)th item has been observed.
func ApplyHandlerPagination[T any](
	c cursor.Cursor[T],
	skipPredicate func(item T) bool,
	reverse bool,
) (cursor.Cursor[T], error) {
	if reverse {
		reversed, err := cursor.Reverse(c)
		if err != nil {
			return nil, err
		}

		c = reversed
	}

	if skipPredicate != nil {
		c = cursor.NewSkipWhileCursor(c, skipPredicate)
	}

	return c, nil
}
