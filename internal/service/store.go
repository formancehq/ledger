package service

import (
	"context"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"google.golang.org/grpc"
)

// Cursor provides a way to iterate over a stream of items
type Cursor[T any] interface {
	// Next returns the next item in the cursor
	// Returns io.EOF when there are no more items
	Next(ctx context.Context) (T, error)
	// Close closes the cursor and releases any resources
	Close() error
}

type GRPCStreamCursor[Res, To any] struct {
	client grpc.ServerStreamingClient[Res]
	mapper func(Res) (To, error)
}

func (cursor GRPCStreamCursor[Res, To]) Next(ctx context.Context) (To, error) {
	next, err := cursor.client.Recv()
	if err != nil {
		var zero To
		return zero, err
	}
	return cursor.mapper(*next)
}

func (cursor GRPCStreamCursor[Res, To]) Close() error {
	return cursor.client.CloseSend()
}

var _ Cursor[any] = (*GRPCStreamCursor[any, any])(nil)

func NewGRPCStreamCursor[Res, To any](client grpc.ServerStreamingClient[Res], mapper func(Res) (To, error)) Cursor[To] {
	return GRPCStreamCursor[Res, To]{client: client, mapper: mapper}
}

// LogWriter handles log writing operations
type LogWriter interface {
	InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error
	GetLastSequenceID(ctx context.Context) (uint64, error)
}

// LogReader handles log reading operations
type LogReader interface {
	GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) // from: optional sequence number to start from (0 = from beginning), to: optional sequence number to stop at (0 = until end, inclusive)
}

type LogReaderFn func(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error)

func (fn LogReaderFn) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	return fn(ctx, from, to)
}

// LogStore embeds both LogWriter and LogReader, plus additional methods
type LogStore interface {
	// todo: relax ?
	BalancesStore
	AccountStore
	LogWriter
	LogReader
	GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledgerpb.Log, error)
	GetLastLog(ctx context.Context) (*ledgerpb.Log, error)
}

// Store embeds LogWriter and LogReader
type Store interface {
	LogWriter
	LogReader
}

// BalancesStore handles balance/volume queries
type BalancesStore interface {
	GetBalances(ctx context.Context, balanceQuery map[string][]string) (ledgerpb.Balances, error)
}

type AccountStore interface {
	GetAccountMetadata(ctx context.Context, accounts []string) (map[string]metadata.Metadata, error)
}
