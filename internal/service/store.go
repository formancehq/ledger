package service

import (
	"context"
	"math/big"

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
	mapper func(*Res) (To, error)
}

func (cursor GRPCStreamCursor[Res, To]) Next(ctx context.Context) (To, error) {
	next, err := cursor.client.Recv()
	if err != nil {
		var zero To
		return zero, err
	}
	return cursor.mapper(next)
}

func (cursor GRPCStreamCursor[Res, To]) Close() error {
	return cursor.client.CloseSend()
}

var _ Cursor[any] = (*GRPCStreamCursor[any, any])(nil)

func NewGRPCStreamCursor[Res, To any](client grpc.ServerStreamingClient[Res], mapper func(*Res) (To, error)) Cursor[To] {
	return GRPCStreamCursor[Res, To]{client: client, mapper: mapper}
}

type MetricsAware interface {
	Metrics() any
}

// LogWriter handles log writing operations
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package service . LogWriter
type LogWriter interface {
	InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error
}

// LogReader handles log reading operations
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package service . LogReader
type LogReader interface {
	GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) // from: optional log ID to start from (0 = from beginning), to: optional log ID to stop at (0 = until end, inclusive)
	GetLogByID(ctx context.Context, id uint64) (*ledgerpb.Log, error)
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package service . LogStore
type LogStore interface {
	LogWriter
	LogReader
}

type LogReaderFn func(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error)

func (fn LogReaderFn) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	return fn(ctx, from, to)
}

func (fn LogReaderFn) GetLogByID(ctx context.Context, id uint64) (*ledgerpb.Log, error) {
	if id == 0 {
		return nil, nil
	}
	cursor, err := fn(ctx, id-1, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cursor.Close()
	}()
	return cursor.Next(ctx)
}

// RuntimeUpdate contains all the updates to apply to the runtime store
type RuntimeUpdate struct {
	// BalanceDiffs contains balance differences to apply: map[account]map[asset]*big.Int
	// Positive values add to balance, negative values subtract
	BalanceDiffs map[string]map[string]*big.Int
	// AccountMetadata contains metadata updates: map[account]map[key]value
	AccountMetadata map[string]map[string]interface{}
	// AccountMetadataDeletes contains metadata keys to delete: map[account][]keys
	AccountMetadataDeletes map[string][]string
	// IdempotencyKeys contains idempotency entries to insert: map[key]{hash, logID}
	IdempotencyKeys map[string]*ledgerpb.IdempotencyEntry
	// LastProcessedLogID is the ID of the last processed log
	LastProcessedLogID uint64
}

// RuntimeStore handles runtime queries for balances and account metadata
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package service . RuntimeStore
type RuntimeStore interface {
	GetBalances(ctx context.Context, balanceQuery map[string][]string) (ledgerpb.Balances, error)
	GetAccountMetadata(ctx context.Context, accounts []string) (map[string]metadata.Metadata, error)
	// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key
	GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) ([]byte, uint64, error)
	// GetLastProcessedLogID retrieves the ID of the last processed log
	GetLastProcessedLogID(ctx context.Context) (uint64, error)
	// Update applies all runtime updates atomically
	Update(ctx context.Context, update RuntimeUpdate) error
}
