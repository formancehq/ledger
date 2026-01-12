package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/numscript"
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
	AccountMetadata map[string]map[string]string
	// AccountMetadataDeletes contains metadata keys to delete: map[account][]keys
	AccountMetadataDeletes map[string][]string
	// IdempotencyKeys contains idempotency entries to insert: map[key]{hash, logID}
	IdempotencyKeys map[string]*ledgerpb.IdempotencyEntry
	// TransactionIDs contains transaction ID to log ID mappings: map[transactionID]logID
	TransactionIDs map[uint64]uint64
	// RevertedTransactionIDs contains transaction IDs that have been reverted: map[transactionID]bool
	RevertedTransactionIDs map[uint64]bool
	// LastProcessedLogID is the ID of the last processed log
	LastProcessedLogID uint64
}

// RuntimeStore handles runtime queries and provides log access.
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package service . RuntimeStore
type RuntimeStore interface {
	LogStore
	GetBalances(ctx context.Context, balanceQuery map[string][]string) (ledgerpb.Balances, error)
	GetAccountMetadata(ctx context.Context, accounts []string) (map[string]metadata.Metadata, error)
	// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key
	GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) ([]byte, uint64, error)
	// GetLogIDForTransactionID retrieves the log ID for a given transaction ID
	GetLogIDForTransactionID(ctx context.Context, transactionID uint64) (uint64, error)
	// IsTransactionReverted checks if a transaction has been reverted
	IsTransactionReverted(ctx context.Context, transactionID uint64) (bool, error)
	// GetLastProcessedLogID retrieves the ID of the last processed log
	GetLastProcessedLogID(ctx context.Context) (uint64, error)
}

type unitOfWork struct {
	RuntimeStore
	KeySetLocker
	releases []func()
}

func (s *unitOfWork) LockKeys(ctx context.Context, keys ...string) (func(), error) {
	release, err := s.KeySetLocker.LockKeys(ctx, keys...)
	if err != nil {
		return nil, err
	}
	s.releases = append(s.releases, release)

	return release, nil
}

func (s *unitOfWork) TryLockKeys(ctx context.Context, keys ...string) (func(), error) {
	release, err := s.KeySetLocker.TryLockKeys(ctx, keys...)
	if err != nil {
		return nil, err
	}
	s.releases = append(s.releases, release)

	return release, nil
}

func (s *unitOfWork) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	// Convert numscript.BalanceQuery to our format
	balanceQuery := make(map[string][]string)
	for account, assets := range q {
		balanceQuery[account] = assets
	}

	lockKeys := makeBalanceLockKeys(balanceQuery)
	_, err := s.LockKeys(ctx, lockKeys...)
	if err != nil {
		return nil, err
	}

	balances, err := s.RuntimeStore.GetBalances(ctx, balanceQuery)
	if err != nil {
		return nil, err
	}

	// Convert to numscript.Balances format
	result := make(numscript.Balances)
	for account, accountBalances := range balances {
		result[account] = make(map[string]*big.Int)
		for asset, balance := range accountBalances {
			result[account][asset] = balance
		}
	}

	return result, nil
}

// GetAccountsMetadata retrieves account metadata for accounts in the query
func (s *unitOfWork) GetAccountsMetadata(ctx context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	// Convert numscript.MetadataQuery (map[string]struct{}) to []string
	accounts := make([]string, 0, len(q))
	for address := range q {
		accounts = append(accounts, address)
	}

	// Get metadata from the runtime store
	metadataMap, err := s.RuntimeStore.GetAccountMetadata(ctx, accounts)
	if err != nil {
		return nil, err
	}

	// Convert to numscript.AccountsMetadata format (map[string]map[string]string)
	result := make(numscript.AccountsMetadata)
	for address, accountMetadata := range metadataMap {
		result[address] = accountMetadata
	}

	// Ensure all requested accounts are in the result (even if empty)
	for address := range q {
		if _, exists := result[address]; !exists {
			result[address] = make(map[string]string)
		}
	}

	return result, nil
}

func (s *unitOfWork) ReleaseLocks() {
	for _, release := range s.releases {
		release()
	}
}

func makeBalanceLockKeys(balanceQuery map[string][]string) []string {
	lockKeys := make([]string, 0)
	for account, assets := range balanceQuery {
		for _, asset := range assets {
			lockKeys = append(lockKeys, fmt.Sprintf("%s:%s", account, asset))
		}
	}
	return lockKeys
}
