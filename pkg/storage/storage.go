package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
)

type Code string

const (
	QueryDefaultPageSize = 15

	ConstraintFailed Code = "CONSTRAINT_FAILED"
	TooManyClient    Code = "TOO_MANY_CLIENT"
	Unknown          Code = "UNKNOWN"
)

type Error struct {
	Code          Code
	OriginalError error
}

func (e Error) Is(err error) bool {
	storageErr, ok := err.(*Error)
	if !ok {
		return false
	}
	if storageErr.Code == "" {
		return true
	}
	return storageErr.Code == e.Code
}

func (e Error) Error() string {
	return fmt.Sprintf("%s [%s]", e.OriginalError, e.Code)
}

func NewError(code Code, originalError error) *Error {
	return &Error{
		Code:          code,
		OriginalError: originalError,
	}
}

func IsError(err error) bool {
	return IsErrorCode(err, "")
}

func IsErrorCode(err error, code Code) bool {
	return errors.Is(err, &Error{
		Code: code,
	})
}

func IsTooManyClientError(err error) bool {
	return IsErrorCode(err, TooManyClient)
}

type API interface {
	GetLastTransaction(ctx context.Context) (*core.Transaction, error)
	CountTransactions(context.Context, TransactionsQuery) (uint64, error)
	GetTransactions(context.Context, TransactionsQuery) (sharedapi.Cursor[core.Transaction], error)
	GetTransaction(ctx context.Context, txid uint64) (*core.Transaction, error)
	GetAccount(ctx context.Context, accountAddress string) (*core.Account, error)
	GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error)
	GetVolumes(ctx context.Context, accountAddress, asset string) (core.Volumes, error)
	CountAccounts(context.Context, AccountsQuery) (uint64, error)
	GetAccounts(context.Context, AccountsQuery) (sharedapi.Cursor[core.Account], error)
	GetBalances(context.Context, BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error)
	GetBalancesAggregated(context.Context, BalancesQuery) (core.AssetsBalances, error)
	LastLog(ctx context.Context) (*core.Log, error)
	Logs(ctx context.Context) ([]core.Log, error)
	LoadMapping(ctx context.Context) (*core.Mapping, error)

	UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata, at time.Time) error
	UpdateAccountMetadata(ctx context.Context, id string, metadata core.Metadata, at time.Time) error
	Commit(ctx context.Context, txs ...core.Transaction) error
	SaveMapping(ctx context.Context, m core.Mapping) error
	Name() string
}

type Store interface {
	API
	Initialize(context.Context) (bool, error)
	Close(context.Context) error
	WithTX(ctx context.Context, callback func(api API) error) error
}

// A no op store. Useful for testing.
type noOpStore struct{}

func (n noOpStore) WithTX(ctx context.Context, callback func(api API) error) error {
	return nil
}

func (n noOpStore) CommitRevert(ctx context.Context, reverted, revert core.Transaction) error {
	return nil
}

func (n noOpStore) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata, at time.Time) error {
	return nil
}

func (n noOpStore) UpdateAccountMetadata(ctx context.Context, id string, metadata core.Metadata, at time.Time) error {
	return nil
}

func (n noOpStore) Commit(ctx context.Context, txs ...core.Transaction) error {
	return nil
}

func (n noOpStore) GetVolumes(ctx context.Context, accountAddress, asset string) (core.Volumes, error) {
	return core.Volumes{}, nil
}

func (n noOpStore) GetLastTransaction(ctx context.Context) (*core.Transaction, error) {
	return &core.Transaction{}, nil
}

func (n noOpStore) Logs(ctx context.Context) ([]core.Log, error) {
	return nil, nil
}

func (n noOpStore) LastMetaID(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) CountTransactions(ctx context.Context, q TransactionsQuery) (uint64, error) {
	return 0, nil
}

func (n noOpStore) GetTransactions(ctx context.Context, q TransactionsQuery) (sharedapi.Cursor[core.Transaction], error) {
	return sharedapi.Cursor[core.Transaction]{}, nil
}

func (n noOpStore) GetTransaction(ctx context.Context, txid uint64) (*core.Transaction, error) {
	return nil, nil
}

func (n noOpStore) GetAccount(ctx context.Context, accountAddress string) (*core.Account, error) {
	return nil, nil
}

func (n noOpStore) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error) {
	return nil, nil
}

func (n noOpStore) LastLog(ctx context.Context) (*core.Log, error) {
	return nil, nil
}

func (n noOpStore) CountAccounts(ctx context.Context, q AccountsQuery) (uint64, error) {
	return 0, nil
}

func (n noOpStore) GetAccounts(ctx context.Context, q AccountsQuery) (sharedapi.Cursor[core.Account], error) {
	return sharedapi.Cursor[core.Account]{}, nil
}

func (n noOpStore) GetBalances(ctx context.Context, q BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error) {
	return sharedapi.Cursor[core.AccountsBalances]{}, nil
}

func (n noOpStore) GetBalancesAggregated(ctx context.Context, q BalancesQuery) (core.AssetsBalances, error) {
	return core.AssetsBalances{}, nil
}

func (n noOpStore) GetMeta(ctx context.Context, s string, s2 string) (core.Metadata, error) {
	return core.Metadata{}, nil
}

func (n noOpStore) CountMeta(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) Initialize(ctx context.Context) (bool, error) {
	return false, nil
}

func (n noOpStore) LoadMapping(context.Context) (*core.Mapping, error) {
	return nil, nil
}

func (n noOpStore) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return nil
}

func (n noOpStore) Name() string {
	return "noop"
}

func (n noOpStore) Close(ctx context.Context) error {
	return nil
}

var _ Store = &noOpStore{}

func NoOpStore() *noOpStore {
	return &noOpStore{}
}
