package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

type Code string

const (
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

type Store interface {
	GetLastTransaction(ctx context.Context) (*core.Transaction, error)
	CountTransactions(context.Context, query.Transactions) (uint64, error)
	GetTransactions(context.Context, query.Transactions) (sharedapi.Cursor[core.Transaction], error)
	GetTransaction(context.Context, uint64) (core.Transaction, error)
	GetAccount(context.Context, string) (core.Account, error)
	AggregateVolumes(context.Context, string) (core.Volumes, error)
	CountAccounts(context.Context, query.Accounts) (uint64, error)
	GetAccounts(context.Context, query.Accounts) (sharedapi.Cursor[core.Account], error)

	AppendLog(ctx context.Context, log ...core.Log) error
	LastLog(ctx context.Context) (*core.Log, error)
	Logs(ctx context.Context) ([]core.Log, error)

	LoadMapping(ctx context.Context) (*core.Mapping, error)
	SaveMapping(ctx context.Context, m core.Mapping) error
	Initialize(context.Context) (bool, error)
	Name() string
	Close(context.Context) error
}

// A no op store. Useful for testing.
type noOpStore struct{}

func (n noOpStore) GetLastTransaction(ctx context.Context) (*core.Transaction, error) {
	return &core.Transaction{}, nil
}

func (n noOpStore) Logs(ctx context.Context) ([]core.Log, error) {
	return nil, nil
}

func (n noOpStore) AppendLog(ctx context.Context, log ...core.Log) error {
	return nil
}

func (n noOpStore) LastMetaID(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) CountTransactions(ctx context.Context, q query.Transactions) (uint64, error) {
	return 0, nil
}

func (n noOpStore) GetTransactions(ctx context.Context, q query.Transactions) (sharedapi.Cursor[core.Transaction], error) {
	return sharedapi.Cursor[core.Transaction]{}, nil
}

func (n noOpStore) GetTransaction(ctx context.Context, s uint64) (core.Transaction, error) {
	return core.Transaction{}, nil
}

func (n noOpStore) GetAccount(ctx context.Context, s string) (core.Account, error) {
	return core.Account{}, nil
}

func (n noOpStore) AggregateVolumes(ctx context.Context, s string) (core.Volumes, error) {
	return nil, nil
}

func (n noOpStore) LastLog(ctx context.Context) (*core.Log, error) {
	return nil, nil
}

func (n noOpStore) CountAccounts(ctx context.Context, q query.Accounts) (uint64, error) {
	return 0, nil
}

func (n noOpStore) GetAccounts(ctx context.Context, q query.Accounts) (sharedapi.Cursor[core.Account], error) {
	return sharedapi.Cursor[core.Account]{}, nil
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
