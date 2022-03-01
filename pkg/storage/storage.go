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

var ErrAborted = errors.New("aborted transactions")

type Error struct {
	Code          Code
	OriginalError error
}

func (e Error) Is(err error) bool {
	eerr, ok := err.(*Error)
	if !ok {
		return false
	}
	if eerr.Code == "" {
		return true
	}
	return eerr.Code == e.Code
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
	LastTransaction(context.Context) (*core.Transaction, error)
	LastMetaID(context.Context) (int64, error)
	SaveTransactions(context.Context, []core.Transaction) (map[int]error, error)
	CountTransactions(context.Context) (int64, error)
	FindTransactions(context.Context, query.Query) (sharedapi.Cursor, error)
	GetTransaction(context.Context, string) (core.Transaction, error)
	AggregateBalances(context.Context, string) (map[string]int64, error)
	AggregateVolumes(context.Context, string) (map[string]map[string]int64, error)
	CountAccounts(context.Context) (int64, error)
	FindAccounts(context.Context, query.Query) (sharedapi.Cursor, error)
	SaveMeta(context.Context, int64, string, string, string, string, string) error
	GetMeta(context.Context, string, string) (core.Metadata, error)
	CountMeta(context.Context) (int64, error)
	LoadMapping(ctx context.Context) (*core.Mapping, error)
	SaveMapping(ctx context.Context, m core.Mapping) error
	Initialize(context.Context) error
	Name() string
	Close(context.Context) error
}

// A no op store. Useful for testing.
type noOpStore struct{}

func (n noOpStore) LastTransaction(ctx context.Context) (*core.Transaction, error) {
	return nil, nil
}

func (n noOpStore) LastMetaID(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) SaveTransactions(ctx context.Context, transactions []core.Transaction) (map[int]error, error) {
	return nil, nil
}

func (n noOpStore) CountTransactions(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) FindTransactions(ctx context.Context, q query.Query) (sharedapi.Cursor, error) {
	return sharedapi.Cursor{}, nil
}

func (n noOpStore) GetTransaction(ctx context.Context, s string) (core.Transaction, error) {
	return core.Transaction{}, nil
}

func (n noOpStore) AggregateBalances(ctx context.Context, s string) (map[string]int64, error) {
	return nil, nil
}

func (n noOpStore) AggregateVolumes(ctx context.Context, s string) (map[string]map[string]int64, error) {
	return nil, nil
}

func (n noOpStore) CountAccounts(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) FindAccounts(ctx context.Context, q query.Query) (sharedapi.Cursor, error) {
	return sharedapi.Cursor{}, nil
}

func (n noOpStore) SaveMeta(ctx context.Context, i int64, s string, s2 string, s3 string, s4 string, s5 string) error {
	return nil
}

func (n noOpStore) GetMeta(ctx context.Context, s string, s2 string) (core.Metadata, error) {
	return core.Metadata{}, nil
}

func (n noOpStore) CountMeta(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) Initialize(ctx context.Context) error {
	return nil
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
