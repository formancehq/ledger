package storage

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

type Code string

const (
	ConstraintFailed Code = "CONSTRAINT_FAILED"
)

type Error struct {
	Code          Code
	OriginalError error
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

type Store interface {
	LastTransaction(context.Context) (*core.Transaction, error)
	LastMetaID(context.Context) (int64, error)
	SaveTransactions(context.Context, []core.Transaction) error
	CountTransactions(context.Context) (int64, error)
	FindTransactions(context.Context, query.Query) (query.Cursor, error)
	GetTransaction(context.Context, string) (core.Transaction, error)
	AggregateBalances(context.Context, string) (map[string]int64, error)
	AggregateVolumes(context.Context, string) (map[string]map[string]int64, error)
	CountAccounts(context.Context) (int64, error)
	FindAccounts(context.Context, query.Query) (query.Cursor, error)
	SaveMeta(context.Context, int64, string, string, string, string, string) error
	GetMeta(context.Context, string, string) (core.Metadata, error)
	CountMeta(context.Context) (int64, error)
	FindContracts(context.Context) ([]core.Contract, error)
	SaveContract(ctx context.Context, contract core.Contract) error
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

func (n noOpStore) SaveTransactions(ctx context.Context, transactions []core.Transaction) error {
	return nil
}

func (n noOpStore) CountTransactions(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) FindTransactions(ctx context.Context, q query.Query) (query.Cursor, error) {
	return query.Cursor{}, nil
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

func (n noOpStore) FindAccounts(ctx context.Context, q query.Query) (query.Cursor, error) {
	return query.Cursor{}, nil
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

func (n noOpStore) FindContracts(context.Context) ([]core.Contract, error) {
	return nil, nil
}
func (n noOpStore) SaveContract(ctx context.Context, contract core.Contract) error {
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
