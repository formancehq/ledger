package postgres

import (
	"context"

	"github.com/jackc/pgx/v4"
	"numary.io/ledger/config"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
)

type PGStore struct {
	conn *pgx.Conn
}

func NewStore(c config.Config) (*PGStore, error) {
	var store *PGStore

	conn, err := pgx.Connect(
		context.Background(),
		"postgresql://localhost/postgres",
	)

	if err != nil {
		return store, err
	}

	store = &PGStore{
		conn: conn,
	}

	return store, nil
}

func (s *PGStore) Initialize() error {
	return nil
}

func (s *PGStore) Close() {

}

func (s *PGStore) SaveTransactions([]core.Transaction) error {
	return nil
}

func (s *PGStore) CountTransactions() (int64, error) {
	return 0, nil
}

func (s *PGStore) FindTransactions(query.Query) (query.Cursor, error) {
	return query.Cursor{}, nil
}

func (s *PGStore) AggregateBalances(string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (s *PGStore) CountAccounts() (int64, error) {
	return 0, nil
}

func (s *PGStore) FindAccounts(query.Query) (query.Cursor, error) {
	return query.Cursor{}, nil
}
