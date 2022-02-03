package storage

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"testing"
)

type noOpStorage struct {
	Store
}

func (noOpStorage) SaveTransactions(context.Context, []core.Transaction) (map[int]error, error) {
	return nil, nil
}
func (noOpStorage) SaveMeta(context.Context, int64, string, string, string, string, string) error {
	return nil
}

func TestCacheState(t *testing.T) {
	s := NewCachedStateStorage(noOpStorage{})
	transactions := []core.Transaction{{}, {}, {}, {
		ID: 3,
	}}
	_, err := s.SaveTransactions(context.Background(), transactions)
	if err != nil {
		t.Fatalf("unable to save transactions: %s", err)
	}
	lastTransaction, err := s.LastTransaction(context.Background())
	if err != nil {
		t.Fatalf("error fetching last transaction: %s", err)
	}
	if lastTransaction == nil {
		t.Fatalf("cached version should not be nil")
	}
	if lastTransaction.ID != 3 {
		t.Fatalf("last transaction id must be 3")
	}

	err = s.SaveMeta(context.Background(), 12, "", "", "", "", "")
	if err != nil {
		t.Fatalf("unable to save meta: %s", err)
	}
	lastMetaID, err := s.LastMetaID(context.Background())
	if err != nil {
		t.Fatalf("error fetching lastMetaId: %s", err)
	}
	if lastMetaID != 12 {
		t.Fatalf("last meta id must be 12")
	}
}
