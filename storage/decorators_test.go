package storage

import (
	"github.com/numary/ledger/core"
	"testing"
)

type noOpStorage struct {
	Store
}

func (noOpStorage) SaveTransactions([]core.Transaction) error {
	return nil
}
func (noOpStorage) SaveMeta(int64, string, string, string, string, string) error {
	return nil
}

func TestCacheState(t *testing.T) {
	s := NewCachedStateStorage(noOpStorage{})
	transactions := []core.Transaction{{}, {}, {}, {
		ID: 3,
	}}
	err := s.SaveTransactions(transactions)
	if err != nil {
		t.Fatalf("unable to save transactions: %s", err)
	}
	lastTransaction, err := s.LastTransaction()
	if err != nil {
		t.Fatalf("error fetching last transaction: %s", err)
	}
	if lastTransaction == nil {
		t.Fatalf("cached version should not be nil")
	}
	if lastTransaction.ID != 3 {
		t.Fatalf("last transaction id must be 3")
	}

	err = s.SaveMeta(12, "", "", "", "", "")
	if err != nil {
		t.Fatalf("unable to save meta: %s", err)
	}
	lastMetaID, err := s.LastMetaID()
	if err != nil {
		t.Fatalf("error fetching lastMetaId: %s", err)
	}
	if lastMetaID != 12 {
		t.Fatalf("last meta id must be 12")
	}
}
