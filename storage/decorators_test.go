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
func (noOpStorage) LoadState() (*core.State, error) {
	return &core.State{}, nil
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
	if s.Cached().LastTransaction == nil {
		t.Fatalf("cached version should not be nil")
	}
	if s.Cached().LastTransaction.ID != 3 {
		t.Fatalf("last transaction id must be 3")
	}

	err = s.SaveMeta(12, "", "", "", "", "")
	if err != nil {
		t.Fatalf("unable to save meta: %s", err)
	}
	if s.Cached().LastMetaID != 12 {
		t.Fatalf("last meta id must be 12")
	}
}
