package storage

import "github.com/numary/ledger/core"

type cachedStateStorage struct {
	Store
	lastTransaction *core.Transaction
	lastMetaId      *int64
}

func (s *cachedStateStorage) LastTransaction() (*core.Transaction, error) {
	if s.lastTransaction != nil {
		return s.lastTransaction, nil
	}
	var err error
	s.lastTransaction, err = s.Store.LastTransaction()
	if err != nil {
		return nil, err
	}
	return s.lastTransaction, nil
}

func (s *cachedStateStorage) LastMetaID() (int64, error) {
	if s.lastMetaId != nil {
		return *s.lastMetaId, nil
	}
	lastMetaID, err := s.Store.LastMetaID()
	if err != nil {
		return 0, err
	}
	*s.lastMetaId = lastMetaID
	return lastMetaID, nil
}

func (s *cachedStateStorage) SaveTransactions(txs []core.Transaction) error {
	err := s.Store.SaveTransactions(txs)
	if err != nil {
		return err
	}
	if len(txs) > 0 {
		s.lastTransaction = &txs[len(txs)-1]
	}
	return nil
}

func (s *cachedStateStorage) SaveMeta(id int64, timestamp, targetType, targetID, key, value string) error {
	err := s.Store.SaveMeta(id, timestamp, targetType, targetID, key, value)
	if err != nil {
		return err
	}
	s.lastMetaId = &id
	return nil
}

func NewCachedStateStorage(underlying Store) *cachedStateStorage {
	return &cachedStateStorage{
		Store: underlying,
	}
}
