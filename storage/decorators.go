package storage

import "github.com/numary/ledger/core"

type cachedStateStorage struct {
	Store
	cache *core.State
}

func (s *cachedStateStorage) Cached() *core.State {
	return s.cache
}

func (s *cachedStateStorage) loadState() (*core.State, error) {
	if s.cache != nil {
		return s.cache, nil
	}
	state, err := s.Store.LoadState()
	if err != nil {
		return nil, err
	}
	s.cache = state
	return state, nil
}

func (s *cachedStateStorage) LoadState() (*core.State, error) {
	state, err := s.loadState()
	if err != nil {
		return nil, err
	}
	cp := *state // State can be modified by the called, we have to provide a copy
	return &cp, nil
}

func (s *cachedStateStorage) SaveTransactions(txs []core.Transaction) error {
	state, err := s.loadState()
	if err != nil {
		return err
	}
	err = s.Store.SaveTransactions(txs)
	if err != nil {
		return err
	}
	if len(txs) > 0 {
		state.LastTransaction = &txs[len(txs)-1]
	}
	return nil
}

func (s *cachedStateStorage) SaveMeta(id int64, timestamp, targetType, targetID, key, value string) error {
	state, err := s.loadState()
	if err != nil {
		return err
	}
	err = s.Store.SaveMeta(id, timestamp, targetType, targetID, key, value)
	if err != nil {
		return err
	}
	state.LastMetaID = id
	return nil
}

func NewCachedStateStorage(underlying Store) *cachedStateStorage {
	return &cachedStateStorage{
		Store: underlying,
	}
}
