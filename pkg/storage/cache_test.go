package storage

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
)

type noOpStorage struct {
	Store
}

func (noOpStorage) AppendLog(context.Context, ...core.Log) error {
	return nil
}

func TestCacheState(t *testing.T) {
	s := NewCachedStateStorage(noOpStorage{})
	err := s.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{}))
	assert.NoError(t, err)

	lastLog, err := s.LastLog(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, lastLog.Data)
}
