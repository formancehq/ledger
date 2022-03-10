package storage

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

type noOpStorage struct {
	Store
}

func (noOpStorage) AppendLog(context.Context, ...core.Log) (map[int]error, error) {
	return nil, nil
}

func TestCacheState(t *testing.T) {
	s := NewCachedStateStorage(noOpStorage{})
	_, err := s.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{
		ID: uuid.New(),
	}))
	assert.NoError(t, err)

	lastLog, err := s.LastLog(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, lastLog.Data)
}
