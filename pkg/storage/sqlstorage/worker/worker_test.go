package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/worker"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSimpleWorker(t *testing.T) {
	ctx := context.Background()
	db := NewMockDB()

	w := worker.NewWorker(db.Write, worker.DefaultConfig)
	go w.Run(ctx)
	defer func() {
		require.NoError(t, w.Stop(context.Background()))
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	eg := errgroup.Group{}
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			log := core.NewTransactionLog(
				core.NewTransaction(),
				map[string]metadata.Metadata{},
			)
			_, errChan := w.WriteModel(ctx, log)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-errChan:
				return err
			}
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}

	require.Len(t, db.logs, 100)
}

type MockDB struct {
	logs []*core.PersistedLog
}

func NewMockDB() *MockDB {
	return &MockDB{
		logs: []*core.PersistedLog{},
	}
}

func (m *MockDB) Write(ctx context.Context, logs []*core.Log) ([]*core.PersistedLog, error) {
	ret := make([]*core.PersistedLog, 0)
	var previous *core.PersistedLog
	if len(m.logs) > 0 {
		previous = m.logs[len(m.logs)-1]
	}
	for _, log := range logs {
		newLog := log.ComputePersistentLog(previous)
		ret = append(ret, newLog)
		m.logs = append(m.logs, newLog)
		previous = newLog
	}

	return ret, nil
}
