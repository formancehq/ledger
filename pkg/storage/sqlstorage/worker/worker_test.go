package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/storage/sqlstorage/worker"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSimpleWorker(t *testing.T) {
	ctx := context.Background()
	db := NewMockDB()

	w := worker.NewWorker(db.Write)
	go w.Run(ctx)
	defer func() {
		require.NoError(t, w.Stop(context.Background()))
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	eg := errgroup.Group{}
	for i := 0; i < 100; i++ {
		i := i
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-w.WriteModels(ctx, &Log{id: i}):
				return err
			}
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}

	require.Len(t, db.ids, 100)
}

func TestBatchWorker(t *testing.T) {
	ctx := context.Background()
	db := NewMockDB()

	w := worker.NewWorker(db.Write)
	go w.Run(ctx)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	eg := errgroup.Group{}
	for i := 0; i < 1000; i += 100 {
		logs := make([]*Log, 0, 100)
		for j := i; j < i+100 && j < 1000; j++ {
			logs = append(logs, &Log{id: j})
		}
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-w.WriteModels(ctx, logs...):
				return err
			}
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}

	require.Len(t, db.ids, 1000)
}

type Log struct {
	id int
}

type MockDB struct {
	ids map[int]struct{}
}

func NewMockDB() *MockDB {
	return &MockDB{
		ids: make(map[int]struct{}),
	}
}

func (m *MockDB) Write(ctx context.Context, logs []*Log) error {
	for _, log := range logs {
		m.ids[log.id] = struct{}{}
	}
	return nil
}
