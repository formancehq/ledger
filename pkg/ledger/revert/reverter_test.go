package revert

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type storeFn func(ctx context.Context, id uint64) (*core.ExpandedTransaction, error)

func (fn storeFn) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	return fn(ctx, id)
}

var _ Store = (storeFn)(nil)

type runnerFn func(
	ctx context.Context,
	script core.RunScript,
	dryRun bool,
	logComputer runner.LogComputer,
) (*core.ExpandedTransaction, *core.LogHolder, error)

func (fn runnerFn) Execute(
	ctx context.Context,
	script core.RunScript,
	dryRun bool,
	logComputer runner.LogComputer,
) (*core.ExpandedTransaction, *core.LogHolder, error) {
	return fn(ctx, script, dryRun, logComputer)
}

var _ Runner = (runnerFn)(nil)

type logIngesterFn func(ctx context.Context, log *core.LogHolder, async bool) error

func (l logIngesterFn) QueueLog(ctx context.Context, log *core.LogHolder, async bool) error {
	return l(ctx, log, async)
}

var _ LogIngester = (logIngesterFn)(nil)

func TestReverter(t *testing.T) {

	txID := uint64(0)
	store := storeFn(func(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
		require.Equal(t, txID, id)
		return &core.ExpandedTransaction{}, nil
	})
	runner := runnerFn(func(ctx context.Context, script core.RunScript, dryRun bool, logComputer runner.LogComputer) (*core.ExpandedTransaction, *core.LogHolder, error) {
		return &core.ExpandedTransaction{}, core.NewLogHolder(nil), nil
	})
	reverter := NewReverter(store, runner, logIngesterFn(func(ctx context.Context, log *core.LogHolder, async bool) error {
		close(log.Ingested)
		return nil
	}))
	_, err := reverter.RevertTransaction(context.Background(), txID, false)
	require.NoError(t, err)

}

func TestReverterWithAlreadyReverted(t *testing.T) {

	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(core.NewTransaction().WithMetadata(
		core.RevertedMetadata(uint64(0)),
	))
	store := storeFn(func(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
		require.Equal(t, tx.ID, id)

		return &tx, nil
	})
	runner := runnerFn(func(ctx context.Context, script core.RunScript, dryRun bool, logComputer runner.LogComputer) (*core.ExpandedTransaction, *core.LogHolder, error) {
		return &core.ExpandedTransaction{}, core.NewLogHolder(nil), nil
	})
	reverter := NewReverter(store, runner, logIngesterFn(func(ctx context.Context, log *core.LogHolder, async bool) error {
		close(log.Ingested)
		return nil
	}))
	_, err := reverter.RevertTransaction(context.Background(), tx.ID, false)
	require.True(t, errors.Is(err, ErrAlreadyReverted))
}

func TestReverterWithRevertOccurring(t *testing.T) {

	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(core.NewTransaction())
	store := storeFn(func(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
		require.Equal(t, tx.ID, id)

		return &tx, nil
	})
	runner := runnerFn(func(ctx context.Context, script core.RunScript, dryRun bool, logComputer runner.LogComputer) (*core.ExpandedTransaction, *core.LogHolder, error) {
		return &core.ExpandedTransaction{}, core.NewLogHolder(nil), nil
	})
	ingestedLog := make(chan *core.LogHolder, 1)
	reverter := NewReverter(store, runner, logIngesterFn(func(ctx context.Context, log *core.LogHolder, async bool) error {
		ingestedLog <- log
		return nil
	}))
	go func() {
		_, err := reverter.RevertTransaction(context.Background(), tx.ID, false)
		require.NoError(t, err)
	}()

	<-ingestedLog

	_, err := reverter.RevertTransaction(context.Background(), tx.ID, false)
	require.True(t, errors.Is(err, ErrRevertOccurring))
}
