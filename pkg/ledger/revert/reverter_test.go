package revert

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type storeFn func(ctx context.Context, id string) (*core.ExpandedTransaction, error)

func (fn storeFn) GetTransaction(ctx context.Context, id string) (*core.ExpandedTransaction, error) {
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

type logIngesterFn func(log *core.LogHolder)

func (l logIngesterFn) QueueLog(log *core.LogHolder) {
	l(log)
}

var _ LogIngester = (logIngesterFn)(nil)

func TestReverter(t *testing.T) {

	txID := uuid.NewString()
	store := storeFn(func(ctx context.Context, id string) (*core.ExpandedTransaction, error) {
		require.Equal(t, txID, id)
		return &core.ExpandedTransaction{}, nil
	})
	runner := runnerFn(func(ctx context.Context, script core.RunScript, dryRun bool, logComputer runner.LogComputer) (*core.ExpandedTransaction, *core.LogHolder, error) {
		return &core.ExpandedTransaction{}, core.NewLogHolder(nil), nil
	})
	reverter := NewReverter(store, runner, logIngesterFn(func(log *core.LogHolder) {
		close(log.Ingested)
	}))
	_, err := reverter.RevertTransaction(context.Background(), txID)
	require.NoError(t, err)

}

func TestReverterWithAlreadyReverted(t *testing.T) {

	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(core.NewTransaction().WithMetadata(
		core.RevertedMetadata(uuid.NewString()),
	))
	store := storeFn(func(ctx context.Context, id string) (*core.ExpandedTransaction, error) {
		require.Equal(t, tx.ID, id)

		return &tx, nil
	})
	runner := runnerFn(func(ctx context.Context, script core.RunScript, dryRun bool, logComputer runner.LogComputer) (*core.ExpandedTransaction, *core.LogHolder, error) {
		return &core.ExpandedTransaction{}, core.NewLogHolder(nil), nil
	})
	reverter := NewReverter(store, runner, logIngesterFn(func(log *core.LogHolder) {
		close(log.Ingested)
	}))
	_, err := reverter.RevertTransaction(context.Background(), tx.ID)
	require.True(t, errors.Is(err, ErrAlreadyReverted))
}

func TestReverterWithRevertOccurring(t *testing.T) {

	tx := core.ExpandTransactionFromEmptyPreCommitVolumes(core.NewTransaction())
	store := storeFn(func(ctx context.Context, id string) (*core.ExpandedTransaction, error) {
		require.Equal(t, tx.ID, id)

		return &tx, nil
	})
	runner := runnerFn(func(ctx context.Context, script core.RunScript, dryRun bool, logComputer runner.LogComputer) (*core.ExpandedTransaction, *core.LogHolder, error) {
		return &core.ExpandedTransaction{}, core.NewLogHolder(nil), nil
	})
	ingestedLog := make(chan *core.LogHolder, 1)
	reverter := NewReverter(store, runner, logIngesterFn(func(log *core.LogHolder) {
		ingestedLog <- log
	}))
	go func() {
		_, err := reverter.RevertTransaction(context.Background(), tx.ID)
		require.NoError(t, err)
	}()

	<-ingestedLog

	_, err := reverter.RevertTransaction(context.Background(), tx.ID)
	require.True(t, errors.Is(err, ErrRevertOccurring))
}
