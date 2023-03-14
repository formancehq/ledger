package ledger

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/pkg/errors"
)

type appendLog func(context.Context, ...core.Log) <-chan error
type postProcessing func(context.Context) error

type Logs struct {
	appendLog       appendLog
	logs            []core.Log
	postProcessings []postProcessing

	errChan <-chan error
}

func NewLogs(appendLog appendLog, logs []core.Log, postProcessings []postProcessing) *Logs {
	return &Logs{
		appendLog:       appendLog,
		logs:            logs,
		postProcessings: postProcessings,
	}
}

func (ls *Logs) AddLog(log core.Log) {
	ls.logs = append(ls.logs, log)
}

func (ls *Logs) AddPostProcessing(postProcessing postProcessing) {
	ls.postProcessings = append(ls.postProcessings, postProcessing)
}

func (ls *Logs) Write(ctx context.Context) error {
	if len(ls.logs) == 0 {
		// Nothing to do, return early
		return nil
	}

	ls.errChan = ls.appendLog(ctx, ls.logs...)

	return nil
}

func (ls *Logs) Wait(ctx context.Context) error {
	if ls.errChan == nil {
		// Nothing to wait on
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ls.errChan:
		if err != nil {
			return errors.Wrap(err, "appending logs")
		}
	}

	for _, postProcessing := range ls.postProcessings {
		if err := postProcessing(ctx); err != nil {
			return errors.Wrap(err, "post processing")
		}
	}

	return nil
}
