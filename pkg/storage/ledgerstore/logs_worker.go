package ledgerstore

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
)

type AppendedLog struct {
	ActiveLog    *core.ActiveLog
	PersistedLog *core.PersistedLog
}

type pendingLog struct {
	*core.LogPersistenceTracker
	log *core.ActiveLog
}

func (s *Store) AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "append_log")
	defer recordMetrics()

	ret := core.NewLogPersistenceTracker(log)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s.writeChannel <- pendingLog{
		LogPersistenceTracker: ret,
		log:                   log,
	}:
		return ret, nil
	}
}

func (s *Store) processPendingLogs(ctx context.Context, pendingLogs []pendingLog) {
	models := make([]*core.ActiveLog, 0)
	for _, holder := range pendingLogs {
		models = append(models, holder.log)
	}
	appendedLogs, err := s.InsertLogs(ctx, models)
	if err != nil {
		panic(err)
	}
	for i, holder := range pendingLogs {
		holder.Resolve(appendedLogs[i].PersistedLog)
	}
	for _, f := range s.onLogsWrote {
		f(appendedLogs)
	}
}

func (s *Store) Run(ctx context.Context) {
	writeLoopStopped := make(chan struct{})
	effectiveSendChannel := make(chan []pendingLog)
	go func() {
		defer close(writeLoopStopped)
		for {
			select {
			case <-s.stopped:
				return
			case pendingLogs := <-effectiveSendChannel:
				s.processPendingLogs(ctx, pendingLogs)
			}
		}
	}()

	var (
		sendChannel         chan []pendingLog
		bufferedPendingLogs = make([]pendingLog, 0)
	)
	for {
		select {
		case ch := <-s.stopChan:
			close(s.stopped)
			<-writeLoopStopped
			if len(bufferedPendingLogs) > 0 {
				s.processPendingLogs(ctx, bufferedPendingLogs)
			}
			close(ch)

			return
		case mh := <-s.writeChannel:
			bufferedPendingLogs = append(bufferedPendingLogs, mh)
			sendChannel = effectiveSendChannel
		case sendChannel <- bufferedPendingLogs:
			bufferedPendingLogs = make([]pendingLog, 0)
			sendChannel = nil
		}
	}
}

func (s *Store) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.stopChan <- ch:
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
