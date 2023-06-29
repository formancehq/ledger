package ledgerstore

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

type pendingLog struct {
	*core.LogPersistenceTracker
	log *core.ActiveLog
}

func (s *Store) AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error) {
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
	err := s.InsertLogs(ctx, models)
	if err != nil {
		panic(err)
	}
	for _, holder := range pendingLogs {
		holder.Resolve()
	}
	for _, f := range s.onLogsWrote {
		f(collectionutils.Map(pendingLogs, func(from pendingLog) *core.ActiveLog {
			return from.log
		}))
	}
}

func (s *Store) Run(ctx context.Context) {
	writeLoopStopped := make(chan struct{})
	effectiveSendChannel := make(chan []pendingLog)
	stopped := make(chan struct{})
	go func() {
		defer close(writeLoopStopped)
		for {
			select {
			case <-stopped:
				return
			case pendingLogs := <-effectiveSendChannel:
				s.processPendingLogs(ctx, pendingLogs)
			}
		}
	}()

	var (
		sendChannel         chan []pendingLog
		bufferedPendingLogs = make([]pendingLog, 0)
		logger              = logging.FromContext(ctx)
	)
	for {
		select {
		case ch := <-s.stopChan:
			logger.Debugf("Terminating store worker, waiting end of write loop")
			close(stopped)
			<-writeLoopStopped
			logger.Debugf("Write loop terminated, store properly closed")
			//if len(bufferedPendingLogs) > 0 {
			//	s.processPendingLogs(ctx, bufferedPendingLogs)
			//}
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
	logging.FromContext(ctx).Info("Close store")
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		logging.FromContext(ctx).Errorf("Unable to close store: %s", ctx.Err())
		return ctx.Err()
	case s.stopChan <- ch:
		logging.FromContext(ctx).Debugf("Signal sent, waiting response")
		select {
		case <-ch:
			logging.FromContext(ctx).Info("Store closed")
			return nil
		case <-ctx.Done():
			logging.FromContext(ctx).Errorf("Unable to close store: %s", ctx.Err())
			return ctx.Err()
		}
	}
}
