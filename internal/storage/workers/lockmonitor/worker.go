package lockmonitor

import (
	"context"
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"time"
)

type Config struct {
	Interval time.Duration
}

type Worker struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	db          bun.IDB
	cfg         Config
	tracer      trace.Tracer
	monitors    []Monitor
}

func (r *Worker) Name() string {
	return "Lock monitor"
}

func (r *Worker) Run(ctx context.Context) error {

	interval := r.cfg.Interval
	if interval == 0 {
		interval = time.Second
	}

	for {
		select {
		case <-time.After(interval):
			if err := r.Fire(ctx); err != nil {
				r.logger.Errorf("error running block runner: %v", err)
			}
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *Worker) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.stopChannel <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
	return nil
}

func (r *Worker) Fire(ctx context.Context) error {

	ctx, span := r.tracer.Start(ctx, "Run")
	defer span.End()

	ret := make([]lock, 0)
	if err := r.db.
		NewRaw(listLocksQuery).
		Scan(ctx, &ret); err != nil {
		return err
	}

	locks := Map(ret, func(row lock) Lock {
		return Lock{
			lock:                              row,
			BlockedStatement:                  newStatement(row.RawBlockedStatement),
			CurrentStatementInBlockingProcess: newStatement(row.RawCurrentStatementInBlockingProcess),
		}
	})

	for _, monitor := range r.monitors {
		func() {
			ctx, span := r.tracer.Start(ctx, "RunMonitor")
			defer func() {
				if e := recover(); e != nil {
					otlp.RecordAsError(ctx, e)
				}
				span.End()
			}()

			monitor.Accept(ctx, locks)
		}()
	}

	return nil
}

func NewWorker(logger logging.Logger, db bun.IDB, cfg Config, opts ...Option) *Worker {
	ret := &Worker{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		db:          db,
		cfg:         cfg,
	}

	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	return ret
}

type Option func(*Worker)

func WithTracer(tracer trace.Tracer) Option {
	return func(r *Worker) {
		r.tracer = tracer
	}
}

func WithMonitors(monitors ...Monitor) Option {
	return func(r *Worker) {
		r.monitors = append(r.monitors, monitors...)
	}
}

var defaultOptions = []Option{
	WithTracer(noop.Tracer{}),
}

// https://www.postgresscripts.com/post/postgresql-lock-monitoring-identify-and-resolve-blocking-application-locks/
const listLocksQuery = `
SELECT
  blocked_locks.pid                   AS blocked_pid,
  blocked_activity.usename            AS blocked_user,
  blocking_locks.pid                  AS blocking_pid,
  blocking_activity.usename           AS blocking_user,
  blocked_activity.query              AS blocked_statement,
  blocking_activity.query             AS current_statement_in_blocking_process,
  blocked_activity.application_name   AS blocked_application,
  blocking_activity.application_name  AS blocking_application
FROM
  pg_catalog.pg_locks           blocked_locks
JOIN
  pg_catalog.pg_stat_activity   blocked_activity
ON
  blocked_activity.pid = blocked_locks.pid
JOIN
  pg_catalog.pg_locks           blocking_locks
ON
  blocking_locks.locktype = blocked_locks.locktype
  AND blocking_locks.DATABASE       IS NOT DISTINCT FROM blocked_locks.DATABASE
  AND blocking_locks.relation       IS NOT DISTINCT FROM blocked_locks.relation
  AND blocking_locks.page           IS NOT DISTINCT FROM blocked_locks.page
  AND blocking_locks.tuple          IS NOT DISTINCT FROM blocked_locks.tuple
  AND blocking_locks.virtualxid     IS NOT DISTINCT FROM blocked_locks.virtualxid
  AND blocking_locks.transactionid  IS NOT DISTINCT FROM blocked_locks.transactionid
  AND blocking_locks.classid        IS NOT DISTINCT FROM blocked_locks.classid
  AND blocking_locks.objid          IS NOT DISTINCT FROM blocked_locks.objid
  AND blocking_locks.objsubid       IS NOT DISTINCT FROM blocked_locks.objsubid
  AND blocking_locks.pid != blocked_locks.pid
JOIN
  pg_catalog.pg_stat_activity blocking_activity
ON
  blocking_activity.pid = blocking_locks.pid
WHERE
  NOT blocked_locks.granted;
`
