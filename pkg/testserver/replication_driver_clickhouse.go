package testserver

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	clickhousedriver "github.com/formancehq/ledger/internal/replication/drivers/clickhouse"
	"github.com/pkg/errors"
	"sync"
	"testing"
)

type ClickhouseDriver struct {
	client driver.Conn
	dsn    string
	mu     sync.Mutex
	logger logging.Logger
}

func (h *ClickhouseDriver) initClient() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.client == nil {
		var err error
		h.client, err = clickhousedriver.OpenDB(h.logger, h.dsn, testing.Verbose())
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *ClickhouseDriver) Clear(ctx context.Context) error {
	if err := h.initClient(); err != nil {
		return err
	}
	return h.client.Exec(ctx, "delete from logs where true")
}

func (h *ClickhouseDriver) ReadMessages(ctx context.Context) ([]drivers.LogWithLedger, error) {
	if err := h.initClient(); err != nil {
		return nil, err
	}

	rows, err := h.client.Query(ctx, "select ledger, id, type, date, toJSONString(data) from logs final")
	if err != nil {
		return nil, err
	}

	ret := make([]drivers.LogWithLedger, 0)
	for rows.Next() {
		var (
			payload string
			id      int64
		)
		newLog := drivers.LogWithLedger{}
		if err := rows.Scan(&newLog.Ledger, &id, &newLog.Type, &newLog.Date, &payload); err != nil {
			return nil, errors.Wrap(err, "scanning data from database")
		}
		newLog.ID = pointer.For(uint64(id))

		newLog.Data, err = ledger.HydrateLog(newLog.Type, []byte(payload))
		if err != nil {
			return nil, errors.Wrap(err, "hydrating log data")
		}

		ret = append(ret, newLog)
	}

	return ret, nil
}

func (h *ClickhouseDriver) Config() map[string]any {
	return map[string]any{
		"dsn": h.dsn,
	}
}

func (h *ClickhouseDriver) Name() string {
	return "clickhouse"
}

var _ Driver = &ClickhouseDriver{}

func NewClickhouseDriver(logger logging.Logger, dsn string) Driver {
	return &ClickhouseDriver{
		dsn:    dsn,
		logger: logger,
	}
}
