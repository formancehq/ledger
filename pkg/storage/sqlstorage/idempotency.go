package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/formancehq/ledger/pkg/api/idempotency"
	"github.com/huandu/go-sqlbuilder"
	"github.com/pkg/errors"
)

func (s *Store) CreateIK(ctx context.Context, key string, response idempotency.Response) error {
	data, err := json.Marshal(response.Header)
	if err != nil {
		return err
	}

	ib := sqlbuilder.NewInsertBuilder()
	q, args := ib.
		InsertInto(s.schema.Table("idempotency")).
		Cols("key", "date", "status_code", "headers", "body", "request_hash").
		Values(key, time.Now().UTC().Format(time.RFC3339), response.StatusCode, string(data), response.Body, response.RequestHash).
		BuildWithFlavor(s.schema.Flavor())

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	_, err = executor.ExecContext(ctx, q, args...)
	return errors.Wrap(err, "creating IK")
}

func (s *Store) ReadIK(ctx context.Context, key string) (*idempotency.Response, error) {
	sb := sqlbuilder.NewSelectBuilder()
	q, args := sb.
		Select("status_code", "headers", "body", "request_hash").
		From(s.schema.Table("idempotency")).
		Where(sb.Equal("key", key)).
		BuildWithFlavor(s.schema.Flavor())

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	row := executor.QueryRowContext(ctx, q, args...)
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	response := &idempotency.Response{}
	headersStringValue := ""
	if err := row.Scan(&response.StatusCode, &headersStringValue, &response.Body, &response.RequestHash); err != nil {
		if err == sql.ErrNoRows {
			return nil, idempotency.ErrIKNotFound
		}
		return nil, s.error(err)
	}

	headers := http.Header{}
	if err := json.Unmarshal([]byte(headersStringValue), &headers); err != nil {
		return nil, s.error(err)
	}
	response.Header = headers

	return response, nil
}
