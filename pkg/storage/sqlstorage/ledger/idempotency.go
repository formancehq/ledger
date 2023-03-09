package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/formancehq/ledger/pkg/api/idempotency"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const idempotencyTableName = "idempotency"

type Idempotency struct {
	bun.BaseModel `bun:"idempotency,alias:idempotency"`

	Key string `bun:"key,type:varchar,pk,notnull"`

	// TODO(polo/gfyrag): use a proper date type
	Date string `bun:"date,type:varchar"`

	StatusCode  int    `bun:"status_code,type:int"`
	Headers     string `bun:"headers,type:varchar"`
	Body        string `bun:"body,type:varchar"`
	RequestHash string `bun:"request_hash,type:varchar"`
}

func (s *Store) CreateIK(ctx context.Context, key string, response idempotency.Response) error {
	data, err := json.Marshal(response.Header)
	if err != nil {
		return err
	}

	i := &Idempotency{
		Key:         key,
		Date:        time.Now().UTC().Format(time.RFC3339),
		StatusCode:  response.StatusCode,
		Headers:     string(data),
		Body:        response.Body,
		RequestHash: response.RequestHash,
	}

	_, err = s.schema.NewInsert(idempotencyTableName).
		Model(i).
		Exec(ctx)
	return errors.Wrap(err, "creating IK")
}

func (s *Store) ReadIK(ctx context.Context, key string) (*idempotency.Response, error) {
	sb := s.schema.NewSelect(idempotencyTableName).
		Model((*Idempotency)(nil)).
		Column("status_code", "headers", "body", "request_hash").
		Where("key = ?", key)

	row := s.schema.QueryRowContext(ctx, sb.String())
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
