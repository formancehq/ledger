package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) buildTransactionsQuery(p map[string]interface{}) (*sqlbuilder.SelectBuilder, TxsPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := TxsPaginationToken{}

	sb.Select("id", "timestamp", "reference", "metadata", "postings")
	sb.From(s.schema.Table("transactions"))
	if account, ok := p["account"]; ok && account.(string) != "" {
		arg := sb.Args.Add(account.(string))
		sb.Where(s.schema.Table("use_account") + "(postings, " + arg + ")")
		t.AccountFilter = account.(string)
	}
	if source, ok := p["source"]; ok && source.(string) != "" {
		arg := sb.Args.Add(source.(string))
		sb.Where(s.schema.Table("use_account_as_source") + "(postings, " + arg + ")")
		t.SourceFilter = source.(string)
	}
	if destination, ok := p["destination"]; ok && destination.(string) != "" {
		arg := sb.Args.Add(destination.(string))
		sb.Where(s.schema.Table("use_account_as_destination") + "(postings, " + arg + ")")
		t.DestinationFilter = destination.(string)
	}
	if reference, ok := p["reference"]; ok && reference.(string) != "" {
		sb.Where(sb.E("reference", reference.(string)))
		t.ReferenceFilter = reference.(string)
	}
	if startTime, ok := p["start_time"]; ok && !startTime.(time.Time).IsZero() {
		sb.Where(sb.GE("timestamp", startTime.(time.Time).UTC().Format(time.RFC3339)))
		t.StartTime = startTime.(time.Time)
	}
	if endTime, ok := p["end_time"]; ok && !endTime.(time.Time).IsZero() {
		sb.Where(sb.L("timestamp", endTime.(time.Time).UTC().Format(time.RFC3339)))
		t.EndTime = endTime.(time.Time)
	}

	return sb, t
}

func (s *Store) getTransactions(ctx context.Context, exec executor, q query.Transactions) (sharedapi.Cursor, error) {
	txs := make([]core.Transaction, 0)

	if q.Limit == 0 {
		return sharedapi.Cursor{Data: txs}, nil
	}

	sb, t := s.buildTransactionsQuery(q.Params)
	sb.OrderBy("id desc")
	if q.AfterTxID > 0 {
		sb.Where(sb.L("id", q.AfterTxID))
	}

	// We fetch an additional transaction to know if there are more
	sb.Limit(int(q.Limit + 1))

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return sharedapi.Cursor{}, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	for rows.Next() {
		var (
			ref sql.NullString
			ts  sql.NullString
		)

		tx := core.Transaction{}
		if err := rows.Scan(
			&tx.ID,
			&ts,
			&ref,
			&tx.Metadata,
			&tx.Postings,
		); err != nil {
			return sharedapi.Cursor{}, err
		}
		tx.Reference = ref.String
		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		timestamp, err := time.Parse(time.RFC3339, ts.String)
		if err != nil {
			return sharedapi.Cursor{}, err
		}
		tx.Timestamp = timestamp.UTC().Format(time.RFC3339)
		txs = append(txs, tx)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor{}, s.error(err)
	}

	var previous, next string
	if q.AfterTxID > 0 && len(txs) > 0 {
		t.AfterTxID = txs[0].ID + query.DefaultLimit + 1
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(txs) == int(q.Limit+1) {
		txs = txs[:len(txs)-1]
		t.AfterTxID = txs[len(txs)-1].ID
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	return sharedapi.Cursor{
		PageSize: len(txs),
		Previous: previous,
		Next:     next,
		Data:     txs,
	}, nil
}

func (s *Store) GetTransactions(ctx context.Context, q query.Transactions) (sharedapi.Cursor, error) {
	return s.getTransactions(ctx, s.schema, q)
}

func (s *Store) getTransaction(ctx context.Context, exec executor, txid uint64) (core.Transaction, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("id", "timestamp", "reference", "metadata", "postings")
	sb.From(s.schema.Table("transactions"))
	sb.Where(sb.Equal("id", txid))
	sb.OrderBy("id desc")

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)
	if row.Err() != nil {
		return core.Transaction{}, s.error(row.Err())
	}

	var (
		ref sql.NullString
		ts  sql.NullString
		tx  core.Transaction
	)

	err := row.Scan(
		&tx.ID,
		&ts,
		&ref,
		&tx.Metadata,
		&tx.Postings,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return core.Transaction{}, nil
		}
		return core.Transaction{}, err
	}

	if tx.Metadata == nil {
		tx.Metadata = core.Metadata{}
	}
	t, err := time.Parse(time.RFC3339, ts.String)
	if err != nil {
		return core.Transaction{}, err
	}
	tx.Timestamp = t.UTC().Format(time.RFC3339)
	tx.Reference = ref.String

	return tx, nil
}

func (s *Store) GetTransaction(ctx context.Context, txId uint64) (tx core.Transaction, err error) {
	return s.getTransaction(ctx, s.schema, txId)
}

func (s *Store) getLastTransaction(ctx context.Context, exec executor) (*core.Transaction, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("id", "timestamp", "reference", "metadata", "postings")
	sb.From(s.schema.Table("transactions"))
	sb.OrderBy("id desc")
	sb.Limit(1)

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	var (
		ref sql.NullString
		ts  sql.NullString
		tx  core.Transaction
	)

	err := row.Scan(
		&tx.ID,
		&ts,
		&ref,
		&tx.Metadata,
		&tx.Postings,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if tx.Metadata == nil {
		tx.Metadata = core.Metadata{}
	}
	t, err := time.Parse(time.RFC3339, ts.String)
	if err != nil {
		return nil, err
	}
	tx.Timestamp = t.UTC().Format(time.RFC3339)
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) GetLastTransaction(ctx context.Context) (*core.Transaction, error) {
	return s.getLastTransaction(ctx, s.schema)
}
