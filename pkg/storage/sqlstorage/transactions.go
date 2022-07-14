package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func (s *Store) buildTransactionsQuery(p storage.TransactionsQuery) (*sqlbuilder.SelectBuilder, TxsPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := TxsPaginationToken{}

	var (
		destination = p.Filters.Destination
		source      = p.Filters.Source
		account     = p.Filters.Account
		reference   = p.Filters.Reference
		startTime   = p.Filters.StartTime
		endTime     = p.Filters.EndTime
		metadata    = p.Filters.Metadata
	)

	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes")
	sb.From(s.schema.Table("transactions"))
	if account != "" {
		arg := sb.Args.Add(account)
		sb.Where(s.schema.Table("use_account") + "(postings, " + arg + ")")
		t.AccountFilter = account
	}
	if source != "" {
		arg := sb.Args.Add(source)
		sb.Where(s.schema.Table("use_account_as_source") + "(postings, " + arg + ")")
		t.SourceFilter = source
	}
	if destination != "" {
		arg := sb.Args.Add(destination)
		sb.Where(s.schema.Table("use_account_as_destination") + "(postings, " + arg + ")")
		t.DestinationFilter = destination
	}
	if reference != "" {
		sb.Where(sb.E("reference", reference))
		t.ReferenceFilter = reference
	}
	if !startTime.IsZero() {
		sb.Where(sb.GE("timestamp", startTime.UTC().Format(time.RFC3339)))
		t.StartTime = startTime
	}
	if !endTime.IsZero() {
		sb.Where(sb.L("timestamp", endTime.UTC().Format(time.RFC3339)))
		t.EndTime = endTime
	}

	for key, value := range metadata {
		arg := sb.Args.Add(value)
		sb.Where(s.schema.Table(
			fmt.Sprintf("%s(metadata, %s, '%s')",
				SQLCustomFuncMetaCompare, arg, strings.ReplaceAll(key, ".", "', '")),
		))
	}
	t.MetadataFilter = metadata

	return sb, t
}

func (s *Store) getTransactions(ctx context.Context, exec executor, q storage.TransactionsQuery) (sharedapi.Cursor[core.Transaction], error) {
	txs := make([]core.Transaction, 0)

	if q.PageSize == 0 {
		return sharedapi.Cursor[core.Transaction]{Data: txs}, nil
	}

	sb, t := s.buildTransactionsQuery(q)
	sb.OrderBy("id").Desc()
	if q.AfterTxID > 0 {
		sb.Where(sb.LE("id", q.AfterTxID))
	}

	// We fetch additional transactions to know if there are more before and/or after.
	sb.Limit(int(q.PageSize + 2))
	t.PageSize = q.PageSize

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return sharedapi.Cursor[core.Transaction]{}, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	for rows.Next() {
		var ref, ts sql.NullString
		tx := core.Transaction{}
		if err := rows.Scan(
			&tx.ID,
			&ts,
			&ref,
			&tx.Metadata,
			&tx.Postings,
			&tx.PreCommitVolumes,
			&tx.PostCommitVolumes,
		); err != nil {
			return sharedapi.Cursor[core.Transaction]{}, err
		}
		tx.Reference = ref.String
		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		timestamp, err := time.Parse(time.RFC3339, ts.String)
		if err != nil {
			return sharedapi.Cursor[core.Transaction]{}, err
		}
		tx.Timestamp = timestamp.UTC().Format(time.RFC3339)
		txs = append(txs, tx)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor[core.Transaction]{}, s.error(err)
	}

	var previous, next string

	// Page with transactions before
	if q.AfterTxID > 0 && len(txs) > 1 && txs[0].ID == q.AfterTxID {
		t.AfterTxID = txs[0].ID + uint64(q.PageSize)
		txs = txs[1:]
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.Transaction]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	// Page with transactions after
	if len(txs) > int(q.PageSize) {
		txs = txs[:q.PageSize]
		t.AfterTxID = txs[len(txs)-1].ID
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.Transaction]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	return sharedapi.Cursor[core.Transaction]{
		PageSize: int(q.PageSize),
		HasMore:  next != "",
		Previous: previous,
		Next:     next,
		Data:     txs,
	}, nil
}

func (s *Store) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (sharedapi.Cursor[core.Transaction], error) {
	return s.getTransactions(ctx, s.schema, q)
}

func (s *Store) getTransaction(ctx context.Context, exec executor, txid uint64) (*core.Transaction, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes")
	sb.From(s.schema.Table("transactions"))
	sb.Where(sb.Equal("id", txid))
	sb.OrderBy("id desc")

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	tx := core.Transaction{
		TransactionData: core.TransactionData{
			Postings: core.Postings{},
			Metadata: core.Metadata{},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref, ts sql.NullString
	if err := row.Scan(
		&tx.ID,
		&ts,
		&ref,
		&tx.Metadata,
		&tx.Postings,
		&tx.PreCommitVolumes,
		&tx.PostCommitVolumes,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	t, err := time.Parse(time.RFC3339, ts.String)
	if err != nil {
		return nil, err
	}
	tx.Timestamp = t.UTC().Format(time.RFC3339)
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) GetTransaction(ctx context.Context, txId uint64) (*core.Transaction, error) {
	return s.getTransaction(ctx, s.schema, txId)
}

func (s *Store) getLastTransaction(ctx context.Context, exec executor) (*core.Transaction, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes")
	sb.From(s.schema.Table("transactions"))
	sb.OrderBy("id desc")
	sb.Limit(1)

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	tx := core.Transaction{
		TransactionData: core.TransactionData{
			Postings: core.Postings{},
			Metadata: core.Metadata{},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref, ts sql.NullString
	if err := row.Scan(
		&tx.ID,
		&ts,
		&ref,
		&tx.Metadata,
		&tx.Postings,
		&tx.PreCommitVolumes,
		&tx.PostCommitVolumes,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
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

func (s *Store) insertTransactions(ctx context.Context, exec executor, txs ...core.Transaction) error {
	var (
		query string
		args  []interface{}
	)

	switch s.Schema().Flavor() {
	case sqlbuilder.SQLite:
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.schema.Table("transactions"))
		ib.Cols("id", "timestamp", "reference", "postings", "metadata", "pre_commit_volumes", "post_commit_volumes")
		for _, tx := range txs {
			postingsData, err := json.Marshal(tx.Postings)
			if err != nil {
				panic(err)
			}

			metadataData := []byte("{}")
			if tx.Metadata != nil {
				metadataData, err = json.Marshal(tx.Metadata)
				if err != nil {
					panic(err)
				}
			}

			preCommitVolumesData, err := json.Marshal(tx.PreCommitVolumes)
			if err != nil {
				panic(err)
			}

			postCommitVolumesData, err := json.Marshal(tx.PostCommitVolumes)
			if err != nil {
				panic(err)
			}

			var reference *string
			if tx.Reference != "" {
				cp := tx.Reference
				reference = &cp
			}

			ib.Values(tx.ID, tx.Timestamp, reference, postingsData,
				metadataData, preCommitVolumesData, postCommitVolumesData)
		}
		query, args = ib.BuildWithFlavor(s.schema.Flavor())
	case sqlbuilder.PostgreSQL:
		ids := make([]uint64, len(txs))
		timestamps := make([]string, len(txs))
		references := make([]*string, len(txs))
		postingDataSet := make([]string, len(txs))
		metadataDataSet := make([]string, len(txs))
		preCommitVolumesDataSet := make([]string, len(txs))
		postCommitVolumesDataSet := make([]string, len(txs))

		for i, tx := range txs {
			postingsData, err := json.Marshal(tx.Postings)
			if err != nil {
				panic(err)
			}

			metadataData := []byte("{}")
			if tx.Metadata != nil {
				metadataData, err = json.Marshal(tx.Metadata)
				if err != nil {
					panic(err)
				}
			}

			preCommitVolumesData, err := json.Marshal(tx.PreCommitVolumes)
			if err != nil {
				panic(err)
			}

			postCommitVolumesData, err := json.Marshal(tx.PostCommitVolumes)
			if err != nil {
				panic(err)
			}

			ids[i] = tx.ID
			timestamps[i] = tx.Timestamp
			postingDataSet[i] = string(postingsData)
			metadataDataSet[i] = string(metadataData)
			preCommitVolumesDataSet[i] = string(preCommitVolumesData)
			postCommitVolumesDataSet[i] = string(postCommitVolumesData)
			references[i] = nil
			if tx.Reference != "" {
				cp := tx.Reference
				references[i] = &cp
			}
		}

		query = fmt.Sprintf(
			`INSERT INTO "%s".transactions (id, timestamp, reference, postings, metadata, pre_commit_volumes, post_commit_volumes) (SELECT * FROM unnest($1::int[], $2::varchar[], $3::varchar[], $4::jsonb[], $5::jsonb[], $6::jsonb[], $7::jsonb[]))`,
			s.schema.Name())
		args = []interface{}{
			ids, timestamps, references, postingDataSet,
			metadataDataSet, preCommitVolumesDataSet, postCommitVolumesDataSet,
		}
	}

	sharedlogging.GetLogger(ctx).Debugf("ExecContext: %s %s", query, args)

	_, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return s.error(err)
	}
	return nil
}

func (s *Store) updateTransactionMetadata(ctx context.Context, exec executor, id uint64, metadata core.Metadata) error {

	ub := sqlbuilder.NewUpdateBuilder()

	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	ub.
		Update(s.schema.Table("transactions")).
		Where(ub.E("id", id))

	switch Flavor(s.schema.Flavor()) {
	case PostgreSQL:
		ub.Set(fmt.Sprintf("metadata = metadata || '%s'::jsonb", string(metadataData)))
	case SQLite:
		ub.Set(fmt.Sprintf("metadata = json_patch(metadata, '%s')", string(metadataData)))
	}

	sqlq, args := ub.BuildWithFlavor(s.schema.Flavor())
	_, err = exec.ExecContext(ctx, sqlq, args...)

	return s.error(err)
}

func (s *Store) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata, at time.Time) error {
	tx, err := s.schema.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err = s.updateTransactionMetadata(ctx, tx, id, metadata); err != nil {
		return errors.Wrap(err, "updating metadata")
	}

	lastLog, err := s.lastLog(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	err = s.appendLog(ctx, tx, core.NewSetMetadataLog(lastLog, at, core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   id,
		Metadata:   metadata,
	}))
	if err != nil {
		return errors.Wrap(err, "appending log")
	}

	if err = tx.Commit(); err != nil {
		return s.error(err)
	}

	return nil
}
