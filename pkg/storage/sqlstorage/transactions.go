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
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pkg/errors"
)

func (s *Store) buildTransactionsQuery(flavor Flavor, p ledger.TransactionsQuery) (*sqlbuilder.SelectBuilder, TxsPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := TxsPaginationToken{}

	var (
		destination   = p.Filters.Destination
		source        = p.Filters.Source
		account       = p.Filters.Account
		reference     = p.Filters.Reference
		startTime     = p.Filters.StartTime
		endTime       = p.Filters.EndTime
		metadata      = p.Filters.Metadata
		regexOperator = "~"
	)
	if flavor == SQLite {
		regexOperator = "REGEXP"
	}

	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes")
	sb.From(s.schema.Table("transactions"))
	if account != "" {
		r := fmt.Sprintf("(^|;)%s($|;)", account)
		arg := sb.Args.Add(r)
		sb.Where(sb.Or(
			fmt.Sprintf("sources %s %s", regexOperator, arg),
			fmt.Sprintf("destinations %s %s", regexOperator, arg),
		))
		t.AccountFilter = account
	}
	if source != "" {
		r := fmt.Sprintf("(^|;)%s($|;)", source)
		arg := sb.Args.Add(r)
		sb.Where(fmt.Sprintf("sources %s %s", regexOperator, arg))
		t.SourceFilter = source
	}
	if destination != "" {
		r := fmt.Sprintf("(^|;)%s($|;)", destination)
		arg := sb.Args.Add(r)
		sb.Where(fmt.Sprintf("destinations %s %s", regexOperator, arg))
		t.DestinationFilter = destination
	}
	if reference != "" {
		sb.Where(sb.E("reference", reference))
		t.ReferenceFilter = reference
	}
	if !startTime.IsZero() {
		sb.Where(sb.GE("timestamp", startTime.UTC()))
		t.StartTime = startTime
	}
	if !endTime.IsZero() {
		sb.Where(sb.L("timestamp", endTime.UTC()))
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

func (s *Store) GetTransactions(ctx context.Context, q ledger.TransactionsQuery) (sharedapi.Cursor[core.ExpandedTransaction], error) {
	txs := make([]core.ExpandedTransaction, 0)

	if q.PageSize == 0 {
		return sharedapi.Cursor[core.ExpandedTransaction]{Data: txs}, nil
	}

	sb, t := s.buildTransactionsQuery(Flavor(s.schema.Flavor()), q)
	sb.OrderBy("id").Desc()
	if q.AfterTxID > 0 {
		sb.Where(sb.LE("id", q.AfterTxID))
	}

	// We fetch additional transactions to know if there are more before and/or after.
	sb.Limit(int(q.PageSize + 2))
	t.PageSize = q.PageSize

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return sharedapi.Cursor[core.ExpandedTransaction]{}, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return sharedapi.Cursor[core.ExpandedTransaction]{}, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	for rows.Next() {
		var ref sql.NullString
		tx := core.ExpandedTransaction{}
		if err := rows.Scan(
			&tx.ID,
			&tx.Timestamp,
			&ref,
			&tx.Metadata,
			&tx.Postings,
			&tx.PreCommitVolumes,
			&tx.PostCommitVolumes,
		); err != nil {
			return sharedapi.Cursor[core.ExpandedTransaction]{}, err
		}
		tx.Reference = ref.String
		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		tx.Timestamp = tx.Timestamp.UTC()
		txs = append(txs, tx)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor[core.ExpandedTransaction]{}, s.error(err)
	}

	var previous, next string

	// Page with transactions before
	if q.AfterTxID > 0 && len(txs) > 1 && txs[0].ID == q.AfterTxID {
		t.AfterTxID = txs[0].ID + uint64(q.PageSize)
		txs = txs[1:]
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.ExpandedTransaction]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	// Page with transactions after
	if len(txs) > int(q.PageSize) {
		txs = txs[:q.PageSize]
		t.AfterTxID = txs[len(txs)-1].ID
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.ExpandedTransaction]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	return sharedapi.Cursor[core.ExpandedTransaction]{
		PageSize: int(q.PageSize),
		HasMore:  next != "",
		Previous: previous,
		Next:     next,
		Data:     txs,
	}, nil
}

func (s *Store) GetTransaction(ctx context.Context, txId uint64) (*core.ExpandedTransaction, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes")
	sb.From(s.schema.Table("transactions"))
	sb.Where(sb.Equal("id", txId))
	sb.OrderBy("id desc")

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := executor.QueryRowContext(ctx, sqlq, args...)
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{},
				Metadata: core.Metadata{},
			},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref sql.NullString
	if err := row.Scan(
		&tx.ID,
		&tx.Timestamp,
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

	tx.Timestamp = tx.Timestamp.UTC()
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) GetLastTransaction(ctx context.Context) (*core.ExpandedTransaction, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes")
	sb.From(s.schema.Table("transactions"))
	sb.OrderBy("id desc")
	sb.Limit(1)

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := executor.QueryRowContext(ctx, sqlq, args...)
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{},
				Metadata: core.Metadata{},
			},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref sql.NullString
	if err := row.Scan(
		&tx.ID,
		&tx.Timestamp,
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

	tx.Timestamp = tx.Timestamp.UTC()
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) insertTransactions(ctx context.Context, txs ...core.ExpandedTransaction) error {
	var (
		query string
		args  []interface{}
	)

	switch s.Schema().Flavor() {
	case sqlbuilder.SQLite:
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.schema.Table("transactions"))
		ib.Cols("id", "timestamp", "reference", "postings", "metadata",
			"pre_commit_volumes", "post_commit_volumes", "sources", "destinations")
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
			sources := ""
			destinations := ""
			for _, p := range tx.Postings {
				sources = fmt.Sprintf("%s;%s", sources, p.Source)
				destinations = fmt.Sprintf("%s;%s", destinations, p.Destination)
			}
			sources = sources[1:]
			destinations = destinations[1:]

			var reference *string
			if tx.Reference != "" {
				cp := tx.Reference
				reference = &cp
			}

			ib.Values(tx.ID, tx.Timestamp, reference, postingsData,
				metadataData, preCommitVolumesData, postCommitVolumesData,
				sources, destinations)
		}
		query, args = ib.BuildWithFlavor(s.schema.Flavor())
	case sqlbuilder.PostgreSQL:
		ids := make([]uint64, len(txs))
		timestamps := make([]time.Time, len(txs))
		references := make([]*string, len(txs))
		postingDataSet := make([]string, len(txs))
		metadataDataSet := make([]string, len(txs))
		preCommitVolumesDataSet := make([]string, len(txs))
		postCommitVolumesDataSet := make([]string, len(txs))
		sources := make([]string, len(txs))
		destinations := make([]string, len(txs))

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

			computedSources := ""
			for _, p := range tx.Postings {
				computedSources = fmt.Sprintf("%s;%s", computedSources, p.Source)
			}
			if len(computedSources) > 0 {
				computedSources = computedSources[1:] // Strip leading ;
			}

			computedDestinations := ""
			for _, p := range tx.Postings {
				computedDestinations = fmt.Sprintf("%s;%s", computedDestinations, p.Destination)
			}
			if len(computedDestinations) > 0 {
				computedDestinations = computedDestinations[1:]
			}

			ids[i] = tx.ID
			timestamps[i] = tx.Timestamp
			postingDataSet[i] = string(postingsData)
			metadataDataSet[i] = string(metadataData)
			preCommitVolumesDataSet[i] = string(preCommitVolumesData)
			postCommitVolumesDataSet[i] = string(postCommitVolumesData)
			references[i] = nil
			sources[i] = computedSources
			destinations[i] = computedDestinations
			if tx.Reference != "" {
				cp := tx.Reference
				references[i] = &cp
			}
		}

		query = fmt.Sprintf(
			`INSERT INTO "%s".transactions (id, timestamp, reference, postings, metadata, pre_commit_volumes, 
                               post_commit_volumes, sources, destinations) (SELECT * FROM unnest(
                                   $1::int[], 
                                   $2::timestamp[], 
                                   $3::varchar[], 
                                   $4::jsonb[], 
                                   $5::jsonb[], 
                                   $6::jsonb[], 
                                   $7::jsonb[],
                                   $8::varchar[],
                                   $9::varchar[]))`,
			s.schema.Name())
		args = []interface{}{
			ids, timestamps, references, postingDataSet,
			metadataDataSet, preCommitVolumesDataSet, postCommitVolumesDataSet,
			sources, destinations,
		}
	}

	sharedlogging.GetLogger(ctx).Debugf("ExecContext: %s %s", query, args)

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	_, err = executor.ExecContext(ctx, query, args...)
	if err != nil {
		return s.error(err)
	}
	return nil
}

func (s *Store) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata, at time.Time) error {
	ub := sqlbuilder.NewUpdateBuilder()

	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	ub.
		Update(s.schema.Table("transactions")).
		Where(ub.E("id", id))

	placeholder := ub.Var(string(metadataData))
	switch Flavor(s.schema.Flavor()) {
	case PostgreSQL:
		ub.Set(fmt.Sprintf("metadata = metadata || %s", placeholder))
	case SQLite:
		ub.Set(fmt.Sprintf("metadata = json_patch(metadata, %s)", placeholder))
	}

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	sqlq, args := ub.BuildWithFlavor(s.schema.Flavor())
	_, err = executor.ExecContext(ctx, sqlq, args...)
	if err != nil {
		return err
	}

	lastLog, err := s.LastLog(ctx)
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	return s.appendLog(ctx, core.NewSetMetadataLog(lastLog, at, core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   id,
		Metadata:   metadata,
	}))
}
