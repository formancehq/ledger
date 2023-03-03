package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/huandu/go-sqlbuilder"
	"github.com/pkg/errors"
)

// this regexp is used to distinguish between deprecated regex queries for
// source, destination and account params and the new wildcard query
// which allows segmented address pattern matching, e.g; "foo:bar:*"
var addressQueryRegexp = regexp.MustCompile(`^(\w+|\*|\.\*)(:(\w+|\*|\.\*))*$`)

func (s *Store) buildTransactionsQuery(p ledger.TransactionsQuery) (*sqlbuilder.SelectBuilder, TxsPaginationToken) {
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

	sb.Select("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		Distinct()
	sb.From(s.schema.Table("transactions"))
	if source != "" || destination != "" || account != "" {
		// new wildcard handling
		sb.Join(fmt.Sprintf(
			"%s postings on postings.txid = %s.id",
			s.schema.Table("postings"),
			s.schema.Table("transactions"),
		))
	}
	if source != "" {
		if !addressQueryRegexp.MatchString(source) {
			// deprecated regex handling
			arg := sb.Args.Add(source)
			sb.Where(s.schema.Table("use_account_as_source") + "(postings, " + arg + ")")
		} else {
			// new wildcard handling
			src := strings.Split(source, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.source) = %d", len(src)))

			for i, segment := range src {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				arg := sb.Args.Add(segment)
				sb.Where(fmt.Sprintf("postings.source @@ ('$[%d] == \"' || %s::text || '\"')::jsonpath", i, arg))
			}
		}
		t.SourceFilter = source
	}
	if destination != "" {
		if !addressQueryRegexp.MatchString(destination) {
			// deprecated regex handling
			arg := sb.Args.Add(destination)
			sb.Where(s.schema.Table("use_account_as_destination") + "(postings, " + arg + ")")
		} else {
			// new wildcard handling
			dst := strings.Split(destination, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.destination) = %d", len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				arg := sb.Args.Add(segment)
				sb.Where(fmt.Sprintf("postings.destination @@ ('$[%d] == \"' || %s::text || '\"')::jsonpath", i, arg))
			}
		}
		t.DestinationFilter = destination
	}
	if account != "" {
		if !addressQueryRegexp.MatchString(account) {
			// deprecated regex handling
			arg := sb.Args.Add(account)
			sb.Where(s.schema.Table("use_account") + "(postings, " + arg + ")")
		} else {
			// new wildcard handling
			dst := strings.Split(account, ":")
			sb.Where(fmt.Sprintf("(jsonb_array_length(postings.destination) = %d OR jsonb_array_length(postings.source) = %d)", len(dst), len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				arg := sb.Args.Add(segment)
				sb.Where(fmt.Sprintf("(postings.source @@ ('$[%d] == \"' || %s::text || '\"')::jsonpath OR postings.destination @@ ('$[%d] == \"' || %s::text || '\"')::jsonpath)", i, arg, i, arg))
			}
		}
		t.AccountFilter = account
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

func (s *Store) GetTransactions(ctx context.Context, q ledger.TransactionsQuery) (api.Cursor[core.ExpandedTransaction], error) {
	txs := make([]core.ExpandedTransaction, 0)

	if q.PageSize == 0 {
		return api.Cursor[core.ExpandedTransaction]{Data: txs}, nil
	}

	sb, t := s.buildTransactionsQuery(q)
	sb.OrderBy("id").Desc()
	if q.AfterTxID > 0 {
		sb.Where(sb.LE("id", q.AfterTxID))
	}

	// We fetch additional transactions to know if there are more before and/or after.
	sb.Limit(int(q.PageSize + 2))
	t.PageSize = q.PageSize

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return api.Cursor[core.ExpandedTransaction]{}, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
	}
	defer rows.Close()

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
			return api.Cursor[core.ExpandedTransaction]{}, err
		}
		tx.Reference = ref.String
		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		tx.Timestamp = tx.Timestamp.UTC()
		txs = append(txs, tx)
	}
	if rows.Err() != nil {
		return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
	}

	var previous, next string

	// Page with transactions before
	if q.AfterTxID > 0 && len(txs) > 1 && txs[0].ID == q.AfterTxID {
		t.AfterTxID = txs[0].ID + uint64(q.PageSize)
		txs = txs[1:]
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	// Page with transactions after
	if len(txs) > int(q.PageSize) {
		txs = txs[:q.PageSize]
		t.AfterTxID = txs[len(txs)-1].ID
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	hasMore := next != ""
	return api.Cursor[core.ExpandedTransaction]{
		PageSize: int(q.PageSize),
		HasMore:  hasMore,
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
	var queryTxs string
	var argsTxs []any

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	txIds := make([]uint64, len(txs))
	timestamps := make([]time.Time, len(txs))
	references := make([]*string, len(txs))
	postingDataSet := make([]string, len(txs))
	metadataDataSet := make([]string, len(txs))
	preCommitVolumesDataSet := make([]string, len(txs))
	postCommitVolumesDataSet := make([]string, len(txs))

	postingTxIds := []uint64{}
	postingIndices := []int{}
	sources := []string{}
	destinations := []string{}

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

		txIds[i] = tx.ID
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

		for i, p := range tx.Postings {
			sourcesBy, err := json.Marshal(strings.Split(p.Source, ":"))
			if err != nil {
				panic(err)
			}
			destinationsBy, err := json.Marshal(strings.Split(p.Destination, ":"))
			if err != nil {
				panic(err)
			}
			postingTxIds = append(postingTxIds, tx.ID)
			postingIndices = append(postingIndices, i)
			sources = append(sources, string(sourcesBy))
			destinations = append(destinations, string(destinationsBy))
		}

		queryTxs = fmt.Sprintf(
			`INSERT INTO "%s".transactions (id, timestamp, reference,
                               postings, metadata,
                               pre_commit_volumes,
                               post_commit_volumes) (SELECT * FROM unnest(
                                   $1::int[],
                                   $2::timestamp[],
                                   $3::varchar[],
                                   $4::jsonb[],
                                   $5::jsonb[],
                                   $6::jsonb[],
                                   $7::jsonb[]))`,
			s.schema.Name())
		argsTxs = []any{
			txIds, timestamps, references,
			postingDataSet, metadataDataSet,
			preCommitVolumesDataSet, postCommitVolumesDataSet,
		}

		queryPostings := fmt.Sprintf(
			`INSERT INTO "%s".postings (txid, posting_index,
                           source, destination) (SELECT * FROM unnest(
                                   $1::int[],
                                   $2::int[],
                                   $3::jsonb[],
                                   $4::jsonb[]))`,
			s.schema.Name())
		argsPostings := []any{
			postingTxIds, postingIndices, sources, destinations,
		}

		_, err = executor.ExecContext(ctx, queryPostings, argsPostings...)
		if err != nil {
			return s.error(err)
		}
	}

	_, err = executor.ExecContext(ctx, queryTxs, argsTxs...)
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
	ub.Set(fmt.Sprintf("metadata = metadata || %s", placeholder))

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	sqlq, args := ub.BuildWithFlavor(s.schema.Flavor())
	_, err = executor.ExecContext(ctx, sqlq, args...)
	if err != nil {
		return err
	}

	lastLog, err := s.GetLastLog(ctx)
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	return s.appendLog(ctx, core.NewSetMetadataLog(lastLog, at, core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   id,
		Metadata:   metadata,
	}))
}
