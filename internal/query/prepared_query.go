package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadPreparedQuery reads a single prepared query by ledger and name from the attributes zone.
func ReadPreparedQuery(ctx context.Context, attr *attributes.Attribute[*commonpb.PreparedQuery], reader dal.PebbleGetter, ledgerName string, name string) (*commonpb.PreparedQuery, error) {
	_, span := queryTracer.Start(ctx, "query.get_prepared_query",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.String("name", name),
		))
	defer span.End()

	return attr.Get(reader, domain.PreparedQueryKey{LedgerName: ledgerName, Name: name}.Bytes())
}

// ReadPreparedQueries reads all prepared queries for a ledger from the attributes zone.
func ReadPreparedQueries(ctx context.Context, attr *attributes.Attribute[*commonpb.PreparedQuery], reader dal.PebbleReader, ledgerName string) ([]*commonpb.PreparedQuery, error) {
	_, span := queryTracer.Start(ctx, "query.list_prepared_queries",
		trace.WithAttributes(attribute.String("ledger", ledgerName)))
	defer span.End()

	// The ledger-scoped canonical prefix is the fixed-width padded name block —
	// every PreparedQueryKey for this ledger starts with these 64 bytes.
	prefix := make([]byte, dal.LedgerNameFixedSize)
	copy(prefix, ledgerName)

	entries, err := attr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning prepared queries for ledger %q: %w", ledgerName, err)
	}

	queries := make([]*commonpb.PreparedQuery, 0, len(entries))
	for _, entry := range entries {
		queries = append(queries, entry.Value)
	}

	return queries, nil
}
