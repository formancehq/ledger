package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadPreparedQuery reads a single prepared query by ledger and name from the attributes zone.
func ReadPreparedQuery(ctx context.Context, attr *attributes.Attribute[*commonpb.PreparedQuery], reader dal.PebbleReader, ledger, name string) (*commonpb.PreparedQuery, error) {
	_, span := queryTracer.Start(ctx, "query.get_prepared_query",
		trace.WithAttributes(
			attribute.String("ledger", ledger),
			attribute.String("name", name),
		))
	defer span.End()

	return attr.Get(reader, domain.PreparedQueryKey{Ledger: ledger, Name: name}.Bytes())
}

// ReadPreparedQueries reads all prepared queries for a ledger from the attributes zone.
func ReadPreparedQueries(ctx context.Context, attr *attributes.Attribute[*commonpb.PreparedQuery], reader dal.PebbleReader, ledger string) ([]*commonpb.PreparedQuery, error) {
	_, span := queryTracer.Start(ctx, "query.list_prepared_queries",
		trace.WithAttributes(attribute.String("ledger", ledger)))
	defer span.End()

	// Use the ledger name as canonical key prefix to scope the scan.
	prefix := append([]byte(ledger), 0x00)

	entries, err := attr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning prepared queries for ledger %s: %w", ledger, err)
	}

	queries := make([]*commonpb.PreparedQuery, 0, len(entries))
	for _, entry := range entries {
		queries = append(queries, entry.Value)
	}

	return queries, nil
}
