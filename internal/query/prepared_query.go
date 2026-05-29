package query

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadPreparedQuery reads a single prepared query by ledger and name from the attributes zone.
func ReadPreparedQuery(ctx context.Context, attr *attributes.Attribute[*commonpb.PreparedQuery], reader dal.PebbleReader, ledgerID uint32, name string) (*commonpb.PreparedQuery, error) {
	_, span := queryTracer.Start(ctx, "query.get_prepared_query",
		trace.WithAttributes(
			attribute.Int64("ledger_id", int64(ledgerID)),
			attribute.String("name", name),
		))
	defer span.End()

	return attr.Get(reader, domain.PreparedQueryKey{LedgerID: ledgerID, Name: name}.Bytes())
}

// ReadPreparedQueries reads all prepared queries for a ledger from the attributes zone.
func ReadPreparedQueries(ctx context.Context, attr *attributes.Attribute[*commonpb.PreparedQuery], reader dal.PebbleReader, ledgerID uint32) ([]*commonpb.PreparedQuery, error) {
	_, span := queryTracer.Start(ctx, "query.list_prepared_queries",
		trace.WithAttributes(attribute.Int64("ledger_id", int64(ledgerID))))
	defer span.End()

	// Use the ledger ID as canonical key prefix to scope the scan.
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, ledgerID)

	entries, err := attr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning prepared queries for ledger %d: %w", ledgerID, err)
	}

	queries := make([]*commonpb.PreparedQuery, 0, len(entries))
	for _, entry := range entries {
		queries = append(queries, entry.Value)
	}

	return queries, nil
}
