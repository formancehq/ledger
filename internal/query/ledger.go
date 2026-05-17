package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

var queryTracer = otel.Tracer("query")

// ReadLedgers returns a cursor over all registered ledgers from the given reader.
func ReadLedgers(ctx context.Context, reader dal.PebbleReader) (dal.Cursor[*commonpb.LedgerInfo], error) {
	_, span := queryTracer.Start(ctx, "query.list_ledgers")
	defer span.End()

	lowerBound := []byte{dal.ZoneGlobal, dal.SubGlobLedgerInfo}
	upperBound := []byte{dal.ZoneGlobal, dal.SubGlobLedgerInfo + 1}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for ledger info: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.LedgerInfo](iter), nil
}

// GetLedgerByName retrieves a ledger by its name from the given reader.
// Returns domain.ErrNotFound if the ledger does not exist or is soft-deleted.
func GetLedgerByName(ctx context.Context, reader dal.PebbleReader, name string) (*commonpb.LedgerInfo, error) {
	_, span := queryTracer.Start(ctx, "query.get_ledger",
		trace.WithAttributes(attribute.String("ledger", name)))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).PutLedgerName(name)

	value, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, domain.ErrNotFound
		}

		return nil, fmt.Errorf("getting ledger by name: %w", err)
	}

	defer func() { _ = closer.Close() }()

	info := &commonpb.LedgerInfo{}
	if err := proto.Unmarshal(value, info); err != nil {
		return nil, fmt.Errorf("unmarshaling ledger info: %w", err)
	}

	if info.GetDeletedAt() != nil {
		return nil, domain.ErrNotFound
	}

	return info, nil
}

// EnrichLedgerMetadata populates the Metadata field on LedgerInfo by scanning
// the ledger metadata attributes from Pebble. The metadata field on LedgerInfo
// is read-time only (not stored as part of LedgerInfo in the attribute store).
func EnrichLedgerMetadata(reader dal.PebbleReader, attrs *attributes.Attributes, info *commonpb.LedgerInfo) error {
	if info == nil {
		return nil
	}

	// Canonical prefix for this ledger's metadata: [ledger]\x01
	prefix := make([]byte, len(info.GetName())+1)
	n := copy(prefix, info.GetName())
	prefix[n] = 0x01

	entries, err := attrs.LedgerMetadata.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return fmt.Errorf("scanning ledger metadata for %q: %w", info.GetName(), err)
	}

	if len(entries) == 0 {
		return nil
	}

	metadata := make(map[string]*commonpb.MetadataValue, len(entries))

	for _, entry := range entries {
		var key domain.LedgerMetadataKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			return fmt.Errorf("unmarshaling ledger metadata key: %w", err)
		}

		metadata[key.Key] = entry.Value
	}

	// Read-time enforcement: convert values to declared types.
	for k, v := range metadata {
		fieldSchema, ok := info.GetMetadataSchema().GetLedgerFields()[k]
		if !ok || v == nil {
			continue
		}

		if !commonpb.TypeMatches(v, fieldSchema.GetType()) {
			metadata[k] = commonpb.ConvertMetadataValue(v, fieldSchema.GetType())
		}
	}

	info.Metadata = metadata

	return nil
}
