package query

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

var queryTracer = otel.Tracer("query")

// ReadLedgers returns a cursor over all registered ledgers from the given reader.
func ReadLedgers(ctx context.Context, reader dal.PebbleReader) (cursor.Cursor[*commonpb.LedgerInfo], error) {
	_, span := queryTracer.Start(ctx, "query.list_ledgers")
	defer span.End()

	cursor, err := dal.ScanZone[*commonpb.LedgerInfo](reader, dal.ZoneGlobal, dal.SubGlobLedgerInfo)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for ledger info: %w", err)
	}

	return cursor, nil
}

// GetLedgerByName retrieves a ledger by its name from the given reader.
// Returns domain.ErrNotFound if the ledger does not exist or is soft-deleted.
func GetLedgerByName(ctx context.Context, reader dal.PebbleGetter, name string) (*commonpb.LedgerInfo, error) {
	_, span := queryTracer.Start(ctx, "query.get_ledger",
		trace.WithAttributes(attribute.String("ledger", name)))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).PutLedgerName(name)

	info, err := dal.ReadProto[*commonpb.LedgerInfo](reader, kb.Build())
	if err != nil {
		return nil, fmt.Errorf("getting ledger by name: %w", err)
	}

	if info == nil || info.GetDeletedAt() != nil {
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

	// Canonical prefix for this ledger's metadata: [ledgerID BE 4B]\x01
	prefix := make([]byte, 4+1)
	binary.BigEndian.PutUint32(prefix[0:4], info.GetId())
	prefix[4] = 0x01

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

// ReadNextLedgerID reads the next ledger ID counter from Pebble.
// Returns 1 if no counter has been stored yet.
func ReadNextLedgerID(reader dal.PebbleGetter) (uint32, error) {
	v, err := dal.ReadUint32(reader, []byte{dal.ZoneGlobal, dal.SubGlobNextLedgerID}, 1)
	if err != nil {
		return 0, fmt.Errorf("getting next ledger ID: %w", err)
	}

	return v, nil
}
