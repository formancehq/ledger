package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

var queryTracer = otel.Tracer("query")

// ReadLedgers returns a cursor over all registered ledgers from the given reader.
func ReadLedgers(ctx context.Context, reader dal.PebbleReader) (dal.Cursor[*commonpb.LedgerInfo], error) {
	_, span := queryTracer.Start(ctx, "query.list_ledgers")
	defer span.End()
	lowerBound := []byte{dal.KeyPrefixLedgerInfo}
	upperBound := []byte{dal.KeyPrefixLedgerInfo + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
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
	kb.PutByte(dal.KeyPrefixLedgerInfo).PutString(name)

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

	if info.DeletedAt != nil {
		return nil, domain.ErrNotFound
	}
	return info, nil
}
