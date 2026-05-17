package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadPeriods returns a cursor over all periods from the given reader, ordered by period ID.
func ReadPeriods(ctx context.Context, reader dal.PebbleReader) (dal.Cursor[*commonpb.Period], error) {
	_, span := queryTracer.Start(ctx, "query.list_periods")
	defer span.End()

	lowerBound := []byte{dal.ZoneGlobal, dal.SubGlobPeriods}
	upperBound := []byte{dal.ZoneGlobal, dal.SubGlobPeriods, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for periods: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.Period](iter), nil
}

// ReadNextPeriodID returns the next period ID from the given reader.
// Returns 1 if not found (default starting value).
func ReadNextPeriodID(reader dal.PebbleReader) (uint64, error) {
	value, closer, err := reader.Get([]byte{dal.ZoneGlobal, dal.SubGlobNextPeriodID})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 1, nil
		}

		return 0, fmt.Errorf("getting next period ID: %w", err)
	}

	defer func() {
		_ = closer.Close()
	}()

	return binary.BigEndian.Uint64(value[:8]), nil
}

// ReadPeriodSchedule loads the period schedule cron expression from the given reader.
// Returns an empty string if no schedule is configured.
func ReadPeriodSchedule(reader dal.PebbleReader) (string, error) {
	value, closer, err := reader.Get([]byte{dal.ZoneGlobal, dal.SubGlobPeriodSchedule})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", nil
		}

		return "", fmt.Errorf("loading period schedule: %w", err)
	}

	defer func() { _ = closer.Close() }()

	return string(value), nil
}
