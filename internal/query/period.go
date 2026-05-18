package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/cursor"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadPeriods returns a cursor over all periods from the given reader, ordered by period ID.
func ReadPeriods(ctx context.Context, reader dal.PebbleReader) (cursor.Cursor[*commonpb.Period], error) {
	_, span := queryTracer.Start(ctx, "query.list_periods")
	defer span.End()

	cursor, err := dal.ScanZone[*commonpb.Period](reader, dal.ZoneGlobal, dal.SubGlobPeriods)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for periods: %w", err)
	}

	return cursor, nil
}

// ReadNextPeriodID returns the next period ID from the given reader.
// Returns 1 if not found (default starting value).
func ReadNextPeriodID(reader dal.PebbleReader) (uint64, error) {
	v, err := dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobNextPeriodID}, 1)
	if err != nil {
		return 0, fmt.Errorf("getting next period ID: %w", err)
	}

	return v, nil
}

// ReadPeriodSchedule loads the period schedule cron expression from the given reader.
// Returns an empty string if no schedule is configured.
func ReadPeriodSchedule(reader dal.PebbleReader) (string, error) {
	v, err := dal.ReadString(reader, []byte{dal.ZoneGlobal, dal.SubGlobPeriodSchedule})
	if err != nil {
		return "", fmt.Errorf("loading period schedule: %w", err)
	}

	return v, nil
}
