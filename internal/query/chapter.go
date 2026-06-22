package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadChapters returns a cursor over all chapters from the given reader, ordered by chapter ID.
func ReadChapters(ctx context.Context, reader dal.PebbleReader) (cursor.Cursor[*commonpb.Chapter], error) {
	_, span := queryTracer.Start(ctx, "query.list_chapters")
	defer span.End()

	cursor, err := dal.ScanZone[*commonpb.Chapter](reader, dal.ZoneGlobal, dal.SubGlobChapters)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for chapters: %w", err)
	}

	return cursor, nil
}

// ReadNextChapterID returns the next chapter ID from the given reader.
// Returns 1 if not found (default starting value).
func ReadNextChapterID(reader dal.PebbleGetter) (uint64, error) {
	v, err := dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobNextChapterID}, 1)
	if err != nil {
		return 0, fmt.Errorf("getting next chapter ID: %w", err)
	}

	return v, nil
}

// ReadChapterSchedule loads the chapter schedule cron expression from the given reader.
// Returns an empty string if no schedule is configured.
func ReadChapterSchedule(reader dal.PebbleGetter) (string, error) {
	v, err := dal.ReadString(reader, []byte{dal.ZoneGlobal, dal.SubGlobChapterSchedule})
	if err != nil {
		return "", fmt.Errorf("loading chapter schedule: %w", err)
	}

	return v, nil
}
