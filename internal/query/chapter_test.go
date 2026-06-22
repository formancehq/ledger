package query_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func collectAllChapters(reader dal.PebbleReader) ([]*commonpb.Chapter, error) {
	c, err := query.ReadChapters(context.Background(), reader)
	if err != nil {
		return nil, err
	}

	return cursor.Collect(c)
}

func TestReadChapters(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		chapters, err := collectAllChapters(handle)
		require.NoError(t, err)
		require.Nil(t, chapters)

		nextID, err := query.ReadNextChapterID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nextID)
	})

	t.Run("StoreSingleChapter", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		batch := s.OpenWriteSession()
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.ChapterStatus_CHAPTER_OPEN,
		}))
		require.NoError(t, state.StoreNextChapterID(batch, 2))
		require.NoError(t, batch.Commit())

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		chapters, err := collectAllChapters(handle)
		require.NoError(t, err)
		require.Len(t, chapters, 1)
		require.Equal(t, uint64(1), chapters[0].GetId())
		require.Equal(t, uint64(1000), chapters[0].GetStart().GetData())
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, chapters[0].GetStatus())

		nextID, err := query.ReadNextChapterID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)
	})

	t.Run("StoreMultipleChaptersOrderedByID", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Insert chapters out of order
		batch := s.OpenWriteSession()
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     3,
			Start:  &commonpb.Timestamp{Data: 3000},
			Status: commonpb.ChapterStatus_CHAPTER_OPEN,
		}))
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.ChapterStatus_CHAPTER_CLOSED,
		}))
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:            2,
			Start:         &commonpb.Timestamp{Data: 2000},
			End:           &commonpb.Timestamp{Data: 3000},
			Status:        commonpb.ChapterStatus_CHAPTER_CLOSED,
			CloseSequence: 10,
			SealingHash:   []byte("hash-2"),
		}))
		require.NoError(t, state.StoreNextChapterID(batch, 4))
		require.NoError(t, batch.Commit())

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// Verify chapters are returned ordered by ID
		chapters, err := collectAllChapters(handle)
		require.NoError(t, err)
		require.Len(t, chapters, 3)
		require.Equal(t, uint64(1), chapters[0].GetId())
		require.Equal(t, uint64(2), chapters[1].GetId())
		require.Equal(t, uint64(3), chapters[2].GetId())

		// Verify fields
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSED, chapters[0].GetStatus())
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSED, chapters[1].GetStatus())
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, chapters[2].GetStatus())
		require.Equal(t, uint64(10), chapters[1].GetCloseSequence())
		require.Equal(t, []byte("hash-2"), chapters[1].GetSealingHash())

		nextID, err := query.ReadNextChapterID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(4), nextID)
	})

	t.Run("UpdateExistingChapter", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Store initial chapter
		batch := s.OpenWriteSession()
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.ChapterStatus_CHAPTER_OPEN,
		}))
		require.NoError(t, state.StoreNextChapterID(batch, 2))
		require.NoError(t, batch.Commit())

		// Update the same chapter (close it)
		batch = s.OpenWriteSession()
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:            1,
			Start:         &commonpb.Timestamp{Data: 1000},
			End:           &commonpb.Timestamp{Data: 2000},
			Status:        commonpb.ChapterStatus_CHAPTER_CLOSED,
			CloseSequence: 5,
			SealingHash:   []byte("sealed"),
		}))
		require.NoError(t, batch.Commit())

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		chapters, err := collectAllChapters(handle)
		require.NoError(t, err)
		require.Len(t, chapters, 1)
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSED, chapters[0].GetStatus())
		require.Equal(t, uint64(5), chapters[0].GetCloseSequence())
		require.Equal(t, []byte("sealed"), chapters[0].GetSealingHash())
	})

	t.Run("PersistAcrossReopen", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		// Store chapters and close
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)

		batch := s.OpenWriteSession()
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.ChapterStatus_CHAPTER_CLOSED,
		}))
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     2,
			Start:  &commonpb.Timestamp{Data: 2000},
			Status: commonpb.ChapterStatus_CHAPTER_OPEN,
		}))
		require.NoError(t, state.StoreNextChapterID(batch, 3))
		require.NoError(t, batch.Commit())

		// Create snapshot so data survives reopen (writes use NoSync)
		_, err = s.CreateSnapshot()
		require.NoError(t, err)
		require.NoError(t, s.Close())

		// Reopen and verify
		s2, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s2.Close() })

		handle, err := s2.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		chapters, err := collectAllChapters(handle)
		require.NoError(t, err)
		require.Len(t, chapters, 2)
		require.Equal(t, uint64(1), chapters[0].GetId())
		require.Equal(t, uint64(2), chapters[1].GetId())

		nextID, err := query.ReadNextChapterID(s2)
		require.NoError(t, err)
		require.Equal(t, uint64(3), nextID)
	})

	t.Run("NextChapterIDUpdate", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Set to 5
		batch := s.OpenWriteSession()
		require.NoError(t, state.StoreNextChapterID(batch, 5))
		require.NoError(t, batch.Commit())

		nextID, err := query.ReadNextChapterID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(5), nextID)

		// Update to 10
		batch = s.OpenWriteSession()
		require.NoError(t, state.StoreNextChapterID(batch, 10))
		require.NoError(t, batch.Commit())

		nextID, err = query.ReadNextChapterID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(10), nextID)
	})

	t.Run("AtomicBatchWithChaptersAndLogs", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		registerLedger(t, s, "test-ledger")

		// Store chapters, nextChapterID, and logs in the same batch
		batch := s.OpenWriteSession()
		require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.ChapterStatus_CHAPTER_OPEN,
		}))
		require.NoError(t, state.StoreNextChapterID(batch, 2))
		require.NoError(t, state.SetAppliedIndex(batch, 42))
		require.NoError(t, batch.Commit())

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// Verify all data was written atomically
		chapters, err := collectAllChapters(handle)
		require.NoError(t, err)
		require.Len(t, chapters, 1)

		nextID, err := query.ReadNextChapterID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)

		lastIndex, err := query.ReadLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastIndex)
	})
}
