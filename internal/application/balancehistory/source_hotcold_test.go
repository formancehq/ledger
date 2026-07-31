package balancehistory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestHotColdSourceHotOnly(t *testing.T) {
	t.Parallel()

	store := newHotSourceTestStore(t)
	first := successfulHotFixture(1, 1)
	first.entry.Hash = []byte("audit-1")
	seedHotSource(t, store, first)

	source := NewHotColdSource(store, nil, nil, "")
	batch, err := source.Read(context.Background(), Position{}, 10)
	require.NoError(t, err)
	require.Equal(t, Position{AuditSequence: 1, LogSequence: 1, AuditHash: []byte("audit-1")}, batch.Head)
	require.Equal(t, batch.Head, batch.Next)
	require.Len(t, batch.Proposals, 1)
	require.Equal(t, uint64(1), batch.Proposals[0].Logs[0].GetSequence())
}

func TestHotColdSourceColdOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	hot := newHotSourceTestStore(t)
	first := successfulHotFixture(1, 1)
	first.entry.Hash = []byte("audit-1")
	second := successfulHotFixture(2, 2)
	second.entry.Hash = []byte("audit-2")
	chapter := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      2,
		StartAuditSequence: 1,
		CloseAuditSequence: 2,
		LastAuditHash:      []byte("audit-2"),
	}
	seedSourceChapters(t, hot, chapter)

	storage, reader := writeTestArchive(t, chapter, []hotSourceFixture{first, second}, nil)
	source := NewHotColdSource(hot, reader, storage, "bucket")
	batch, err := source.Read(ctx, Position{}, 10)
	require.NoError(t, err)
	require.Equal(t, Position{AuditSequence: 2, LogSequence: 2, AuditHash: []byte("audit-2")}, batch.Head)
	require.Equal(t, batch.Head, batch.Next)
	require.Len(t, batch.Proposals, 2)
	require.Equal(t, uint64(1), batch.Proposals[0].Logs[0].GetSequence())
	require.Equal(t, uint64(2), batch.Proposals[1].Logs[0].GetSequence())
}

func TestHotColdSourceCrossesColdHotBoundaryAtomically(t *testing.T) {
	t.Parallel()

	hot := newHotSourceTestStore(t)
	coldAudit := successfulHotFixture(1, 1)
	coldAudit.entry.Hash = []byte("audit-1")
	closeProposal := successfulHotFixture(2, 2)
	closeProposal.entry.Hash = []byte("audit-2")
	hotProposal := successfulHotFixture(3, 3)
	hotProposal.entry.Hash = []byte("audit-3")
	// The CloseChapter log belongs to chapter 1's log range even though its
	// AuditEntry is the first audit entry of chapter 2.
	closeLog := closeProposal.logs[0]
	closeProposal.logs = nil
	seedHotSource(t, hot, closeProposal, hotProposal)

	archived := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      2,
		StartAuditSequence: 1,
		CloseAuditSequence: 1,
		LastAuditHash:      []byte("audit-1"),
	}
	open := &commonpb.Chapter{
		Id:                 2,
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence:      3,
		StartAuditSequence: 2,
	}
	seedSourceChapters(t, hot, archived, open)

	storage, reader := writeTestArchive(t, archived, []hotSourceFixture{coldAudit}, []*commonpb.Log{closeLog})
	source := NewHotColdSource(hot, reader, storage, "bucket")
	batch, err := source.Read(context.Background(), Position{}, 10)
	require.NoError(t, err)
	require.Len(t, batch.Proposals, 3)
	require.Equal(t, uint64(1), batch.Proposals[0].Logs[0].GetSequence())
	require.Equal(t, uint64(2), batch.Proposals[1].Logs[0].GetSequence())
	require.Equal(t, uint64(3), batch.Proposals[2].Logs[0].GetSequence())
	require.Equal(t, Position{AuditSequence: 3, LogSequence: 3, AuditHash: []byte("audit-3")}, batch.Next)
}

func TestHotColdSourceFailsClosedForMissingArchive(t *testing.T) {
	t.Parallel()

	hot := newHotSourceTestStore(t)
	chapter := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      1,
		StartAuditSequence: 1,
		CloseAuditSequence: 1,
		LastAuditHash:      []byte("audit-1"),
	}
	seedSourceChapters(t, hot, chapter)

	storage := coldstorage.NewFilesystemStorage(t.TempDir())
	reader := coldstorage.NewColdReader(
		storage,
		"bucket",
		t.TempDir(),
		2,
		0,
		logging.FromContext(logging.TestingContext()),
	)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	_, err := NewHotColdSource(hot, reader, storage, "bucket").Read(context.Background(), Position{}, 1)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.ErrorContains(t, err, "checksum")
}

func TestHotColdSourceRejectsCorruptArchive(t *testing.T) {
	t.Parallel()

	hot := newHotSourceTestStore(t)
	fixture := successfulHotFixture(1, 1)
	fixture.entry.Hash = []byte("audit-1")
	chapter := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      1,
		StartAuditSequence: 1,
		CloseAuditSequence: 1,
		LastAuditHash:      []byte("audit-1"),
	}
	seedSourceChapters(t, hot, chapter)

	basePath := t.TempDir()
	storage, reader := writeTestArchiveAt(t, basePath, chapter, []hotSourceFixture{fixture}, nil)
	archivePath := filepath.Join(basePath, "bucket", "chapters", "1", "archive.sst")
	file, err := os.OpenFile(archivePath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString("corruption")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, err = NewHotColdSource(hot, reader, storage, "bucket").Read(context.Background(), Position{}, 1)
	var invalid *ErrSourceInvalid
	require.ErrorAs(t, err, &invalid)
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestHotColdSourceRejectsMissingArchivedPrefix(t *testing.T) {
	t.Parallel()

	hot := newHotSourceTestStore(t)
	seedSourceChapters(t, hot, &commonpb.Chapter{
		Id:                 2,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      9,
		CloseSequence:      10,
		StartAuditSequence: 4,
		CloseAuditSequence: 5,
	})

	_, err := NewHotColdSource(hot, nil, nil, "").Head(context.Background())
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.ErrorContains(t, err, "instead of 1/1")
}

func TestHotColdSourceSnapshotSurvivesConcurrentPurge(t *testing.T) {
	t.Parallel()

	store := newHotSourceTestStore(t)
	fixture := successfulHotFixture(1, 1)
	fixture.entry.Hash = []byte("audit-1")
	seedHotSource(t, store, fixture)
	chapter := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVING,
		StartSequence:      1,
		CloseSequence:      1,
		StartAuditSequence: 1,
		CloseAuditSequence: 1,
		LastAuditHash:      []byte("audit-1"),
	}
	seedSourceChapters(t, store, chapter)

	reader, readerState := newPurgingSnapshotReader(t, store, func() {
		batch := store.OpenWriteSession()
		archived := chapter.CloneVT()
		archived.Status = commonpb.ChapterStatus_CHAPTER_ARCHIVED
		require.NoError(t, state.StoreChapter(batch, archived))
		require.NoError(t, batch.DeleteKey(hotAuditKey(1)))
		require.NoError(t, batch.DeleteKey(hotAuditItemKey(1, 0)))
		require.NoError(t, batch.DeleteKey(hotLogKey(1)))
		require.NoError(t, batch.Commit())
	})

	batch, err := NewHotColdSource(reader, nil, nil, "").Read(context.Background(), Position{}, 1)
	require.NoError(t, err)
	require.Len(t, batch.Proposals, 1)
	require.Equal(t, uint64(1), batch.Proposals[0].Logs[0].GetSequence())
	require.True(t, readerState.purged)
}

func TestHotColdSourceLeaseSurvivesConcurrentColdEviction(t *testing.T) {
	t.Parallel()

	const proposalCount = 500

	hot := newHotSourceTestStore(t)
	fixtures := make([]hotSourceFixture, 0, proposalCount)
	for sequence := uint64(1); sequence <= proposalCount; sequence++ {
		fixture := successfulHotFixture(sequence, sequence)
		fixture.entry.Hash = fmt.Appendf(nil, "audit-%d", sequence)
		fixtures = append(fixtures, fixture)
	}
	chapter := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      proposalCount,
		StartAuditSequence: 1,
		CloseAuditSequence: proposalCount,
		LastAuditHash:      append([]byte(nil), fixtures[len(fixtures)-1].entry.GetHash()...),
	}
	seedSourceChapters(t, hot, chapter)

	basePath := t.TempDir()
	storage, _ := writeTestArchiveAt(t, basePath, chapter, fixtures, nil)
	for chapterID := uint64(2); chapterID <= 3; chapterID++ {
		sequence := proposalCount + chapterID
		fixture := successfulHotFixture(sequence, sequence)
		fixture.entry.Hash = fmt.Appendf(nil, "audit-%d", sequence)
		_, _ = writeTestArchiveAt(t, basePath, &commonpb.Chapter{
			Id:                 chapterID,
			Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
			StartSequence:      sequence,
			CloseSequence:      sequence,
			StartAuditSequence: sequence,
			CloseAuditSequence: sequence,
			LastAuditHash:      append([]byte(nil), fixture.entry.GetHash()...),
		}, []hotSourceFixture{fixture}, nil)
	}

	coldReader := coldstorage.NewColdReader(
		storage,
		"bucket",
		t.TempDir(),
		1,
		2*time.Millisecond,
		logging.FromContext(logging.TestingContext()),
	)
	t.Cleanup(func() { require.NoError(t, coldReader.Close()) })
	source := NewHotColdSource(hot, coldReader, storage, "bucket")

	stressCtx, cancelStress := context.WithCancel(context.Background())
	var evictionAttempts atomic.Uint64
	stressDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stressCtx.Done():
				stressDone <- nil

				return
			default:
			}
			for chapterID := uint64(2); chapterID <= 3; chapterID++ {
				if _, err := coldReader.GetReader(stressCtx, chapterID); err != nil {
					stressDone <- err

					return
				}
				evictionAttempts.Add(1)
			}
		}
	}()

	batch, err := source.Read(context.Background(), Position{}, proposalCount)
	cancelStress()
	require.NoError(t, <-stressDone)
	require.NoError(t, err)
	require.Greater(t, evictionAttempts.Load(), uint64(2))
	require.Len(t, batch.Proposals, proposalCount)
	require.Equal(t, uint64(1), batch.Proposals[0].Logs[0].GetSequence())
	require.Equal(t, uint64(proposalCount), batch.Proposals[len(batch.Proposals)-1].Logs[0].GetSequence())
}

type purgingSnapshotReaderState struct {
	store  *dal.Store
	once   sync.Once
	purge  func()
	purged bool
}

func newPurgingSnapshotReader(
	t *testing.T,
	store *dal.Store,
	purge func(),
) (*MockSnapshotReader, *purgingSnapshotReaderState) {
	t.Helper()

	state := &purgingSnapshotReaderState{store: store, purge: purge}
	reader := NewMockSnapshotReader(gomock.NewController(t))
	reader.EXPECT().NewReadHandle().AnyTimes().DoAndReturn(state.newReadHandle)

	return reader, state
}

func (r *purgingSnapshotReaderState) newReadHandle() (*dal.ReadHandle, error) {
	handle, err := r.store.NewReadHandle()
	if err != nil {
		return nil, err
	}
	r.once.Do(func() {
		r.purge()
		r.purged = true
	})

	return handle, nil
}

func seedSourceChapters(t *testing.T, store *dal.Store, chapters ...*commonpb.Chapter) {
	t.Helper()

	batch := store.OpenWriteSession()
	for _, chapter := range chapters {
		require.NoError(t, state.StoreChapter(batch, chapter))
	}
	require.NoError(t, batch.Commit())
}

type testArchivePair struct {
	key   []byte
	value []byte
}

func writeTestArchive(
	t *testing.T,
	chapter *commonpb.Chapter,
	fixtures []hotSourceFixture,
	extraLogs []*commonpb.Log,
) (coldstorage.ColdStorage, *coldstorage.ColdReader) {
	t.Helper()

	return writeTestArchiveAt(t, t.TempDir(), chapter, fixtures, extraLogs)
}

func writeTestArchiveAt(
	t *testing.T,
	basePath string,
	chapter *commonpb.Chapter,
	fixtures []hotSourceFixture,
	extraLogs []*commonpb.Log,
) (coldstorage.ColdStorage, *coldstorage.ColdReader) {
	t.Helper()

	metadata, err := json.Marshal(archiveMetadata{
		ChapterID:          chapter.GetId(),
		StartSequence:      chapter.GetStartSequence(),
		CloseSequence:      chapter.GetCloseSequence(),
		StartAuditSequence: chapter.GetStartAuditSequence(),
		CloseAuditSequence: chapter.GetCloseAuditSequence(),
	})
	require.NoError(t, err)

	pairsByKey := map[string]testArchivePair{
		string(state.MetadataKey): {key: append([]byte(nil), state.MetadataKey...), value: metadata},
	}
	for _, fixture := range fixtures {
		putTestArchiveProto(t, pairsByKey, hotAuditKey(fixture.entry.GetSequence()), fixture.entry.MarshalVT)
		for _, item := range fixture.items {
			putTestArchiveProto(t, pairsByKey, hotAuditItemKey(fixture.entry.GetSequence(), item.GetOrderIndex()), item.MarshalVT)
		}
		for _, log := range fixture.logs {
			putTestArchiveProto(t, pairsByKey, hotLogKey(log.GetSequence()), log.MarshalVT)
		}
	}
	for _, log := range extraLogs {
		putTestArchiveProto(t, pairsByKey, hotLogKey(log.GetSequence()), log.MarshalVT)
	}
	pairs := make([]testArchivePair, 0, len(pairsByKey))
	for _, pair := range pairsByKey {
		pairs = append(pairs, pair)
	}
	slices.SortFunc(pairs, func(left, right testArchivePair) int { return bytes.Compare(left.key, right.key) })

	sstPath := filepath.Join(t.TempDir(), "history-chapter.sst")
	sstFile, err := vfs.Default.Create(sstPath, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	writer := sstable.NewWriter(objstorageprovider.NewFileWritable(sstFile), sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
	})
	for _, pair := range pairs {
		require.NoError(t, writer.Set(pair.key, pair.value))
	}
	require.NoError(t, writer.Close())

	file, err := os.Open(sstPath)
	require.NoError(t, err)
	checksum, err := coldstorage.ComputeSHA256(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	file, err = os.Open(sstPath)
	require.NoError(t, err)
	storage := coldstorage.NewFilesystemStorage(basePath)
	require.NoError(t, storage.Archive(context.Background(), "bucket", chapter.GetId(), file, checksum))
	require.NoError(t, file.Close())

	reader := coldstorage.NewColdReader(
		storage,
		"bucket",
		t.TempDir(),
		4,
		0,
		logging.FromContext(logging.TestingContext()),
	)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	return storage, reader
}

func putTestArchiveProto(
	t *testing.T,
	pairs map[string]testArchivePair,
	key []byte,
	marshal func() ([]byte, error),
) {
	t.Helper()

	value, err := marshal()
	require.NoError(t, err)
	pairs[string(key)] = testArchivePair{key: key, value: value}
}
