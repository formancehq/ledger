package balancehistory

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// HotColdSource reconstructs one consecutive audit/log stream from immutable
// archived chapters and the primary-store tail. It deliberately scans logs in
// chapter-sized groups after collecting a bounded proposal batch: resolving a
// proposal never degenerates into one cold-storage lookup per audit item.
//
// The primary-store snapshot also pins the chapter registry. A concurrent
// ConfirmArchiveChapter may therefore purge the live ranges while Read is in
// progress without changing whether this call reads those ranges from hot or
// cold storage.
type HotColdSource struct {
	hot        dal.SnapshotReader
	coldReader *coldstorage.ColdReader
	bucketID   string

	verifiedMu      sync.Mutex
	verifiedReaders map[uint64]dal.PebbleReader
}

var _ Source = (*HotColdSource)(nil)

// NewHotColdSource builds the full rebuild source. coldReader may be nil while
// no archived chapter exists; encountering an archive in
// that configuration fails closed with ErrSourceMissing.
func NewHotColdSource(
	hot dal.SnapshotReader,
	coldReader *coldstorage.ColdReader,
	bucketID string,
) *HotColdSource {
	return &HotColdSource{
		hot:             hot,
		coldReader:      coldReader,
		bucketID:        bucketID,
		verifiedReaders: make(map[uint64]dal.PebbleReader),
	}
}

func (s *HotColdSource) Head(ctx context.Context) (position Position, err error) {
	snapshot, err := s.openSnapshot(ctx)
	if err != nil {
		return Position{}, err
	}
	defer func() {
		if closeErr := snapshot.close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	return snapshot.head, nil
}

func (s *HotColdSource) Read(
	ctx context.Context,
	after Position,
	maxProposals int,
) (batch Batch, err error) {
	if maxProposals <= 0 {
		return Batch{}, fmt.Errorf("balance history source batch limit must be positive, got %d", maxProposals)
	}
	if err := ctx.Err(); err != nil {
		return Batch{}, err
	}

	snapshot, err := s.openSnapshot(ctx)
	if err != nil {
		return Batch{}, err
	}
	defer func() {
		if closeErr := snapshot.close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err := validateCursorAgainstHead(after, snapshot.head); err != nil {
		return Batch{}, err
	}
	if after.AuditSequence == snapshot.head.AuditSequence {
		return Batch{Next: clonePosition(after), Head: clonePosition(snapshot.head)}, nil
	}

	proposals, next, err := s.readProposalHeaders(ctx, snapshot, after, maxProposals)
	if err != nil {
		return Batch{}, err
	}
	if len(proposals) == 0 {
		return Batch{}, &ErrSourceMissing{Detail: fmt.Sprintf(
			"expected audit sequence %d, but no source entry is available before head %d",
			after.AuditSequence+1,
			snapshot.head.AuditSequence,
		)}
	}

	if err := s.resolveProposalLogs(ctx, snapshot, proposals); err != nil {
		return Batch{}, err
	}

	return Batch{
		Proposals: proposals,
		Next:      next,
		Head:      clonePosition(snapshot.head),
	}, nil
}

type hotColdSnapshot struct {
	hot      *dal.ReadHandle
	chapters []*commonpb.Chapter
	archived []*commonpb.Chapter
	head     Position
}

func (s *HotColdSource) openSnapshot(ctx context.Context) (*hotColdSnapshot, error) {
	if s.hot == nil {
		return nil, &ErrSourceMissing{Detail: "primary snapshot reader is not configured"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	hot, err := s.hot.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("opening hot+cold balance history snapshot: %w", err)
	}
	snapshot := &hotColdSnapshot{hot: hot}

	chapters, err := readSourceChapters(ctx, hot)
	if err != nil {
		_ = hot.Close()

		return nil, err
	}
	snapshot.chapters = chapters
	for _, chapter := range chapters {
		if chapter.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			snapshot.archived = append(snapshot.archived, chapter)
		}
	}
	if err := validateChapterTopology(chapters); err != nil {
		_ = hot.Close()

		return nil, err
	}

	hotHead, err := readHotHead(hot)
	if err != nil {
		_ = hot.Close()

		return nil, err
	}
	snapshot.head = combinedSourceHead(hotHead, snapshot.archived)

	return snapshot, nil
}

func (s *hotColdSnapshot) close() error {
	if s.hot == nil {
		return nil
	}
	if err := s.hot.Close(); err != nil {
		return fmt.Errorf("closing hot+cold balance history snapshot: %w", err)
	}
	s.hot = nil

	return nil
}

func readSourceChapters(ctx context.Context, reader dal.PebbleReader) ([]*commonpb.Chapter, error) {
	chaptersCursor, err := query.ReadChapters(ctx, reader)
	if err != nil {
		return nil, fmt.Errorf("opening chapter registry for balance history: %w", err)
	}
	defer func() { _ = chaptersCursor.Close() }()

	chapters := make([]*commonpb.Chapter, 0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chapter, nextErr := chaptersCursor.Next()
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				break
			}

			return nil, fmt.Errorf("reading chapter registry for balance history: %w", nextErr)
		}
		chapters = append(chapters, chapter)
	}

	return chapters, nil
}

func validateChapterTopology(chapters []*commonpb.Chapter) error {
	var (
		previousID       uint64
		previousArchived *commonpb.Chapter
		seenLive         bool
	)
	for index, chapter := range chapters {
		if chapter == nil {
			return &ErrSourceInvalid{Detail: fmt.Sprintf("chapter registry row %d is nil", index)}
		}
		if index > 0 && chapter.GetId() <= previousID {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"chapter ids are not strictly increasing at %d after %d",
				chapter.GetId(),
				previousID,
			)}
		}
		previousID = chapter.GetId()

		if chapter.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			seenLive = true

			continue
		}
		if seenLive {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"archived chapter %d follows a non-archived chapter",
				chapter.GetId(),
			)}
		}
		if chapter.GetStartSequence() == 0 || chapter.GetCloseSequence() < chapter.GetStartSequence() {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"archived chapter %d has invalid log range [%d,%d]",
				chapter.GetId(),
				chapter.GetStartSequence(),
				chapter.GetCloseSequence(),
			)}
		}
		if chapter.GetStartAuditSequence() == 0 || chapter.GetCloseAuditSequence()+1 < chapter.GetStartAuditSequence() {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"archived chapter %d has invalid audit range [%d,%d]",
				chapter.GetId(),
				chapter.GetStartAuditSequence(),
				chapter.GetCloseAuditSequence(),
			)}
		}
		if previousArchived == nil {
			if chapter.GetStartSequence() != 1 || chapter.GetStartAuditSequence() != 1 {
				return &ErrSourceMissing{Detail: fmt.Sprintf(
					"oldest archived chapter %d starts at log %d audit %d instead of 1/1",
					chapter.GetId(),
					chapter.GetStartSequence(),
					chapter.GetStartAuditSequence(),
				)}
			}
		} else if chapter.GetStartSequence() != previousArchived.GetCloseSequence()+1 ||
			chapter.GetStartAuditSequence() != previousArchived.GetCloseAuditSequence()+1 {
			return &ErrSourceMissing{Detail: fmt.Sprintf(
				"archived chapter %d does not continue chapter %d ranges",
				chapter.GetId(),
				previousArchived.GetId(),
			)}
		}
		previousArchived = chapter
	}

	return nil
}

func combinedSourceHead(hot Position, archived []*commonpb.Chapter) Position {
	head := clonePosition(hot)
	for _, chapter := range archived {
		if chapter.GetCloseSequence() > head.LogSequence {
			head.LogSequence = chapter.GetCloseSequence()
		}
		if chapter.GetCloseAuditSequence() > head.AuditSequence {
			head.AuditSequence = chapter.GetCloseAuditSequence()
			head.AuditHash = append(head.AuditHash[:0], chapter.GetLastAuditHash()...)
		}
	}

	return head
}

func validateCursorAgainstHead(after, head Position) error {
	if after.AuditSequence > head.AuditSequence {
		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"cursor audit sequence %d is ahead of source head %d",
			after.AuditSequence,
			head.AuditSequence,
		)}
	}
	if after.LogSequence > head.LogSequence {
		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"cursor log sequence %d is ahead of source head %d",
			after.LogSequence,
			head.LogSequence,
		)}
	}
	if after.AuditSequence == head.AuditSequence {
		if after.LogSequence != head.LogSequence {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"cursor covers audit head %d but log watermark is %d instead of %d",
				head.AuditSequence,
				after.LogSequence,
				head.LogSequence,
			)}
		}
		if len(after.AuditHash) > 0 && len(head.AuditHash) > 0 && !bytes.Equal(after.AuditHash, head.AuditHash) {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"cursor audit hash %x differs from source head hash %x at sequence %d",
				after.AuditHash,
				head.AuditHash,
				head.AuditSequence,
			)}
		}
	}

	return nil
}

func clonePosition(position Position) Position {
	position.AuditHash = append([]byte(nil), position.AuditHash...)

	return position
}

func (s *HotColdSource) readProposalHeaders(
	ctx context.Context,
	snapshot *hotColdSnapshot,
	after Position,
	maxProposals int,
) ([]VerifiedProposal, Position, error) {
	proposals := make([]VerifiedProposal, 0, maxProposals)
	next := clonePosition(after)
	expectedAuditSequence := after.AuditSequence + 1

	for _, chapter := range snapshot.archived {
		if len(proposals) >= maxProposals || expectedAuditSequence > chapter.GetCloseAuditSequence() {
			continue
		}
		if expectedAuditSequence < chapter.GetStartAuditSequence() {
			return nil, after, &ErrSourceMissing{Detail: fmt.Sprintf(
				"expected audit sequence %d before archived chapter %d starts at %d",
				expectedAuditSequence,
				chapter.GetId(),
				chapter.GetStartAuditSequence(),
			)}
		}

		reader, release, err := s.archiveReader(ctx, chapter)
		if err != nil {
			return nil, after, err
		}
		readErr := func() (err error) {
			defer func() {
				err = errors.Join(err, release())
			}()

			return collectProposalHeaders(
				ctx,
				reader,
				chapter.GetCloseAuditSequence(),
				maxProposals,
				&proposals,
				&next,
				&expectedAuditSequence,
			)
		}()
		if readErr != nil {
			return nil, after, fmt.Errorf("reading archived chapter %d: %w", chapter.GetId(), readErr)
		}
	}

	if len(proposals) < maxProposals && expectedAuditSequence <= snapshot.head.AuditSequence {
		if err := collectProposalHeaders(
			ctx,
			snapshot.hot,
			snapshot.head.AuditSequence,
			maxProposals,
			&proposals,
			&next,
			&expectedAuditSequence,
		); err != nil {
			return nil, after, fmt.Errorf("reading hot audit tail: %w", err)
		}
	}

	return proposals, next, nil
}

func collectProposalHeaders(
	ctx context.Context,
	reader dal.PebbleReader,
	endAuditSequence uint64,
	maxProposals int,
	proposals *[]VerifiedProposal,
	next *Position,
	expectedAuditSequence *uint64,
) (err error) {
	if *expectedAuditSequence > endAuditSequence || len(*proposals) >= maxProposals {
		return nil
	}

	afterSequence := *expectedAuditSequence - 1
	entries, err := query.ReadAuditEntries(ctx, reader, &afterSequence)
	if err != nil {
		return fmt.Errorf("opening audit entries after %d: %w", afterSequence, err)
	}
	defer func() {
		if closeErr := entries.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing audit entry cursor: %w", closeErr))
		}
	}()

	for len(*proposals) < maxProposals && *expectedAuditSequence <= endAuditSequence {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, nextErr := entries.Next()
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				return &ErrSourceMissing{Detail: fmt.Sprintf(
					"expected audit sequence %d before physical range ends at %d",
					*expectedAuditSequence,
					endAuditSequence,
				)}
			}

			return fmt.Errorf("reading audit sequence %d: %w", *expectedAuditSequence, nextErr)
		}
		if entry.GetSequence() != *expectedAuditSequence {
			return &ErrSourceMissing{Detail: fmt.Sprintf(
				"expected audit sequence %d, first available sequence is %d",
				*expectedAuditSequence,
				entry.GetSequence(),
			)}
		}

		proposal, maxLogSequence, verifyErr := readProposalHeader(ctx, reader, entry)
		if verifyErr != nil {
			return verifyErr
		}
		if maxLogSequence > 0 {
			minLogSequence := entry.GetSuccess().GetMinLogSequence()
			if minLogSequence != next.LogSequence+1 {
				return &ErrSourceMissing{Detail: fmt.Sprintf(
					"audit sequence %d starts fresh log range at %d after watermark %d",
					entry.GetSequence(),
					minLogSequence,
					next.LogSequence,
				)}
			}
			next.LogSequence = maxLogSequence
		}
		*proposals = append(*proposals, proposal)
		next.AuditSequence = entry.GetSequence()
		next.AuditHash = append(next.AuditHash[:0], entry.GetHash()...)
		*expectedAuditSequence++
	}

	return nil
}

func readProposalHeader(
	ctx context.Context,
	reader dal.PebbleReader,
	entry *auditpb.AuditEntry,
) (VerifiedProposal, uint64, error) {
	auditSequence := entry.GetSequence()
	if len(entry.GetItems()) != 0 {
		return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d embeds %d items in its header",
			auditSequence,
			len(entry.GetItems()),
		)}
	}
	if entry.GetSuccess() == nil && entry.GetFailure() == nil {
		return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d has no outcome",
			auditSequence,
		)}
	}

	items, err := query.ReadAuditItems(ctx, reader, auditSequence)
	if err != nil {
		return VerifiedProposal{}, 0, fmt.Errorf("reading audit items for sequence %d: %w", auditSequence, err)
	}
	if uint32(len(items)) != entry.GetOrderCount() {
		return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
			"audit sequence %d declares %d items but %d are available",
			auditSequence,
			entry.GetOrderCount(),
			len(items),
		)}
	}

	logs := make([]*commonpb.Log, len(items))
	var minLogSequence, maxLogSequence uint64
	if success := entry.GetSuccess(); success != nil {
		minLogSequence = success.GetMinLogSequence()
		maxLogSequence = success.GetMaxLogSequence()
		if (minLogSequence == 0) != (maxLogSequence == 0) {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d has partial fresh log range [%d,%d]",
				auditSequence,
				minLogSequence,
				maxLogSequence,
			)}
		}
		if minLogSequence > maxLogSequence {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d has descending fresh log range [%d,%d]",
				auditSequence,
				minLogSequence,
				maxLogSequence,
			)}
		}
	}
	freshLogs := make(map[uint64]struct{})
	for index, item := range items {
		if item == nil {
			return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d is missing item %d",
				auditSequence,
				index,
			)}
		}
		if item.GetOrderIndex() != uint32(index) {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d item at position %d declares order index %d",
				auditSequence,
				index,
				item.GetOrderIndex(),
			)}
		}

		logSequence := item.GetLogSequence()
		if logSequence == 0 {
			continue
		}
		if entry.GetFailure() != nil {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"failed audit sequence %d item %d references log %d",
				auditSequence,
				index,
				logSequence,
			)}
		}
		if maxLogSequence == 0 || logSequence > maxLogSequence {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d item %d references log %d beyond fresh range maximum %d",
				auditSequence,
				index,
				logSequence,
				maxLogSequence,
			)}
		}
		if logSequence >= minLogSequence {
			if _, exists := freshLogs[logSequence]; exists {
				return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d references fresh log %d more than once",
					auditSequence,
					logSequence,
				)}
			}
			freshLogs[logSequence] = struct{}{}
		}
	}
	if minLogSequence > 0 {
		expectedFreshLogs := maxLogSequence - minLogSequence + 1
		for offset := range expectedFreshLogs {
			sequence := minLogSequence + offset
			if _, exists := freshLogs[sequence]; !exists {
				return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
					"audit sequence %d is missing fresh log %d from range [%d,%d]",
					auditSequence,
					sequence,
					minLogSequence,
					maxLogSequence,
				)}
			}
		}
	}

	return VerifiedProposal{Entry: entry, Items: items, Logs: logs}, maxLogSequence, nil
}

type logSlot struct {
	proposal int
	item     int
}

func (s *HotColdSource) resolveProposalLogs(
	ctx context.Context,
	snapshot *hotColdSnapshot,
	proposals []VerifiedProposal,
) error {
	sequences, slots, remaining := planRequestedLogs(proposals)
	if len(sequences) == 0 {
		return nil
	}
	for _, chapter := range snapshot.archived {
		start := lowerBoundIndex(sequences, chapter.GetStartSequence())
		end := upperBoundIndex(sequences, chapter.GetCloseSequence())
		if start == end {
			continue
		}

		reader, release, err := s.archiveReader(ctx, chapter)
		if err != nil {
			return err
		}
		scanErr := func() (err error) {
			defer func() {
				err = errors.Join(err, release())
			}()

			return scanRequestedLogs(
				ctx,
				reader,
				sequences[start:end],
				proposals,
				slots,
				remaining,
			)
		}()
		if scanErr != nil {
			return fmt.Errorf("scanning logs from archived chapter %d: %w", chapter.GetId(), scanErr)
		}
	}

	if len(remaining) > 0 {
		unresolved := make([]uint64, 0, len(remaining))
		for sequence := range remaining {
			unresolved = append(unresolved, sequence)
		}
		slices.Sort(unresolved)
		if err := scanRequestedLogs(ctx, snapshot.hot, unresolved, proposals, slots, remaining); err != nil {
			return fmt.Errorf("scanning logs from hot tail: %w", err)
		}
	}

	return requireAllRequestedLogs(remaining)
}

func resolveProposalLogsFromReader(
	ctx context.Context,
	reader dal.PebbleReader,
	proposals []VerifiedProposal,
) error {
	sequences, slots, remaining := planRequestedLogs(proposals)
	if len(sequences) == 0 {
		return nil
	}
	if err := scanRequestedLogs(ctx, reader, sequences, proposals, slots, remaining); err != nil {
		return err
	}

	return requireAllRequestedLogs(remaining)
}

func planRequestedLogs(proposals []VerifiedProposal) (
	[]uint64,
	map[uint64][]logSlot,
	map[uint64]struct{},
) {
	slots := make(map[uint64][]logSlot)
	for proposalIndex := range proposals {
		for itemIndex, item := range proposals[proposalIndex].Items {
			if sequence := item.GetLogSequence(); sequence > 0 {
				slots[sequence] = append(slots[sequence], logSlot{proposal: proposalIndex, item: itemIndex})
			}
		}
	}
	sequences := make([]uint64, 0, len(slots))
	remaining := make(map[uint64]struct{}, len(slots))
	for sequence := range slots {
		sequences = append(sequences, sequence)
		remaining[sequence] = struct{}{}
	}
	slices.Sort(sequences)

	return sequences, slots, remaining
}

func requireAllRequestedLogs(remaining map[uint64]struct{}) error {
	if len(remaining) == 0 {
		return nil
	}
	missing := make([]uint64, 0, len(remaining))
	for sequence := range remaining {
		missing = append(missing, sequence)
	}
	slices.Sort(missing)

	return &ErrSourceMissing{Detail: fmt.Sprintf("referenced logs are missing: %v", missing)}
}

func lowerBoundIndex(sequences []uint64, value uint64) int {
	return sort.Search(len(sequences), func(index int) bool { return sequences[index] >= value })
}

func upperBoundIndex(sequences []uint64, value uint64) int {
	return sort.Search(len(sequences), func(index int) bool { return sequences[index] > value })
}

func scanRequestedLogs(
	ctx context.Context,
	reader dal.PebbleReader,
	requested []uint64,
	proposals []VerifiedProposal,
	slots map[uint64][]logSlot,
	remaining map[uint64]struct{},
) (err error) {
	if len(requested) == 0 {
		return nil
	}
	for start := 0; start < len(requested); {
		end := start + 1
		for end < len(requested) && requested[end-1] != ^uint64(0) && requested[end] == requested[end-1]+1 {
			end++
		}
		if err := scanRequestedLogRange(ctx, reader, requested[start:end], proposals, slots, remaining); err != nil {
			return err
		}
		start = end
	}

	return nil
}

func scanRequestedLogRange(
	ctx context.Context,
	reader dal.PebbleReader,
	requested []uint64,
	proposals []VerifiedProposal,
	slots map[uint64][]logSlot,
	remaining map[uint64]struct{},
) (err error) {
	start := requested[0]
	end := requested[len(requested)-1]
	after := uint64(0)
	if start > 0 {
		after = start - 1
	}
	logs, err := query.ReadLogsSinceRaw(ctx, reader, after)
	if err != nil {
		return fmt.Errorf("opening log scan at %d: %w", start, err)
	}
	defer func() {
		if closeErr := logs.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing log scan: %w", closeErr))
		}
	}()

	logPrefix := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).Build()
	for valid := logs.First(); valid; valid = logs.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := logs.Key()
		if len(key) != len(logPrefix)+8 || !bytes.HasPrefix(key, logPrefix) {
			return &ErrSourceInvalid{Detail: fmt.Sprintf("hot/cold log scan returned malformed key %x", key)}
		}
		sequence := binary.BigEndian.Uint64(key[len(logPrefix):])
		if sequence > end {
			break
		}
		if _, needed := remaining[sequence]; !needed {
			continue
		}
		var log commonpb.Log
		if err := log.UnmarshalVT(logs.Value()); err != nil {
			return &ErrSourceInvalid{Detail: fmt.Sprintf("decoding referenced log %d: %v", sequence, err)}
		}
		if log.GetSequence() != sequence {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"referenced log key %d contains payload sequence %d",
				sequence,
				log.GetSequence(),
			)}
		}
		for _, slot := range slots[sequence] {
			proposals[slot.proposal].Logs[slot.item] = log.CloneVT()
		}
		delete(remaining, sequence)
	}
	if err := logs.Error(); err != nil {
		return fmt.Errorf("reading log scan: %w", err)
	}

	return nil
}

func (s *HotColdSource) archiveReader(
	ctx context.Context,
	chapter *commonpb.Chapter,
) (dal.PebbleReader, func() error, error) {
	if s.coldReader == nil || s.bucketID == "" {
		return nil, nil, &ErrSourceMissing{Detail: fmt.Sprintf(
			"archived chapter %d has no configured cold source",
			chapter.GetId(),
		)}
	}
	s.verifiedMu.Lock()
	defer s.verifiedMu.Unlock()

	knownReader := s.verifiedReaders[chapter.GetId()]
	reader, release, err := s.coldReader.AcquireReader(ctx, chapter.GetId())
	if err != nil {
		return nil, nil, &ErrSourceMissing{Detail: fmt.Sprintf("opening archived chapter %d: %v", chapter.GetId(), err)}
	}
	if knownReader == reader {
		return reader, release, nil
	}
	// A new reader represents either the first fetch or a re-fetch after cache
	// eviction. Validate the archive structure before trusting the local DB.
	if err := verifyArchiveContents(ctx, reader, chapter); err != nil {
		err = errors.Join(err, release())

		return nil, nil, err
	}
	s.verifiedReaders[chapter.GetId()] = reader

	return reader, release, nil
}

type archiveMetadata struct {
	ChapterID          uint64 `json:"chapterId"`
	StartSequence      uint64 `json:"startSequence"`
	CloseSequence      uint64 `json:"closeSequence"`
	StartAuditSequence uint64 `json:"startAuditSequence"`
	CloseAuditSequence uint64 `json:"closeAuditSequence"`
}

func verifyArchiveContents(
	ctx context.Context,
	reader dal.PebbleReader,
	chapter *commonpb.Chapter,
) error {
	metadataBytes, closer, err := reader.Get(state.MetadataKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return &ErrSourceMissing{Detail: fmt.Sprintf("archived chapter %d has no metadata", chapter.GetId())}
		}

		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"reading archived chapter %d metadata: %v",
			chapter.GetId(),
			err,
		)}
	}
	metadataCopy := append([]byte(nil), metadataBytes...)
	if closeErr := closer.Close(); closeErr != nil {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"closing archived chapter %d metadata: %v",
			chapter.GetId(),
			closeErr,
		)}
	}
	metadata := archiveMetadata{}
	if err := json.Unmarshal(metadataCopy, &metadata); err != nil {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"decoding archived chapter %d metadata: %v",
			chapter.GetId(),
			err,
		)}
	}
	want := archiveMetadata{
		ChapterID:          chapter.GetId(),
		StartSequence:      chapter.GetStartSequence(),
		CloseSequence:      chapter.GetCloseSequence(),
		StartAuditSequence: chapter.GetStartAuditSequence(),
		CloseAuditSequence: chapter.GetCloseAuditSequence(),
	}
	if metadata != want {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"archived chapter %d metadata %+v differs from registry %+v",
			chapter.GetId(),
			metadata,
			want,
		)}
	}

	if err := verifyArchiveLogBounds(ctx, reader, chapter); err != nil {
		return err
	}
	if err := verifyArchiveAuditBounds(ctx, reader, chapter); err != nil {
		return err
	}

	return nil
}

func verifyArchiveLogBounds(
	ctx context.Context,
	reader dal.PebbleReader,
	chapter *commonpb.Chapter,
) (err error) {
	logs, err := query.ReadLogsSince(ctx, reader, chapter.GetStartSequence()-1)
	if err != nil {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"opening archived chapter %d log bounds: %v",
			chapter.GetId(),
			err,
		)}
	}
	defer func() {
		if closeErr := logs.Close(); closeErr != nil {
			err = errors.Join(err, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"closing archived chapter %d log bounds: %v",
				chapter.GetId(),
				closeErr,
			)})
		}
	}()
	first, err := logs.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return &ErrSourceMissing{Detail: fmt.Sprintf("archived chapter %d contains no logs", chapter.GetId())}
		}

		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"reading archived chapter %d first log: %v",
			chapter.GetId(),
			err,
		)}
	}
	if first.GetSequence() != chapter.GetStartSequence() {
		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"archived chapter %d first log is %d, want %d",
			chapter.GetId(),
			first.GetSequence(),
			chapter.GetStartSequence(),
		)}
	}
	last, err := query.ReadLastLog(reader)
	if err != nil {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"reading archived chapter %d last log: %v",
			chapter.GetId(),
			err,
		)}
	}
	if last == nil || last.GetSequence() != chapter.GetCloseSequence() {
		lastSequence := uint64(0)
		if last != nil {
			lastSequence = last.GetSequence()
		}

		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"archived chapter %d last log is %d, want %d",
			chapter.GetId(),
			lastSequence,
			chapter.GetCloseSequence(),
		)}
	}

	return nil
}

func verifyArchiveAuditBounds(
	ctx context.Context,
	reader dal.PebbleReader,
	chapter *commonpb.Chapter,
) (err error) {
	if chapter.GetCloseAuditSequence()+1 == chapter.GetStartAuditSequence() {
		last, readErr := query.ReadLastAuditEntry(reader)
		if readErr != nil {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"reading empty archived chapter %d audit range: %v",
				chapter.GetId(),
				readErr,
			)}
		}
		if last != nil {
			return &ErrSourceInvalid{Detail: fmt.Sprintf(
				"archived chapter %d declares an empty audit range but contains sequence %d",
				chapter.GetId(),
				last.GetSequence(),
			)}
		}

		return nil
	}

	before := chapter.GetStartAuditSequence() - 1
	entries, err := query.ReadAuditEntries(ctx, reader, &before)
	if err != nil {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"opening archived chapter %d audit bounds: %v",
			chapter.GetId(),
			err,
		)}
	}
	defer func() {
		if closeErr := entries.Close(); closeErr != nil {
			err = errors.Join(err, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"closing archived chapter %d audit bounds: %v",
				chapter.GetId(),
				closeErr,
			)})
		}
	}()
	first, err := entries.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return &ErrSourceMissing{Detail: fmt.Sprintf("archived chapter %d contains no audit entries", chapter.GetId())}
		}

		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"reading archived chapter %d first audit entry: %v",
			chapter.GetId(),
			err,
		)}
	}
	if first.GetSequence() != chapter.GetStartAuditSequence() {
		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"archived chapter %d first audit sequence is %d, want %d",
			chapter.GetId(),
			first.GetSequence(),
			chapter.GetStartAuditSequence(),
		)}
	}
	last, err := query.ReadLastAuditEntry(reader)
	if err != nil {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"reading archived chapter %d last audit entry: %v",
			chapter.GetId(),
			err,
		)}
	}
	if last == nil || last.GetSequence() != chapter.GetCloseAuditSequence() {
		lastSequence := uint64(0)
		if last != nil {
			lastSequence = last.GetSequence()
		}

		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"archived chapter %d last audit sequence is %d, want %d",
			chapter.GetId(),
			lastSequence,
			chapter.GetCloseAuditSequence(),
		)}
	}
	if !bytes.Equal(last.GetHash(), chapter.GetLastAuditHash()) {
		return &ErrSourceInvalid{Detail: fmt.Sprintf(
			"archived chapter %d last audit hash %x differs from registry %x",
			chapter.GetId(),
			last.GetHash(),
			chapter.GetLastAuditHash(),
		)}
	}

	return nil
}
