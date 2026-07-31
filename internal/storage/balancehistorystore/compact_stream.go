package balancehistorystore

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/cockroachdb/pebble/v2"
)

type compactionStreamer struct {
	*viewManager
}

const compactionBatchBytes = 4 << 20

func (s *compactionStreamer) openCompactionView(ctx context.Context) (*View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mutationMu.Lock()
	if err := s.ensureNotQuarantined(); err != nil {
		s.mutationMu.Unlock()

		return nil, err
	}
	snapshot := s.db.NewSnapshot()
	manifest, err := readManifest(snapshot)
	if err != nil {
		_ = snapshot.Close()
		s.mutationMu.Unlock()

		return nil, err
	}
	s.acquireManifestLease(manifest)
	tiering := s.tiering.Load()
	view := &View{
		ctx:                ctx,
		store:              s.viewManager,
		snapshot:           snapshot,
		manifest:           cloneManifest(manifest),
		generation:         s.generation.Load(),
		allowSourceMissing: false,
		coldRuns:           make(map[uint64]*coldRunView),
	}
	s.mutationMu.Unlock()

	for _, run := range manifest.Runs {
		if !run.LocalRemoved {
			continue
		}
		if tiering == nil {
			_ = view.Close()

			return nil, &ErrSourceMissing{Detail: fmt.Sprintf("cold run %d cannot be compacted without an archive", run.ID)}
		}
		cold := &coldRunView{archive: tiering.archive, parts: make([]coldPartView, len(run.ArchiveParts))}
		for index, part := range run.ArchiveParts {
			cold.parts[index].meta = cloneArchiveParts([]ArchivePart{part})[0]
		}
		view.coldRuns[run.ID] = cold
	}

	return view, nil
}

func (s *compactionStreamer) reserveCompactionRun(
	expected []RunRef,
	generation uint64,
) (uint64, bool, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.ensureNotQuarantined(); err != nil {
		return 0, false, err
	}
	if generation != s.generation.Load() {
		return 0, false, nil
	}
	current, err := readManifest(s.db)
	if err != nil {
		return 0, false, err
	}
	if !compactionInputsPresent(current.Runs, expected) {
		return 0, false, nil
	}

	runID := current.NextRunID
	if runID == 0 {
		runID = 1
	}
	if runID == ^uint64(0) {
		return 0, false, errors.New("balance history run id space exhausted")
	}

	next := cloneManifest(current)
	next.Version++
	next.NextRunID = runID + 1
	encodedManifest, err := encodeManifest(next)
	if err != nil {
		return 0, false, err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(manifestKey(next.Version), encodedManifest, nil); err != nil {
		return 0, false, fmt.Errorf("staging compaction run reservation: %w", err)
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], next.Version)
	if err := batch.Set(latestManifestKey(), version[:], nil); err != nil {
		return 0, false, fmt.Errorf("staging compaction reservation pointer: %w", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return 0, false, fmt.Errorf("committing compaction run reservation: %w", err)
	}
	s.leaseMu.Lock()
	if s.preparedRuns[runID] != 0 {
		s.leaseMu.Unlock()

		return 0, false, fmt.Errorf("invariant: compaction run %d was reserved twice", runID)
	}
	s.preparedRuns[runID] = 1
	s.leaseMu.Unlock()
	s.signalChanged()

	return runID, true, nil
}

func (s *compactionStreamer) streamCompactedRun(
	ctx context.Context,
	view *View,
	runs []RunRef,
	runID uint64,
) (RunRef, error) {
	cursors := make([]*semanticRunCursor, 0, len(runs))
	defer func() {
		for _, cursor := range cursors {
			_ = cursor.Close()
		}
	}()

	queue := make(semanticCursorHeap, 0, len(runs))
	for _, run := range runs {
		cursor, err := view.newSemanticRunCursor(ctx, run.ID)
		if err != nil {
			return RunRef{}, err
		}
		cursors = append(cursors, cursor)
		valid, err := cursor.Advance(ctx)
		if err != nil {
			return RunRef{}, err
		}
		if valid {
			heap.Push(&queue, cursor)
		}
	}

	writer := newCompactionRunWriter(s.db, runID)
	defer func() { _ = writer.Close() }()
	for queue.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return RunRef{}, err
		}

		cursor := heap.Pop(&queue).(*semanticRunCursor)
		key := bytes.Clone(cursor.key)
		input := new(big.Int).Set(cursor.input)
		output := new(big.Int).Set(cursor.output)
		if err := advanceSemanticCursor(ctx, &queue, cursor); err != nil {
			return RunRef{}, err
		}
		for queue.Len() > 0 && bytes.Equal(queue[0].key, key) {
			duplicate := heap.Pop(&queue).(*semanticRunCursor)
			input.Add(input, duplicate.input)
			output.Add(output, duplicate.output)
			if err := advanceSemanticCursor(ctx, &queue, duplicate); err != nil {
				return RunRef{}, err
			}
		}
		if input.Sign() == 0 && output.Sign() == 0 {
			continue
		}
		identity, timestamp, err := decodeSemanticDataKey(key)
		if err != nil {
			return RunRef{}, err
		}
		if err := writer.Add(identity, timestamp, input, output); err != nil {
			return RunRef{}, err
		}
	}
	if err := writer.Close(); err != nil {
		return RunRef{}, err
	}

	snapshot := s.db.NewSnapshot()
	checksum, entries, identities, verifyErr := verifyRunRecordsContext(ctx, snapshot, runID)
	closeErr := snapshot.Close()
	if err := errors.Join(verifyErr, closeErr); err != nil {
		return RunRef{}, err
	}
	if entries != writer.entryCount || identities != writer.identityCount {
		return RunRef{}, &ErrCorrupt{Detail: fmt.Sprintf(
			"streamed compacted run %d count mismatch (entries=%d/%d identities=%d/%d)",
			runID,
			entries,
			writer.entryCount,
			identities,
			writer.identityCount,
		)}
	}

	merged := RunRef{
		ID:                 runID,
		Level:              runs[0].Level + 1,
		FirstAuditSequence: runs[0].FirstAuditSequence,
		LastAuditSequence:  runs[0].LastAuditSequence,
		MaxLogSequence:     runs[0].MaxLogSequence,
		EntryCount:         entries,
		IdentityCount:      identities,
		Checksum:           checksum,
	}
	for _, run := range runs[1:] {
		merged.FirstAuditSequence = min(merged.FirstAuditSequence, run.FirstAuditSequence)
		merged.LastAuditSequence = max(merged.LastAuditSequence, run.LastAuditSequence)
		merged.MaxLogSequence = max(merged.MaxLogSequence, run.MaxLogSequence)
	}

	return merged, nil
}

func decodeSemanticDataKey(key []byte) (recordIdentity, uint64, error) {
	physical := append(runPrefix(prefixRunData, 0), key...)
	_, identity, timestamp, err := decodeDataKey(physical)
	if err != nil {
		return recordIdentity{}, 0, &ErrCorrupt{Detail: fmt.Sprintf("decoding compacted semantic key: %v", err)}
	}

	return identity, timestamp, nil
}

type compactionRunWriter struct {
	db            *pebble.DB
	runID         uint64
	batch         *pebble.Batch
	identity      recordIdentity
	cumulative    cumulativeValue
	hasIdentity   bool
	entryCount    uint64
	identityCount uint64
	closed        bool
}

func newCompactionRunWriter(db *pebble.DB, runID uint64) *compactionRunWriter {
	return &compactionRunWriter{db: db, runID: runID, batch: db.NewBatch()}
}

func (w *compactionRunWriter) Add(
	identity recordIdentity,
	timestamp uint64,
	input, output *big.Int,
) error {
	if !w.hasIdentity || identity != w.identity {
		w.identity = identity
		w.cumulative = newCumulativeValue()
		w.hasIdentity = true
		catalog, err := catalogKey(w.runID, identity)
		if err != nil {
			return err
		}
		if err := w.batch.Set(catalog, nil, nil); err != nil {
			return fmt.Errorf("staging streamed compacted catalog: %w", err)
		}
		w.identityCount++
	}
	w.cumulative.add(input, output)
	key, err := dataKey(w.runID, identity, timestamp)
	if err != nil {
		return err
	}
	if err := w.batch.Set(key, encodeCumulative(w.cumulative), nil); err != nil {
		return fmt.Errorf("staging streamed compacted data: %w", err)
	}
	w.entryCount++
	if w.batch.Len() >= compactionBatchBytes {
		return w.flush()
	}

	return nil
}

func (w *compactionRunWriter) flush() error {
	if w.batch == nil || w.batch.Count() == 0 {
		return nil
	}
	if err := w.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing streamed compacted run chunk: %w", err)
	}
	if err := w.batch.Close(); err != nil {
		return fmt.Errorf("closing streamed compacted run chunk: %w", err)
	}
	w.batch = w.db.NewBatch()

	return nil
}

func (w *compactionRunWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	flushErr := w.flush()
	var closeErr error
	if w.batch != nil {
		closeErr = w.batch.Close()
		w.batch = nil
	}

	return errors.Join(flushErr, closeErr)
}

func (s *compactionStreamer) publishPreparedCompaction(
	expected []RunRef,
	merged RunRef,
	generation uint64,
) (bool, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.ensureNotQuarantined(); err != nil {
		return false, err
	}
	if generation != s.generation.Load() {
		return false, nil
	}
	current, err := readManifest(s.db)
	if err != nil {
		return false, err
	}
	if !compactionInputsPresent(current.Runs, expected) {
		return false, nil
	}

	selectedIDs := make(map[uint64]struct{}, len(expected))
	for _, run := range expected {
		selectedIDs[run.ID] = struct{}{}
	}
	next := cloneManifest(current)
	next.Version++
	next.Runs = make([]RunRef, 0, len(current.Runs)-len(expected)+1)
	for _, run := range current.Runs {
		if _, selected := selectedIDs[run.ID]; !selected {
			next.Runs = append(next.Runs, run)
		}
	}
	next.Runs = append(next.Runs, merged)
	sort.Slice(next.Runs, func(i, j int) bool {
		if next.Runs[i].Level != next.Runs[j].Level {
			return next.Runs[i].Level < next.Runs[j].Level
		}

		return next.Runs[i].ID < next.Runs[j].ID
	})
	encodedManifest, err := encodeManifest(next)
	if err != nil {
		return false, err
	}
	encodedRun, err := json.Marshal(merged)
	if err != nil {
		return false, fmt.Errorf("marshaling streamed compacted run: %w", err)
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(runMetaKey(merged.ID), encodedRun, nil); err != nil {
		return false, fmt.Errorf("staging streamed compacted metadata: %w", err)
	}
	if err := batch.Set(manifestKey(next.Version), encodedManifest, nil); err != nil {
		return false, fmt.Errorf("staging streamed compacted manifest: %w", err)
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], next.Version)
	if err := batch.Set(latestManifestKey(), version[:], nil); err != nil {
		return false, fmt.Errorf("staging streamed compacted manifest pointer: %w", err)
	}
	// WAL ordering is the atomicity boundary: recovery can only replay this
	// manifest after the reservation and every preceding prepared-run chunk.
	// The builder's periodic SyncWAL supplies the durability barrier.
	if err := batch.Commit(pebble.NoSync); err != nil {
		return false, fmt.Errorf("publishing streamed compacted run: %w", err)
	}
	if err := s.releasePreparedRun(merged.ID); err != nil {
		return false, err
	}
	s.signalChanged()

	return true, nil
}

func (s *compactionStreamer) discardPreparedRun(runID uint64) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	current, err := readManifest(s.db)
	if err != nil {
		return err
	}
	for _, run := range current.Runs {
		if run.ID == runID {
			return s.releasePreparedRun(runID)
		}
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
		prefix := runPrefix(kind, runID)
		if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return err
		}
	}
	if err := batch.Delete(runMetaKey(runID), nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	return s.releasePreparedRun(runID)
}

func (s *compactionStreamer) releasePreparedRun(runID uint64) error {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	if s.preparedRuns[runID] == 0 {
		return fmt.Errorf("invariant: prepared run %d was released without a reservation", runID)
	}
	delete(s.preparedRuns, runID)

	return nil
}
