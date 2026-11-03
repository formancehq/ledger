package wal

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// errReplayRebuild reports a WAL history that the exact rebuild cannot
// reconcile with etcd's ReadAll result. Startup fails closed on it.
var errReplayRebuild = errors.New("rebuilding the replayed WAL log")

// logID identifies one log position by index and term.
type logID struct {
	index uint64
	term  uint64
}

// replayRecord is the part of one WAL record the rebuild needs.
type replayRecord struct {
	kind     int64
	entry    logID
	snapshot logID
	// commit is the HardState commit index of a StateType record.
	commit uint64
}

// replayLog is the live Raft log reconstructed from WAL records in write order.
//
// It mirrors the in-memory DefaultWAL log rather than etcd's ReadAll filter:
//
//   - an entry record at index i drops every retained entry at index >= i,
//     whatever the replay snapshot index (ReadAll ignores records at or below
//     the replay snapshot, so it keeps tails those records truncated);
//   - a committed snapshot record that does not match the rebuilt log was
//     received from a leader. Raft installs a received snapshot only when the
//     log does not already hold its (index, term), and DefaultWAL.ApplySnapshot
//     then discards the whole log without writing entry records. The rebuild
//     discards it too, except the entries the same Ready persisted just
//     before the snapshot records (see suffixCandidate).
type replayLog struct {
	// entries is the contiguous retained log; entries[0].index is base.index+1.
	entries []logID
	// base is the position just before entries[0]. baseKnown reports whether
	// its term is known (it came from a snapshot record).
	base      logID
	baseKnown bool

	// seenSnapshots holds every committed snapshot (index, term) already
	// applied to the rebuild, so a repeated record (barrier, ConfState
	// update) matches even after the entry itself is gone.
	seenSnapshots map[logID]struct{}

	// run is the entries written since the last Save call's HardState record.
	run []logID
	// suffixCandidate is the run closed by the last Save call, valid while no
	// entry or further Save call follows it.
	suffixCandidate []logID
	// afterCut is set by a segment metadata record. etcd rewrites the current
	// HardState right after it when cutting a segment; that record is not a
	// Save call and must not close the run.
	afterCut bool
}

func newReplayLog() *replayLog {
	return &replayLog{seenSnapshots: map[logID]struct{}{}}
}

func (l *replayLog) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.base.index
	}

	return l.entries[len(l.entries)-1].index
}

// termAt returns the term at index when the rebuild knows it.
func (l *replayLog) termAt(index uint64) (uint64, bool) {
	if l.baseKnown && index == l.base.index {
		return l.base.term, true
	}
	if len(l.entries) == 0 || index <= l.base.index || index > l.lastIndex() {
		return 0, false
	}

	return l.entries[index-l.base.index-1].term, true
}

func (l *replayLog) appendEntry(id logID) {
	switch {
	case len(l.entries) > 0 && id.index > l.base.index && id.index <= l.lastIndex()+1:
		// Overwrite or contiguous append inside the known window.
		l.entries = append(l.entries[:id.index-l.base.index-1:id.index-l.base.index-1], id)
	case len(l.entries) == 0 && id.index == l.base.index+1:
		l.entries = append(l.entries, id)
	default:
		// The record lies at or below the base, or leaves a gap: everything
		// from id.index on is replaced and nothing before it is known.
		l.entries = []logID{id}
		l.base = logID{index: id.index - 1}
		l.baseKnown = false
	}
	l.run = append(l.run, id)
	l.suffixCandidate = nil
}

// applyState closes the current Save call. etcd's Save writes the call's
// entries, then one HardState record, so every HardState record outside a
// segment header marks a Save boundary.
func (l *replayLog) applyState() {
	if l.afterCut {
		l.afterCut = false

		return
	}
	l.suffixCandidate = l.run
	l.run = nil
}

// applySnapshot applies a committed snapshot record.
func (l *replayLog) applySnapshot(id logID) {
	if id.index == 0 {
		return
	}
	if _, seen := l.seenSnapshots[id]; seen {
		return
	}
	l.seenSnapshots[id] = struct{}{}

	if term, known := l.termAt(id.index); known {
		if term == id.term {
			// Local snapshot (CreateSnapshot) of a position the log holds:
			// the retained entries are unchanged.
			return
		}
	} else if id.index <= l.lastIndex() {
		// The position predates what the rebuild can see. Keep the log, as
		// ReadAll does; the term checks that follow the rebuild still apply.
		return
	}

	// Received snapshot: the log did not hold (index, term). Keep the run the
	// same Ready persisted just before installation when it extends the
	// snapshot; it is written again after installation, and a crash before
	// that rewrite must not lose entries a durable HardState may commit.
	kept := l.suffixCandidate
	if len(kept) == 0 || kept[0].index != id.index+1 || kept[0].term < id.term || !contiguous(kept) {
		kept = nil
	}
	l.entries = slices.Clone(kept)
	l.base = id
	l.baseKnown = true
	l.run = nil
	l.suffixCandidate = nil
}

func contiguous(ids []logID) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i].index != ids[i-1].index+1 {
			return false
		}
	}

	return true
}

// rebuildReplayLog replays records in write order and returns the log
// positions above the replay snapshot.
//
// A snapshot record counts only once its HardState is durable: the next entry
// or HardState record must be a HardState whose commit reaches the snapshot
// index. ApplySnapshot writes exactly that record between its guard and
// barrier records, and CreateSnapshot runs on a position already committed.
// A guard left by a crash before its HardState was written is followed by the
// restarted process's records instead, and is ignored even if the commit index
// later passes it; the log the node resumed with stays valid. commit is the
// final durable commit index; like etcd's ValidSnapshotEntries, snapshot
// records above it never count.
func rebuildReplayLog(records []replayRecord, replay logID, commit uint64) ([]logID, error) {
	l := newReplayLog()
	var pending *logID
	for _, rec := range records {
		if rec.kind != wal.StateType && rec.kind != wal.MetadataType {
			l.afterCut = false
		}
		switch rec.kind {
		case wal.MetadataType:
			l.afterCut = true
		case wal.EntryType:
			pending = nil
			l.appendEntry(rec.entry)
		case wal.StateType:
			if pending != nil && !l.afterCut {
				if rec.commit >= pending.index {
					l.applySnapshot(*pending)
				}
				pending = nil
			}
			l.applyState()
		case wal.SnapshotType:
			pending = nil
			if rec.snapshot.index <= commit {
				id := rec.snapshot
				pending = &id
			}
		}
	}

	if l.lastIndex() <= replay.index {
		return nil, nil
	}
	if l.base.index > replay.index {
		return nil, fmt.Errorf("%w: rebuilt log starts after index %d, above the replay snapshot %d",
			errReplayRebuild, l.base.index, replay.index)
	}

	return slices.Clone(l.entries[replay.index-l.base.index:]), nil
}

// readReplayRecords decodes every retained WAL segment in write order.
//
// It reads the same files etcd's ValidSnapshotEntries does, opened read-only,
// and validates the CRC chain the same way. A torn tail ends the stream like a
// clean end: New runs this after ReadAll has repaired the WAL.
func readReplayRecords(walDir string) ([]replayRecord, error) {
	names, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return nil, fmt.Errorf("listing WAL segments: %w", err)
	}
	// etcd names segments %016x-%016x.wal, so a lexical sort is chronological.
	slices.Sort(names)

	readers := make([]fileutil.FileReader, 0, len(names))
	files := make([]*os.File, 0, len(names))
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	for _, name := range names {
		f, err := os.OpenFile(name, os.O_RDONLY, 0)
		if err != nil {
			return nil, fmt.Errorf("opening WAL segment %q: %w", name, err)
		}
		files = append(files, f)
		readers = append(readers, fileutil.NewFileReader(f))
	}

	decoder := wal.NewDecoder(readers...)
	var records []replayRecord
	rec := &walpb.Record{}
	for err = decoder.Decode(rec); err == nil; err = decoder.Decode(rec) {
		switch rec.GetType() {
		case wal.EntryType:
			var e raftpb.Entry
			if err := proto.Unmarshal(rec.GetData(), &e); err != nil {
				return nil, fmt.Errorf("decoding WAL entry record: %w", err)
			}
			records = append(records, replayRecord{kind: wal.EntryType, entry: logID{index: e.GetIndex(), term: e.GetTerm()}})
		case wal.StateType:
			var hs raftpb.HardState
			if err := proto.Unmarshal(rec.GetData(), &hs); err != nil {
				return nil, fmt.Errorf("decoding WAL HardState record: %w", err)
			}
			records = append(records, replayRecord{kind: wal.StateType, commit: hs.GetCommit()})
		case wal.MetadataType:
			records = append(records, replayRecord{kind: wal.MetadataType})
		case wal.SnapshotType:
			var snap walpb.Snapshot
			if err := proto.Unmarshal(rec.GetData(), &snap); err != nil {
				return nil, fmt.Errorf("decoding WAL snapshot record: %w", err)
			}
			records = append(records, replayRecord{kind: wal.SnapshotType, snapshot: logID{index: snap.GetIndex(), term: snap.GetTerm()}})
		case wal.CrcType:
			crc := decoder.LastCRC()
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, fmt.Errorf("reading WAL segments: %w", wal.ErrCRCMismatch)
			}
			decoder.UpdateCRC(rec.GetCrc())
		}
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("reading WAL segments: %w", err)
	}

	return records, nil
}

// reconcileReplayedEntries returns the live Raft log above the replay
// snapshot, starting from the entries etcd's ReadAll returned.
//
// The order is:
//
//  1. the raw WAL records are rebuilt in write order (rebuildReplayLog);
//  2. the ReadAll entries are trimmed to the rebuilt log, which may only
//     remove a suffix;
//  3. a removed suffix starting at or below HardState.Commit fails closed;
//  4. discardStaleReplayTail runs on the result as a backstop. It should find
//     nothing; if it does, the suffix is still dropped above the commit index.
func reconcileReplayedEntries(
	walDir string,
	snapshot *raftpb.Snapshot,
	hardState *raftpb.HardState,
	readAll []*raftpb.Entry,
	logger logging.Logger,
) ([]*raftpb.Entry, error) {
	replay := logID{index: snapshot.GetMetadata().GetIndex(), term: snapshot.GetMetadata().GetTerm()}
	commit := hardState.GetCommit()
	fields := map[string]any{
		"snapshotIndex": replay.index,
		"snapshotTerm":  replay.term,
		"commit":        commit,
	}

	records, err := readReplayRecords(walDir)
	if err != nil {
		return nil, err
	}
	rebuilt, err := rebuildReplayLog(records, replay, commit)
	if err != nil {
		return nil, err
	}
	entries, err := trimToRebuiltLog(readAll, rebuilt)
	if err != nil {
		return nil, err
	}

	if dropped := len(readAll) - len(entries); dropped > 0 {
		first := readAll[len(entries)]
		if first.GetIndex() <= commit {
			return nil, fmt.Errorf("%w: rebuilt log drops entry index %d term %d (snapshot index %d, commit %d)",
				errStaleTailIntersectsCommit, first.GetIndex(), first.GetTerm(), replay.index, commit)
		}
		logger.WithFields(withFields(fields, map[string]any{
			"firstDiscardedIndex": first.GetIndex(),
			"firstDiscardedTerm":  first.GetTerm(),
			"discarded":           dropped,
			"kept":                len(entries),
		})).Errorf("Discarded uncommitted WAL tail that replay resurrected")
	}

	tail, err := discardStaleReplayTail(replay.term, commit, entries)
	if err != nil {
		return nil, err
	}
	if tail.discarded > 0 {
		details := withFields(fields, map[string]any{
			"firstDiscardedIndex": tail.firstDiscarded.GetIndex(),
			"firstDiscardedTerm":  tail.firstDiscarded.GetTerm(),
			"discarded":           tail.discarded,
		})
		assert.Unreachable("rebuilt WAL log still holds a suffix that cannot belong to a Raft log", details)
		logger.WithFields(details).Errorf("Discarded WAL suffix that the rebuild kept but cannot belong to a Raft log")
	}

	return tail.kept, nil
}

func withFields(base, extra map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(extra))
	maps.Copy(out, base)
	maps.Copy(out, extra)

	return out
}

// trimToRebuiltLog returns the prefix of the ReadAll entries that matches the
// rebuilt log. The rebuild may only remove a suffix: any other difference
// means the two readers disagree on history, and startup fails closed.
func trimToRebuiltLog(readAll []*raftpb.Entry, rebuilt []logID) ([]*raftpb.Entry, error) {
	if len(rebuilt) > len(readAll) {
		return nil, fmt.Errorf("%w: rebuilt log has %d entries, ReadAll returned %d",
			errReplayRebuild, len(rebuilt), len(readAll))
	}
	for i, id := range rebuilt {
		if readAll[i].GetIndex() != id.index || readAll[i].GetTerm() != id.term {
			return nil, fmt.Errorf("%w: rebuilt entry (%d, term %d) differs from ReadAll entry (%d, term %d)",
				errReplayRebuild, id.index, id.term, readAll[i].GetIndex(), readAll[i].GetTerm())
		}
	}

	return readAll[:len(rebuilt):len(rebuilt)], nil
}
