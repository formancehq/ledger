package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

const (
	walCreationCompletedFile = "WAL_CREATION_COMPLETED"
	etcdWalDir               = "etcd"
	snapDir                  = "snap"

	defaultPurgeInterval = 30 * time.Second
)

// Option configures a DefaultWAL instance.
type Option func(*DefaultWAL)

// DefaultWAL implements raft.Storage interface for etcd/raft using etcd/wal.
type DefaultWAL struct {
	// snapshotPersistenceMu serializes each full snapshot-file + WAL-record
	// persistence sequence. It must be acquired before mu so the persisted
	// order matches the order in which snapshot state is captured.
	snapshotPersistenceMu sync.Mutex

	// mu protects all mutable state (entries, snapshot, hardState)
	mu sync.RWMutex

	// HardState contains the current term and commit index
	hardState *raftpb.HardState

	// Snapshot stores the most recent snapshot
	snapshot *raftpb.Snapshot

	// compactedIndex is the last entry incorporated into the compacted log
	// prefix. Its term remains available through compactedTerm so raft can
	// match an AppendEntries request at FirstIndex()-1. This boundary is
	// allowed to lag behind snapshot.Metadata.Index: a newer snapshot may be
	// published while older entries remain available for follower catch-up.
	compactedIndex uint64
	compactedTerm  uint64

	// DefaultWAL for storing log entries
	wal *wal.WAL

	// In-memory cache of entries (for fast access)
	// This is rebuilt from DefaultWAL on startup
	entries []*raftpb.Entry

	logger      logging.Logger
	meter       metric.Meter
	dataDir     string
	snapshotter *Snapshotter
	etcdWalDir  string

	// Purger for old WAL segment files
	stopPurge     chan struct{}
	purgeDone     <-chan struct{}
	purgeInterval time.Duration

	// Zap logger for etcd WAL and purger
	zapLogger *zap.Logger

	// Metrics
	appendSaveHistogram      metric.Int64Histogram
	appendBatchSizeHistogram metric.Int64Histogram
	recoveryRepairCounter    metric.Int64Counter
}

// walDirPopulated reports whether the etcd WAL directory at walDir holds any
// consensus state that must never be deleted. It is used only on the
// creation-marker-missing startup branch to distinguish the disposable
// empty-bootstrap remnant (a crash between wal.Create and the marker write,
// which leaves a fresh state-free segment) from populated or ambiguous state
// (EN-1525).
//
// "Populated" is any of three signals, checked in a single raw-decoder pass so
// none is lost:
//   - a non-empty HardState (a StateType record carrying a term/vote/commit) —
//     losing it risks a double-vote → split brain;
//   - at least one log entry (an EntryType record) — losing an unsnapshotted
//     entry is acknowledged-write loss;
//   - a snapshot record carrying a ConfState with voters (a SnapshotType
//     record) — this is the persisted cluster membership. wal.ReadAll returns
//     only metadata/HardState/entries and drops the snapshot record entirely,
//     so a WAL that recorded CreateSnapshot(0, initialConfState) but crashed
//     before any HardState/entry would wrongly read as empty; scanning
//     SnapshotType directly closes that gap.
//
// The decoder is fed the raw *.wal segments opened read-only (no lock), so it
// does not conflict with a concurrent writer. Crucially we do NOT rely on
// wal.ReadAll / wal.OpenForRead here: their read-mode contract silently accepts
// a torn tail (io.ErrUnexpectedEOF) and returns the records decoded so far,
// which would let a segment truncated mid-write past its first record read as
// empty and be deleted. We instead inspect the terminal decode error: only a
// clean io.EOF is treated as fully read; a torn tail (io.ErrUnexpectedEOF) or
// any other error (CRC mismatch, unknown record type, open failure) is returned
// so the caller fails closed. A genuinely fresh, empty WAL decodes cleanly (its
// metadata + CRC + empty-snapshot records terminate on io.EOF) with no state and
// is reported as not populated.
func walDirPopulated(walDir string) (bool, error) {
	// etcd names segments %016x-%016x.wal, so a lexical sort is chronological.
	// A single concatenated decoder over all segments mirrors how etcd's own
	// ReadAll / ValidSnapshotEntries stream the directory.
	names, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return false, fmt.Errorf("listing WAL segments for verification: %w", err)
	}
	slices.Sort(names)

	readers := make([]fileutil.FileReader, 0, len(names))
	var files []*os.File
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	for _, name := range names {
		f, err := os.OpenFile(name, os.O_RDONLY, 0)
		if err != nil {
			return false, fmt.Errorf("opening WAL segment %q for verification: %w", name, err)
		}
		files = append(files, f)
		readers = append(readers, fileutil.NewFileReader(f))
	}

	decoder := wal.NewDecoder(readers...)

	var populated bool
	rec := &walpb.Record{}
	for err = decoder.Decode(rec); err == nil; err = decoder.Decode(rec) {
		switch rec.GetType() {
		case wal.StateType:
			var hs raftpb.HardState
			if uErr := proto.Unmarshal(rec.GetData(), &hs); uErr != nil {
				return false, fmt.Errorf("decoding WAL HardState record for verification: %w", uErr)
			}
			if !raft.IsEmptyHardState(&hs) {
				populated = true
			}
		case wal.EntryType:
			// Any log entry means real consensus activity: not the remnant.
			populated = true
		case wal.SnapshotType:
			var snap walpb.Snapshot
			if uErr := proto.Unmarshal(rec.GetData(), &snap); uErr != nil {
				return false, fmt.Errorf("decoding WAL snapshot record for verification: %w", uErr)
			}
			// A snapshot record persisting cluster membership (voters) is state
			// we cannot rebuild; treat it as populated even at index 0.
			if len(snap.GetConfState().GetVoters()) > 0 {
				populated = true
			}

		case wal.CrcType:
			// Each segment's records are checksummed from that segment's seed,
			// and the decoder validates every non-CRC record against its running
			// sum, so the seed must be imported at each boundary. Without this a
			// multi-segment WAL fails at the second boundary and the caller
			// reports a healthy directory as torn or corrupt.
			crc := decoder.LastCRC()
			if crc != 0 && rec.Validate(crc) != nil {
				return false, fmt.Errorf("validating WAL CRC record for verification: %w", wal.ErrCRCMismatch)
			}

			decoder.UpdateCRC(rec.GetCrc())
		}
	}

	// Only a clean end-of-stream means we read the whole WAL. A torn tail
	// (io.ErrUnexpectedEOF) or any other decode error is ambiguous — the caller
	// must fail closed rather than risk deleting unread consensus state.
	if !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("reading WAL for verification (possibly torn or corrupt): %w", err)
	}

	return populated, nil
}

// walRecord is one entry or snapshot record, kept in physical order so the
// reconstruction below can apply their effects in the order they were written.
type walRecord struct {
	entry *raftpb.Entry
	snap  *walpb.Snapshot
}

// reconstructEntries replays the retained WAL segments in physical record order
// and returns the logical entry log they imply for a WAL opened at the snapshot
// selectedSnapIndex/selectedSnapTerm.
//
// This exists because etcd's ReadAll applies an entry record's suffix-truncation
// effect only when that entry's index is greater than the snapshot the WAL was
// opened at. A truncating overwrite written at or below the snapshot index is
// therefore skipped together with the truncation it implies, and the physically
// earlier entries it replaced survive replay — leaving a tail no valid Raft log
// can contain. The node's last-entry term then regresses, which is enough for it
// to grant a vote it must refuse (raft compares last-entry term before index),
// so a replica whose log is missing committed entries can win an election and
// overwrite them.
//
// Two rules reconstruct the log etcd's own in-memory storage would hold:
//
//   - an entry record at index i drops every reconstructed entry at index >= i,
//     whether or not it is retained afterwards. This is ReadAll's ents[:offset]
//     without its snapshot-relative guard, and it is what the defect above is
//     missing;
//   - the selected snapshot, applied once at the end, drops the compacted prefix
//     and — when the log held a different entry at the snapshot's own index —
//     the whole log. That second effect is what ApplySnapshot performs in memory
//     (it clears the entry cache) but never records on disk.
//
// Snapshot *records* have no effect of their own. ApplySnapshot writes a guard
// record before advancing HardState, so an interrupted install leaves one that
// describes no durable state; a later recovery that commits past its index would
// make any commit-based validity test accept it retroactively and delete
// committed entries. Only the snapshot the caller actually selected — which
// required a matching snap file — is authoritative.
//
// Completeness: this reads the same segments ReadAll reads, and segments are
// reclaimed oldest-first, so any truncating record ReadAll could have observed
// is visible here too — a reclaimed truncating record took the entries it
// truncated with it.
func reconstructEntries(walDir string, selectedSnapIndex, selectedSnapTerm uint64) ([]*raftpb.Entry, error) {
	// etcd names segments %016x-%016x.wal, so a lexical sort is chronological.
	names, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return nil, fmt.Errorf("listing WAL segments for reconstruction: %w", err)
	}
	slices.Sort(names)

	readers := make([]fileutil.FileReader, 0, len(names))

	var files []*os.File

	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	for _, name := range names {
		f, openErr := os.OpenFile(name, os.O_RDONLY, 0)
		if openErr != nil {
			return nil, fmt.Errorf("opening WAL segment %q for reconstruction: %w", name, openErr)
		}

		files = append(files, f)
		readers = append(readers, fileutil.NewFileReader(f))
	}

	fold := newWALFold(selectedSnapIndex, selectedSnapTerm)
	decoder := wal.NewDecoder(readers...)

	rec := &walpb.Record{}
	for err = decoder.Decode(rec); err == nil; err = decoder.Decode(rec) {
		switch rec.GetType() {
		case wal.EntryType:
			var entry raftpb.Entry
			if uErr := proto.Unmarshal(rec.GetData(), &entry); uErr != nil {
				return nil, fmt.Errorf("decoding WAL entry record for reconstruction: %w", uErr)
			}

			fold.addEntry(&entry)

		case wal.SnapshotType:
			var snap walpb.Snapshot
			if uErr := proto.Unmarshal(rec.GetData(), &snap); uErr != nil {
				return nil, fmt.Errorf("decoding WAL snapshot record for reconstruction: %w", uErr)
			}

			fold.addSnapshot(&snap)

		case wal.CrcType:
			if crcErr := importCRCSeed(decoder, rec); crcErr != nil {
				return nil, crcErr
			}
		}
	}

	// A torn tail is tolerated exactly as ValidSnapshotEntries tolerates it: the
	// incomplete record was never committed, and New's repair path owns it.
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("reading WAL for reconstruction (possibly torn or corrupt): %w", err)
	}

	return fold.result(), nil
}

// importCRCSeed mirrors ReadAll/ValidSnapshotEntries: a decoder built over a
// subset of the segments starts at CRC zero and must import each segment's seed,
// or the first record of a reclaimed WAL fails to validate.
func importCRCSeed(decoder wal.Decoder, rec *walpb.Record) error {
	crc := decoder.LastCRC()
	if crc != 0 && rec.Validate(crc) != nil {
		return fmt.Errorf("validating WAL CRC record for reconstruction: %w", wal.ErrCRCMismatch)
	}

	decoder.UpdateCRC(rec.GetCrc())

	return nil
}

// walFold folds physically-ordered WAL records into the logical log they imply,
// relative to the snapshot the WAL was opened at. It is a type rather than a
// loop so the decode pass can stream into it, and so the fold stays
// unit-testable without a WAL on disk.
//
// Only entries above the snapshot boundary are retained, but every record's
// truncation effect applies regardless of where it lands — that asymmetry is
// what bounds memory to the recovered tail while still honouring an overwrite
// written below the boundary, which is the defect this exists for.
//
// Indices in the reconstruction are contiguous and ascending by construction, so
// every lookup is either the sequential-append fast path or a binary search. A
// linear rescan per record would make an ordinary recovery quadratic in the size
// of the retained tail, which is time spent inside New before the node starts.
type walFold struct {
	entries []*raftpb.Entry

	snapIndex uint64
	snapTerm  uint64

	// boundaryTerm is the term of the entry the log currently holds at
	// snapIndex, which decides whether the selected snapshot agrees with the log
	// it sits in. Tracked as a scalar because that entry is never retained.
	boundaryTerm  uint64
	boundaryKnown bool
}

func newWALFold(snapIndex, snapTerm uint64) *walFold {
	return &walFold{snapIndex: snapIndex, snapTerm: snapTerm}
}

// addEntry applies one entry record: drop the suffix it replaces, then retain it
// if it lives above the snapshot boundary. Unlike ReadAll the truncation is
// unconditional — it is a property of the record, not of the snapshot the WAL
// happens to be opened at.
func (f *walFold) addEntry(entry *raftpb.Entry) {
	idx := entry.GetIndex()
	f.entries = f.truncateTo(idx)

	switch {
	case idx < f.snapIndex:
		// Everything from here up is replaced, including whatever stood at the
		// boundary.
		f.boundaryKnown = false
	case idx == f.snapIndex:
		f.boundaryTerm, f.boundaryKnown = entry.GetTerm(), true
	default:
		f.entries = append(f.entries, entry)
	}
}

// addSnapshot applies a snapshot record, but only the one the caller selected —
// matched on both index and term.
//
// Every other snapshot record is ignored. ApplySnapshot writes a guard record
// before advancing HardState, so an interrupted install leaves one that
// describes no durable state; deciding validity from the commit index would
// accept it retroactively once later commits overtake it, and delete committed
// entries. The selected snapshot earned its authority by having a matching snap
// file on disk.
//
// It applies at its physical position, not at the end of the stream. A received
// snapshot invalidates the log that preceded it, never the entries the node
// legitimately appended and committed afterwards — collapsing that distinction
// turns an ordinary install-then-catch-up into a permanent boot failure.
func (f *walFold) addSnapshot(snap *walpb.Snapshot) {
	if snap.GetIndex() != f.snapIndex || snap.GetTerm() != f.snapTerm {
		return
	}

	// A different entry at the snapshot's own index means the log it sits in is
	// obsolete, which is what ApplySnapshot does in memory (it clears the entry
	// cache) but never records on disk.
	if f.boundaryKnown && f.boundaryTerm != f.snapTerm {
		f.entries = nil
	}

	// From here the snapshot IS the boundary, so nothing earlier can conflict
	// with it again.
	f.boundaryKnown = false
}

// result returns the recovered log. A conflict the selected snapshot never
// arrived to resolve still invalidates it: the snapshot is durable whether or
// not its record survived reclamation.
func (f *walFold) result() []*raftpb.Entry {
	if f.boundaryKnown && f.boundaryTerm != f.snapTerm {
		return nil
	}

	return f.entries
}

// truncateTo returns the reconstruction with every entry at index >= idx
// removed. The sequential-append case answers without searching at all.
//
// Discarded slots are cleared rather than left beyond the new length: the result
// becomes the node's live entry cache, and a reslice alone would keep every
// overwritten payload reachable through the backing array for the process
// lifetime.
func (f *walFold) truncateTo(idx uint64) []*raftpb.Entry {
	if len(f.entries) == 0 || f.entries[len(f.entries)-1].GetIndex() < idx {
		return f.entries
	}

	keep := sort.Search(len(f.entries), func(i int) bool {
		return f.entries[i].GetIndex() >= idx
	})

	clear(f.entries[keep:])

	return f.entries[:keep]
}

// resolveWALRecords folds a slice of records in one call. Production streams
// into walFold directly; this keeps the fold testable from literals.
func resolveWALRecords(records []walRecord, selectedSnapIndex, selectedSnapTerm uint64) []*raftpb.Entry {
	fold := newWALFold(selectedSnapIndex, selectedSnapTerm)

	for _, r := range records {
		switch {
		case r.entry != nil:
			fold.addEntry(r.entry)
		case r.snap != nil:
			fold.addSnapshot(r.snap)
		}
	}

	return fold.result()
}

// repairRecoveredEntries replaces the slice ReadAll returned with the log the
// physical WAL records actually imply, and refuses to start when the result
// cannot describe this node's durable state.
//
// See reconstructEntries for why ReadAll's output is not trustworthy on its own.
// The reconstruction is authoritative; the comparison against ReadAll exists so
// an occurrence is visible in logs and metrics rather than silently corrected.
func (s *DefaultWAL) repairRecoveredEntries() error {
	snapIndex := s.snapshot.GetMetadata().GetIndex()
	snapTerm := s.snapshot.GetMetadata().GetTerm()

	reconstructed, err := reconstructEntries(s.etcdWalDir, snapIndex, snapTerm)
	if err != nil {
		return err
	}

	if !sameEntryIdentities(s.entries, reconstructed) {
		discarded := len(s.entries) - len(reconstructed)

		assert.Reachable("WAL replay resurrected entries the physical log had overwritten", map[string]any{
			"snapshotIndex":        snapIndex,
			"snapshotTerm":         snapTerm,
			"readAllEntries":       len(s.entries),
			"reconstructedEntries": len(reconstructed),
		})

		s.logger.Errorf("========================================")
		s.logger.Errorf("WAL REPLAY INCONSISTENT: etcd ReadAll returned %d entries, the physical records imply %d",
			len(s.entries), len(reconstructed))
		s.logger.Errorf("Snapshot is index=%d term=%d; replaying overwrite records below that index restores the real log",
			snapIndex, snapTerm)
		s.logger.Errorf("Discarding the resurrected suffix — those entries were overwritten before this snapshot was taken")
		s.logger.Errorf("========================================")

		if s.recoveryRepairCounter != nil && discarded > 0 {
			s.recoveryRepairCounter.Add(context.Background(), int64(discarded))
		}

		s.entries = reconstructed
	}

	// Anything the durable HardState says is committed must survive recovery.
	// A reconstruction that cannot produce it is not a log this node may serve
	// from, and no local repair can invent the missing entries (invariant #7).
	commit := s.hardState.GetCommit()

	lastIndex := snapIndex
	if len(s.entries) > 0 {
		lastIndex = s.entries[len(s.entries)-1].GetIndex()
	}

	if commit > lastIndex {
		assert.Unreachable("recovered WAL does not reach the durable commit index", map[string]any{
			"commit":        commit,
			"lastIndex":     lastIndex,
			"snapshotIndex": snapIndex,
		})

		return fmt.Errorf(
			"recovered WAL ends at index %d but HardState commits through %d; "+
				"refusing to start with acknowledged entries missing from the log "+
				"(manual intervention required)",
			lastIndex, commit,
		)
	}

	// Contiguity from the compacted boundary is what makes the log addressable
	// at all: Entries/Term index into this slice relative to the snapshot.
	//
	// These two checks are defence in depth rather than a reachable path today:
	// a record stream that skips an index makes ReadAll fail first with
	// ErrSliceOutOfRange, so New returns before the reconstruction runs. They
	// stay because the reconstruction is what the node actually serves from, and
	// an unaddressable log must not be one of the shapes it can come up with.
	if len(s.entries) > 0 && s.entries[0].GetIndex() != snapIndex+1 {
		assert.Unreachable("recovered WAL does not resume at the snapshot boundary", map[string]any{
			"firstIndex":    s.entries[0].GetIndex(),
			"snapshotIndex": snapIndex,
		})

		return fmt.Errorf(
			"recovered WAL resumes at index %d but the snapshot boundary is %d; "+
				"refusing to start with a gap between the compacted prefix and the log "+
				"(manual intervention required)",
			s.entries[0].GetIndex(), snapIndex,
		)
	}

	for i := 1; i < len(s.entries); i++ {
		if s.entries[i].GetIndex() != s.entries[i-1].GetIndex()+1 {
			assert.Unreachable("recovered WAL has a gap between consecutive entries", map[string]any{
				"previousIndex": s.entries[i-1].GetIndex(),
				"index":         s.entries[i].GetIndex(),
			})

			return fmt.Errorf(
				"recovered WAL jumps from index %d to %d; "+
					"refusing to start with a non-contiguous log (manual intervention required)",
				s.entries[i-1].GetIndex(), s.entries[i].GetIndex(),
			)
		}
	}

	return nil
}

// sameEntryIdentities reports whether two recovered logs agree on every entry's
// index and term. Payloads are not compared: a divergence in identity is what
// distinguishes a resurrected suffix, and the payloads are byte-identical
// whenever the identities are.
func sameEntryIdentities(a, b []*raftpb.Entry) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i].GetIndex() != b[i].GetIndex() || a[i].GetTerm() != b[i].GetTerm() {
			return false
		}
	}

	return true
}

// New creates a new DefaultWAL instance.
func New(dataDir string, logger logging.Logger, meter metric.Meter, opts ...Option) (*DefaultWAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	logger = logger.WithFields(map[string]any{"cmp": "wal"})

	snapshotter, err := NewSnapshotter(filepath.Join(dataDir, snapDir), logger)
	if err != nil {
		return nil, err
	}

	s := &DefaultWAL{
		entries:       make([]*raftpb.Entry, 0),
		hardState:     &raftpb.HardState{},
		snapshot:      &raftpb.Snapshot{},
		logger:        logger,
		meter:         meter,
		dataDir:       dataDir,
		snapshotter:   snapshotter,
		etcdWalDir:    filepath.Join(dataDir, etcdWalDir),
		purgeInterval: defaultPurgeInterval,
	}

	for _, opt := range opts {
		opt(s)
	}

	// Create metrics
	s.appendSaveHistogram, err = meter.Int64Histogram(
		"wal.append.save.duration",
		metric.WithDescription("Time spent saving entries to DefaultWAL"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating append save histogram: %w", err)
	}

	s.appendBatchSizeHistogram, err = meter.Int64Histogram(
		"wal.append.batch_size",
		metric.WithDescription("Number of entries appended at once"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating append batch size histogram: %w", err)
	}

	s.recoveryRepairCounter, err = meter.Int64Counter(
		"wal.recovery.repaired_entries",
		metric.WithDescription("Entries discarded at startup because WAL replay resurrected an overwritten suffix"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating recovery repair counter: %w", err)
	}

	type zapProvider interface {
		Zap() *zap.Logger
	}
	if zp, ok := logger.(zapProvider); ok {
		s.zapLogger = zp.Zap()
	} else {
		s.zapLogger = zap.NewNop()
	}

	zapLogger := s.zapLogger

	markerFilePath := filepath.Join(s.dataDir, walCreationCompletedFile)

	_, err = os.Stat(markerFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("checking DefaultWAL creation completion marker: %w", err)
	}

	snap := &walpb.Snapshot{}

	if err == nil {
		s.logger.Infof("WAL creation completed, opening existing DefaultWAL")

		// List valid snapshot records from the etcd WAL (source of truth).
		validStart := time.Now()
		walSnaps, err := wal.ValidSnapshotEntries(zapLogger, s.etcdWalDir)
		if err != nil {
			return nil, fmt.Errorf("reading valid snapshot entries from WAL: %w", err)
		}
		s.logger.WithFields(map[string]any{
			"duration": time.Since(validStart).String(),
			"count":    len(walSnaps),
		}).Infof("WAL valid snapshot entries scanned")

		// Load the newest snap file that matches a WAL snapshot record.
		// This ensures orphaned snap files (written before a crash, without
		// a corresponding WAL record) are not used.
		loadStart := time.Now()
		loadedSnap, err := s.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil {
			return nil, fmt.Errorf("loading snapshot matching WAL: %w", err)
		}

		if loadedSnap != nil {
			s.snapshot = loadedSnap
			s.logger.
				WithFields(map[string]any{
					"index":    s.snapshot.GetMetadata().GetIndex(),
					"term":     s.snapshot.GetMetadata().GetTerm(),
					"duration": time.Since(loadStart).String(),
				}).
				Infof("Loaded snapshot from disk")
			snap = &walpb.Snapshot{
				Index: new(s.snapshot.GetMetadata().GetIndex()),
				Term:  new(s.snapshot.GetMetadata().GetTerm()),
			}
		} else if recovered := recoverConfStateFromWALRecords(walSnaps); recovered != nil {
			// Snap file lost (crash during non-atomic write, corrupted, or deleted).
			// Recover the ConfState from the latest WAL snapshot record so the node
			// can rejoin the cluster instead of failing with "first start requires
			// --bootstrap or --join".
			s.logger.Errorf("========================================")
			s.logger.Errorf("SNAP FILE RECOVERY: no snap file matches any of %d WAL snapshot records", len(walSnaps))
			s.logger.Errorf("Recovering ConfState from WAL record (index=%d, term=%d, voters=%v)",
				recovered.GetIndex(), recovered.GetTerm(), recovered.GetConfState().GetVoters())
			s.logger.Errorf("========================================")

			// Torn-boot recovery is the correct answer to a lost snap file;
			// asserting it confirms fault exploration actually reaches this
			// lifecycle outcome.
			assert.Reachable("snap file recovered from WAL snapshot records", map[string]any{
				"index":      recovered.GetIndex(),
				"term":       recovered.GetTerm(),
				"walRecords": len(walSnaps),
			})

			s.snapshot = &raftpb.Snapshot{
				Metadata: &raftpb.SnapshotMetadata{
					Index:     new(recovered.GetIndex()),
					Term:      new(recovered.GetTerm()),
					ConfState: recovered.GetConfState(),
				},
				// Data is empty: only peer addresses are lost (they will be
				// rediscovered via etcd). The FSM state lives in Pebble
				// (separate volume) and is not affected.
			}
			snap = &walpb.Snapshot{
				Index: new(recovered.GetIndex()),
				Term:  new(recovered.GetTerm()),
			}

			// Persist the recovered snapshot so subsequent restarts succeed normally.
			if saveErr := s.snapshotter.Save(s.snapshot); saveErr != nil {
				s.logger.WithFields(map[string]any{"error": saveErr}).
					Errorf("Failed to persist recovered snapshot (node will retry recovery on next restart)")
			}
		}

		walOpenStart := time.Now()
		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening existing DefaultWAL: %w", err)
		}
		s.logger.WithFields(map[string]any{
			"duration": time.Since(walOpenStart).String(),
		}).Infof("WAL opened")
	} else {
		s.logger.Infof("DefaultWAL creation not completed, creating new DefaultWAL")

		// A WAL holding state on this branch means the creation marker was lost
		// while the fsynced WAL survived (marker dirent lost to a torn first
		// boot, marker deletion, disk repair). Deleting it would destroy vote
		// records (double-vote → split brain) and entries not yet in a snapshot
		// (acknowledged-write loss), so we MUST NOT fall through to os.RemoveAll
		// for any WAL we cannot PROVE is the disposable empty-bootstrap remnant.
		//
		// File presence alone is not the signal: a crash between wal.Create and
		// the marker write legitimately leaves a fresh, state-free segment here.
		// Only a WAL verified empty (no HardState AND no entries) is safe to
		// remove. A populated WAL (HardState or entries) or one we cannot read
		// (corrupt/ambiguous) fails startup with a contextual error. The
		// assert.Unreachable records the invariant breach for Antithesis, but it
		// is the mandatory return that follows — not the assert — that makes the
		// branch terminal in a production (no-op assert) build. See EN-1525.
		existing, globErr := filepath.Glob(filepath.Join(s.etcdWalDir, "*.wal"))
		if globErr != nil {
			return nil, fmt.Errorf("scanning for existing WAL segments in %q: %w", s.etcdWalDir, globErr)
		}
		if len(existing) > 0 {
			populated, inspectErr := walDirPopulated(s.etcdWalDir)
			switch {
			case inspectErr != nil:
				// Corrupt / unreadable / ambiguous: a WAL we cannot prove empty
				// may hold votes or unsnapshotted entries. Fail closed.
				assert.Unreachable("WAL present without creation marker could not be verified before cleanup", map[string]any{
					"walFileCount": len(existing),
					"walDir":       s.etcdWalDir,
					"error":        inspectErr.Error(),
				})

				return nil, fmt.Errorf("WAL segments present in %q without a creation marker and could not be verified; refusing to delete possibly-populated consensus state (manual intervention required): %w", s.etcdWalDir, inspectErr)
			case populated:
				assert.Unreachable("WAL recreation would discard an existing populated WAL directory", map[string]any{
					"walFileCount": len(existing),
					"walDir":       s.etcdWalDir,
				})

				return nil, fmt.Errorf("WAL segments present in %q without a creation marker hold consensus state (HardState or log entries); refusing to delete to avoid double-vote or acknowledged-write loss (manual intervention required)", s.etcdWalDir)
			}

			// Verified empty: the disposable empty-bootstrap remnant (a crash
			// between wal.Create and the marker write). Safe to remove below.
			s.logger.Infof("existing WAL directory verified empty (bootstrap remnant), recreating")
		}

		if err := os.RemoveAll(s.etcdWalDir); err != nil {
			return nil, fmt.Errorf("removing existing DefaultWAL directory: %w", err)
		}

		w, err := wal.Create(zapLogger, s.etcdWalDir, nil)
		if err != nil {
			return nil, fmt.Errorf("creating new DefaultWAL: %w", err)
		}

		// Close the DefaultWAL created by wal.Create() and reopen it with wal.Open()
		// This is necessary because wal.Create() returns a DefaultWAL in write mode,
		// and ReadAll() requires a DefaultWAL opened with wal.Open()
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("closing newly created DefaultWAL: %w", err)
		}

		f, err := os.Create(markerFilePath)
		if err != nil {
			return nil, fmt.Errorf("creating DefaultWAL creation completion marker: %w", err)
		}

		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("syncing DefaultWAL creation completion marker: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("closing DefaultWAL creation completion marker: %w", err)
		}

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening newly created DefaultWAL: %w", err)
		}
	}

	_, s.hardState, s.entries, err = s.wal.ReadAll()
	if err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("reading DefaultWAL entries: %w", err)
		}

		// WAL has a partially written record from a crash (OOMKill, SIGKILL, power loss).
		// The incomplete entries were never committed by Raft, so truncating is safe.
		s.logger.Errorf("========================================")
		s.logger.Errorf("WAL CORRUPTED: unexpected EOF detected")
		s.logger.Errorf("Attempting automatic repair by truncating incomplete records...")
		s.logger.Errorf("========================================")

		closeErr := s.wal.Close()
		if closeErr != nil {
			s.logger.WithFields(map[string]any{"error": closeErr}).Errorf("Failed to close corrupted WAL before repair")
		}

		if !wal.Repair(zapLogger, s.etcdWalDir) {
			return nil, errors.New("WAL repair failed after unexpected EOF — manual intervention required")
		}

		s.logger.Errorf("WAL repair succeeded — re-opening WAL")

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening repaired WAL: %w", err)
		}

		_, s.hardState, s.entries, err = s.wal.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("reading repaired WAL entries: %w", err)
		}

		s.logger.Errorf("========================================")
		s.logger.Errorf("WAL recovery complete — %d entries recovered", len(s.entries))
		s.logger.Errorf("========================================")
	}
	if s.hardState == nil {
		s.hardState = &raftpb.HardState{}
	}

	if err := s.repairRecoveredEntries(); err != nil {
		return nil, err
	}

	// etcd WAL replay starts immediately after the latest durable full
	// snapshot. That snapshot is therefore the compacted-prefix boundary after
	// a restart. Compact advances the boundary independently for the remainder
	// of this process lifetime.
	s.compactedIndex = s.snapshot.GetMetadata().GetIndex()
	s.compactedTerm = s.snapshot.GetMetadata().GetTerm()

	s.logger.
		WithFields(map[string]any{
			"entries":          len(s.entries),
			"hardState.Term":   s.hardState.GetTerm(),
			"hardState.Commit": s.hardState.GetCommit(),
			"snapshot.Index":   s.snapshot.GetMetadata().GetIndex(),
			"snapshot.Term":    s.snapshot.GetMetadata().GetTerm(),
		}).Infof("WAL replay completed")

	// Start background purger to delete old WAL segment files that have been
	// unlocked by ReleaseLockTo during compaction.
	s.stopPurge = make(chan struct{})
	// Use a nop logger for the purger to suppress the benign "failed to lock file"
	// warning that occurs when ReleaseLockTo keeps one extra segment locked.
	// The purger retries on the next cycle and eventually succeeds.
	s.purgeDone, _ = fileutil.PurgeFileWithDoneNotify(zap.NewNop(), s.etcdWalDir, ".wal", 1, s.purgeInterval, s.stopPurge)

	return s, nil
}

// recoverConfStateFromWALRecords scans WAL snapshot records from newest to
// oldest and returns the first one that carries a non-empty ConfState.
// Returns nil if no usable record is found.
func recoverConfStateFromWALRecords(walSnaps []*walpb.Snapshot) *walpb.Snapshot {
	for i := range slices.Backward(walSnaps) {
		if walSnaps[i].GetConfState() != nil && len(walSnaps[i].GetConfState().GetVoters()) > 0 {
			return walSnaps[i]
		}
	}

	return nil
}

// InitialState returns the saved HardState and ConfState information.
//
// The ConfState is wrapped with raftpb.EnsureConfState so it (and all of its
// pointer fields) is never nil. raft v3.7 requires this: newRaft feeds the
// returned ConfState straight into confchange.Restore, which dereferences its
// slice fields — a nil ConfState (as an empty WAL would otherwise yield, since
// s.snapshot starts as &raftpb.Snapshot{} with a nil Metadata.ConfState) would
// panic. This mirrors what raft's own MemoryStorage.InitialState does.
//
// EnsureConfState mutates its argument in place (it sets a nil AutoLeave pointer
// field), so it must NOT be called on s.snapshot's ConfState: that value is owned
// by the shared snapshot and is only guarded by an RLock here, so a write through
// it races with concurrent InitialState / snapshot readers. We first clone the
// shared ConfState (or start from nil when there is none) and let EnsureConfState
// fill in the clone's pointer fields, leaving the shared value untouched.
func (s *DefaultWAL) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var confStateCopy *raftpb.ConfState
	if shared := s.snapshot.GetMetadata().GetConfState(); shared != nil {
		confStateCopy = proto.Clone(shared).(*raftpb.ConfState)
	}

	return s.hardState, raftpb.EnsureConfState(confStateCopy), nil
}

// Entries returns a slice of log entries in the range [lo, hi).
func (s *DefaultWAL) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, fmt.Errorf("invalid range: lo=%d, hi=%d", lo, hi)
	}

	firstIndex := s.firstIndexLocked()
	lastIndex := s.lastIndexLocked()

	if lo < firstIndex {
		return nil, raft.ErrCompacted
	}

	if hi > lastIndex+1 {
		return nil, fmt.Errorf("entries[%d:%d) is out of bound [%d:%d]", lo, hi, firstIndex, lastIndex+1)
	}

	// Only contains dummy entries.
	if len(s.entries) == 0 {
		return nil, raft.ErrUnavailable
	}

	offset := s.entries[0].GetIndex()
	if lo < offset {
		return nil, raft.ErrCompacted
	}

	if hi > offset+uint64(len(s.entries)) {
		return nil, raft.ErrUnavailable
	}

	// Slice the entries
	ents := s.entries[lo-offset : hi-offset]

	// Limit size
	size := uint64(0)
	for i := range ents {
		size += uint64(proto.Size(ents[i]))
		if size > maxSize {
			ents = ents[:i+1]

			break
		}
	}

	// Cap the returned slice to its final length. Raft's Storage contract
	// permits callers to append to the returned slice (e.g. unstable entries
	// forwarded via MsgApp and serialized asynchronously). After contiguous
	// growth retains spare append capacity, an uncapped subrange or
	// maxSize-limited window would share writable slots with s.entries: a
	// caller append could overwrite retained entries, and a later append
	// could overwrite the caller's suffix. The full slice expression makes
	// the borrowed window ownership-safe — appends reallocate instead of
	// writing into s.entries' backing array. Matches raft.MemoryStorage.
	ents = slices.Clip(ents)

	return ents, nil
}

// Term returns the term of entry i.
func (s *DefaultWAL) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.termLocked(i)
}

// LastIndex returns the index of the last entry in the log.
func (s *DefaultWAL) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastIndexLocked(), nil
}

// lastIndexLocked returns the last index without acquiring lock (caller must hold lock).
func (s *DefaultWAL) lastIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return s.snapshot.GetMetadata().GetIndex()
	}

	return s.entries[len(s.entries)-1].GetIndex()
}

// FirstIndex returns the index of the first log entry.
func (s *DefaultWAL) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.firstIndexLocked(), nil
}

// firstIndexLocked returns the first index without acquiring lock (caller must hold lock).
func (s *DefaultWAL) firstIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return max(s.snapshot.GetMetadata().GetIndex(), s.compactedIndex) + 1
	}

	// Compact retains the boundary entry as a dummy so its term remains
	// available for matching, but the entry itself is no longer returned by
	// Entries. This mirrors raft.MemoryStorage's Storage contract.
	return max(s.entries[0].GetIndex(), s.compactedIndex+1)
}

// Snapshot returns the most recent snapshot.
func (s *DefaultWAL) Snapshot() (*raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot, nil
}

// Append appends entries to the log.
func (s *DefaultWAL) Append(hardState *raftpb.HardState, entries []*raftpb.Entry) error {
	s.mu.Lock()

	if hardStateEqual(hardState, s.hardState) && len(entries) == 0 {
		s.mu.Unlock()

		return nil
	}

	var firstIdx, lastIdx uint64
	if len(entries) > 0 {
		firstIdx = entries[0].GetIndex()
		lastIdx = entries[len(entries)-1].GetIndex()
	}

	if s.logger.Enabled(logging.TraceLevel) {
		logger := s.logger.WithFields(map[string]any{
			"entries":          len(entries),
			"firstIndex":       firstIdx,
			"lastIndex":        lastIdx,
			"hardState.Term":   hardState.GetTerm(),
			"hardState.Vote":   hardState.GetVote(),
			"hardState.Commit": hardState.GetCommit(),
			"prevCommit":       s.hardState.GetCommit(),
			"cachedEntries":    len(s.entries),
		})
		logger.Tracef("WAL Append")
	}

	// Update in-memory cache.
	if len(entries) > 0 {
		if len(s.entries) > 0 {
			offset := s.entries[0].GetIndex()

			last := entries[0].GetIndex() + uint64(len(entries)) - 1
			switch {
			case last < offset:
				// Stale append: every incoming entry precedes the cached
				// window. Forwarding them to wal.Save would rewind the
				// on-disk log past the active snapshot. Drop them — but
				// continue with HardState persistence below so a piggy-
				// backed term/vote/commit bump on the same call is not
				// silently lost (#301).
				if hardStateEqual(hardState, s.hardState) {
					s.mu.Unlock()

					return nil
				}

				entries = nil
			case entries[0].GetIndex() == offset+uint64(len(s.entries)):
				// Contiguous append: the incoming batch starts exactly at the
				// next index after the cached window. Grow in place so repeated
				// batches amortize instead of copying the full retained prefix
				// on every call (EN-1964). Entries() caps its borrowed window,
				// so this spare capacity cannot be aliased by Raft callers.
				s.entries = append(s.entries, entries...)
			case entries[0].GetIndex() > offset+uint64(len(s.entries)):
				// Gap (never produced by valid Raft, but preserved for the
				// existing defensive behaviour).
				s.entries = append(s.entries, entries...)
			default:
				truncateIndex := entries[0].GetIndex()
				var kept []*raftpb.Entry
				if truncateIndex > offset {
					kept = s.entries[:truncateIndex-offset]
				}

				// Allocate a new slice instead of appending into the old
				// backing array. Entries() returns sub-slices of s.entries
				// that end up in MsgApp messages serialized asynchronously
				// by the transport. Reusing the backing array after truncation
				// would overwrite entries the transport is still reading.
				merged := make([]*raftpb.Entry, len(kept)+len(entries))
				copy(merged, kept)
				copy(merged[len(kept):], entries)
				s.entries = merged
			}
		} else {
			s.entries = append(s.entries, entries...)
		}
	}

	newHardState := s.hardState
	if !raft.IsEmptyHardState(hardState) {
		s.hardState = hardState
		newHardState = hardState
	}
	s.mu.Unlock()

	// Save to DefaultWAL
	saveStart := time.Now()
	err := s.wal.Save(newHardState, entries)
	s.appendSaveHistogram.Record(context.Background(), time.Since(saveStart).Microseconds())
	s.appendBatchSizeHistogram.Record(context.Background(), int64(len(entries)))

	if err != nil {
		s.logger.WithFields(map[string]any{
			"error":            err,
			"hardState.Commit": newHardState.GetCommit(),
		}).Errorf("WAL Save failed")
	}

	return err
}

// hardStateEqual returns true if a and b represent the same HardState.
// Nil is treated as an empty HardState.
func hardStateEqual(a, b *raftpb.HardState) bool {
	return a.GetTerm() == b.GetTerm() && a.GetVote() == b.GetVote() && a.GetCommit() == b.GetCommit()
}

// CreateSnapshot creates a snapshot at the given index.
func (s *DefaultWAL) CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) error {
	s.snapshotPersistenceMu.Lock()
	defer s.snapshotPersistenceMu.Unlock()

	s.mu.Lock()

	s.logger.WithFields(map[string]any{"index": index}).Infof("Creating snapshot")

	// Allow creating snapshot on empty storage (for initial cluster setup or restore).
	// Otherwise, prevent creating snapshot at same or lower index.
	isEmptyStorage := s.snapshot.GetMetadata().GetIndex() == 0 &&
		len(s.snapshot.GetMetadata().GetConfState().GetVoters()) == 0 &&
		len(s.entries) == 0
	if !isEmptyStorage && index <= s.snapshot.GetMetadata().GetIndex() {
		s.mu.Unlock()

		return raft.ErrSnapOutOfDate
	}

	// Get term directly without taking another lock
	// For initial snapshot (index 0 on empty storage), use term 0
	// For restore snapshot (empty storage, index > 0), use term 1
	var (
		term uint64
		err  error
	)

	if s.snapshot.GetMetadata().GetIndex() == 0 && len(s.entries) == 0 {
		if index == 0 {
			// Initial snapshot at index 0 - use term 0
			term = 0
		} else {
			// Restore snapshot: WAL is empty but we have restored data at a non-zero index.
			// Use term 1 to start a new Raft term for the restored cluster.
			term = 1
		}
	} else {
		term, err = s.termLocked(index)
		if err != nil {
			s.mu.Unlock()

			return err
		}
	}

	snap := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{
			Index:     new(index),
			Term:      new(term),
			ConfState: cs,
		},
		Data: data,
	}
	s.snapshot = snap
	if len(s.entries) == 0 {
		// Restore/empty-storage snapshots have no retained log entries. The
		// snapshot itself is therefore the only matching point available to
		// Raft and becomes the compacted prefix boundary.
		s.compactedIndex = index
		s.compactedTerm = term
	}
	s.mu.Unlock()

	if err := s.snapshotter.Save(snap); err != nil {
		return fmt.Errorf("saving snapshot file: %w", err)
	}

	// Write the WAL snapshot record BEFORE cleaning up old snap files.
	// If a crash occurs between Save and SaveSnapshot, the old snap file
	// is still on disk and LoadNewestAvailable will fall back to it.
	// Without this ordering, a crash would leave zero matching snap files,
	// causing the node to enter the fresh-start branch and lose cache state.
	if err := s.wal.SaveSnapshot(&walpb.Snapshot{
		Index:     new(snap.GetMetadata().GetIndex()),
		Term:      new(snap.GetMetadata().GetTerm()),
		ConfState: cs,
	}); err != nil {
		return fmt.Errorf("saving snapshot record: %w", err)
	}

	// Safe to clean up old snap files now — the WAL record guarantees that
	// LoadNewestAvailable will match the new snap file on restart.
	s.snapshotter.CleanupOlderThan(snap.GetMetadata().GetIndex())

	s.logger.WithFields(map[string]any{"index": index}).Infof("Snapshot created")

	return nil
}

// UpdateSnapshotConfState updates the ConfState of the latest snapshot without
// changing the snapshot data or index. This is used when cluster membership
// changes (e.g. a learner is added) so that etcd/raft sends snapshots with
// the correct ConfState to newly added nodes.
func (s *DefaultWAL) UpdateSnapshotConfState(cs *raftpb.ConfState) error {
	s.snapshotPersistenceMu.Lock()
	defer s.snapshotPersistenceMu.Unlock()

	s.mu.Lock()

	// Nothing to update if there is no snapshot yet.
	if s.snapshot.GetMetadata().GetIndex() == 0 && len(s.snapshot.GetMetadata().GetConfState().GetVoters()) == 0 {
		s.mu.Unlock()

		return nil
	}

	// Clone the snapshot so we don't mutate the previous value in place;
	// callers may still hold references (e.g. Snapshot() returned it).
	snap := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{
			Index:     new(s.snapshot.GetMetadata().GetIndex()),
			Term:      new(s.snapshot.GetMetadata().GetTerm()),
			ConfState: cs,
		},
		Data: s.snapshot.GetData(),
	}
	s.snapshot = snap
	s.mu.Unlock()

	err := s.snapshotter.Save(snap)
	if err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	err = s.wal.SaveSnapshot(&walpb.Snapshot{
		Index:     new(snap.GetMetadata().GetIndex()),
		Term:      new(snap.GetMetadata().GetTerm()),
		ConfState: cs,
	})
	if err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	s.logger.WithFields(map[string]any{
		"index": snap.GetMetadata().GetIndex(),
	}).Infof("Snapshot ConfState updated")

	return nil
}

// termLocked returns the term of entry i without taking a lock (assumes lock is already held).
func (s *DefaultWAL) termLocked(i uint64) (uint64, error) {
	firstIndex := s.firstIndexLocked()
	lastIndex := s.lastIndexLocked()

	if i < firstIndex-1 {
		return 0, raft.ErrCompacted
	}

	if i > lastIndex {
		return 0, fmt.Errorf("term of index %d is out of bound", i)
	}

	if i == s.compactedIndex {
		return s.compactedTerm, nil
	}

	if len(s.entries) > 0 {
		offset := s.entries[0].GetIndex()
		if i >= offset && i < offset+uint64(len(s.entries)) {
			return s.entries[i-offset].GetTerm(), nil
		}

		if i < offset {
			return 0, raft.ErrCompacted
		}
	}

	return 0, raft.ErrUnavailable
}

// ApplySnapshot applies a snapshot to the storage.
func (s *DefaultWAL) ApplySnapshot(snap *raftpb.Snapshot) error {
	s.snapshotPersistenceMu.Lock()
	defer s.snapshotPersistenceMu.Unlock()

	s.mu.Lock()

	s.logger.WithFields(map[string]any{
		"snapIndex":      snap.GetMetadata().GetIndex(),
		"snapTerm":       snap.GetMetadata().GetTerm(),
		"prevSnapIndex":  s.snapshot.GetMetadata().GetIndex(),
		"prevHardCommit": s.hardState.GetCommit(),
		"cachedEntries":  len(s.entries),
	}).Infof("WAL ApplySnapshot")

	s.snapshot = snap
	s.entries = nil // Clear entries after applying snapshot
	s.compactedIndex = snap.GetMetadata().GetIndex()
	s.compactedTerm = snap.GetMetadata().GetTerm()

	// Ensure the HardState commit index is at least as high as the snapshot.
	// A snapshot at index N implies all entries up to N are committed.
	if s.hardState.GetCommit() < snap.GetMetadata().GetIndex() {
		s.hardState = &raftpb.HardState{
			Term:   new(snap.GetMetadata().GetTerm()),
			Vote:   new(s.hardState.GetVote()),
			Commit: new(snap.GetMetadata().GetIndex()),
		}
	}

	// Persist the full snapshot file before its WAL record, following etcd's
	// crash-safety order: an orphaned snap file is harmless and cleaned up by a
	// later snapshot, while a WAL snapshot record without its snap file would
	// make restart fail. The in-memory snapshot and entry cache were already
	// replaced above; a persistence failure returns an error but does not roll
	// those in-memory mutations back.
	if err := s.snapshotter.Save(snap); err != nil {
		s.mu.Unlock()

		return fmt.Errorf("saving snapshot file: %w", err)
	}

	walSnap := &walpb.Snapshot{
		Index:     new(snap.GetMetadata().GetIndex()),
		Term:      new(snap.GetMetadata().GetTerm()),
		ConfState: snap.GetMetadata().GetConfState(),
	}
	// Persist a guard snapshot record before advancing HardState. Until the
	// HardState commit reaches this index, etcd ignores the record on restart
	// and the older snapshot and WAL segments remain available. Once HardState
	// is durable, this record guarantees that the new snapshot is recognized.
	if err := s.wal.SaveSnapshot(walSnap); err != nil {
		s.mu.Unlock()

		return fmt.Errorf("saving guard snapshot to WAL: %w", err)
	}

	// A commit-only HardState update does not make etcd WAL Save sync by
	// itself. Buffer it after the durable guard record, then write the same
	// snapshot record again as the sync barrier. Every durable prefix is safe:
	// either the old HardState ignores the guard record, or the new HardState
	// has a matching snapshot record that was already synced before it.
	if err := s.wal.Save(s.hardState, nil); err != nil {
		s.mu.Unlock()

		return fmt.Errorf("saving HardState after guard snapshot to WAL: %w", err)
	}

	if err := s.wal.SaveSnapshot(walSnap); err != nil {
		s.mu.Unlock()

		return fmt.Errorf("syncing snapshot and HardState to WAL: %w", err)
	}

	// Safe to clean up old snap files now — the WAL record is persisted.
	s.snapshotter.CleanupOlderThan(snap.GetMetadata().GetIndex())
	s.mu.Unlock()

	// Applying a received snapshot discards the entire cached entry slice.
	// Release the matching etcd WAL segment locks immediately; this
	// node may not apply local entries soon enough for Compact to do it.
	s.releaseLockTo(snap.GetMetadata().GetIndex())

	return nil
}

// Compact compacts the log up to the given index.
func (s *DefaultWAL) Compact(compactIndex uint64) error {
	s.mu.Lock()

	if compactIndex > s.snapshot.GetMetadata().GetIndex() {
		s.mu.Unlock()

		return fmt.Errorf(
			"index (%d) after last snapshot index(%d): %w",
			compactIndex,
			s.snapshot.GetMetadata().GetIndex(),
			raft.ErrCompacted,
		)
	}

	firstIndex := s.firstIndexLocked()
	if compactIndex < firstIndex {
		s.mu.Unlock()

		return fmt.Errorf("index before first index: %w", raft.ErrCompacted)
	}

	if len(s.entries) == 0 {
		s.mu.Unlock()

		return nil
	}

	compactTerm, err := s.termLocked(compactIndex)
	if err != nil {
		s.mu.Unlock()

		return fmt.Errorf("reading term at compaction index %d: %w", compactIndex, err)
	}
	// Truncate entries before compactIndex
	offset := s.entries[0].GetIndex()
	truncateIndex := compactIndex - offset
	if truncateIndex < uint64(len(s.entries)) {
		// IMPORTANT: Create a new slice to release memory of old entries.
		// Simply re-slicing with s.entries[truncateIndex:] keeps a reference
		// to the original backing array, preventing GC from reclaiming memory.
		remaining := len(s.entries) - int(truncateIndex)
		newEntries := make([]*raftpb.Entry, remaining)
		copy(newEntries, s.entries[truncateIndex:])
		s.entries = newEntries
	} else {
		// Set to nil instead of s.entries[:0] to release the backing array
		s.entries = nil
	}
	s.compactedIndex = compactIndex
	s.compactedTerm = compactTerm

	// Release s.mu before the I/O-bound ReleaseLockTo call.
	// The in-memory compaction is done; holding s.mu during file cleanup
	// would block Append (which also needs s.mu), stalling the Ready pipeline.
	s.mu.Unlock()

	// IMPORTANT: Release WAL file locks up to compactIndex.
	// This allows the etcd WAL to release memory associated with old log entries
	// and potentially remove old WAL segment files.
	// Without this call, the etcd WAL keeps file handles and memory indefinitely.
	s.releaseLockTo(compactIndex)

	return nil
}

// releaseLockTo makes WAL segment reclamation best-effort after the in-memory
// state has already advanced. Failure must not roll back a persisted snapshot
// or a completed in-memory compaction.
func (s *DefaultWAL) releaseLockTo(index uint64) {
	err := s.wal.ReleaseLockTo(index)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"index": index,
			"error": err,
		}).Errorf("Failed to release WAL lock")
	}
}

// Close closes the DefaultWAL. Safe to call multiple times.
func (s *DefaultWAL) Close() error {
	select {
	case <-s.stopPurge:
		// Already closed
	default:
		close(s.stopPurge)
		<-s.purgeDone
	}

	return s.wal.Close()
}
