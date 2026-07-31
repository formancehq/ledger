package balancehistorystore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

type tierManager struct {
	*storeCore
}

const (
	DefaultMaxSegmentBytes      int64 = 128 << 20
	DefaultMaxRunsPerTierPass         = 4
	archiveBindingFormatVersion       = 1
)

type archiveBinding struct {
	FormatVersion       uint32 `json:"formatVersion"`
	DestinationIdentity string `json:"destinationIdentity"`
	MutationEpoch       uint64 `json:"mutationEpoch"`
}

// TieringConfig enables content-addressed cold runs. The configured Archive's
// namespace is exclusively owned by this Store: runtime uploads must flow
// through Tier so the durable mutation epoch is advanced before remote I/O.
// RetainLocalRuns keeps the newest runs hot regardless of level; MinimumLevel
// prevents small, rapidly compacted runs from being archived prematurely.
type TieringConfig struct {
	Archive         balancehistoryarchive.Archive
	MinimumLevel    uint32
	RetainLocalRuns int
	MaxSegmentBytes int64
	MaxRunsPerPass  int
}

type tieringState struct {
	archive                    balancehistoryarchive.Archive
	reclaimer                  balancehistoryarchive.Reclaimer
	archiveIdentity            string
	archiveGate                *sync.RWMutex
	minimumLevel               uint32
	retainLocalRuns            int
	maxSegmentBytes            uint64
	maxRunsPerPass             int
	beforeGate                 func()
	afterMutationBeforeArchive func()
	afterArchiveBeforePublish  func()
}

// ConfigureTiering enables or disables cold tiering. A zero configuration
// preserves the local-only behavior; a non-zero policy requires an Archive.
func (s *tierManager) ConfigureTiering(config TieringConfig) error {
	if config.RetainLocalRuns < 0 {
		return fmt.Errorf("balance history local run retention must not be negative: %d", config.RetainLocalRuns)
	}
	if config.Archive == nil {
		if config.MinimumLevel != 0 || config.RetainLocalRuns != 0 || config.MaxSegmentBytes != 0 || config.MaxRunsPerPass != 0 {
			return errors.New("balance history archive is required when cold tiering policy is configured")
		}

		return disableTiering(s)
	}
	identified, ok := config.Archive.(balancehistoryarchive.IdentifiedArchive)
	if !ok || identified.DestinationIdentity() == "" {
		return errors.New("balance history archive destination identity is required")
	}
	reclaimer, _ := config.Archive.(balancehistoryarchive.Reclaimer)
	if reclaimer != nil && reclaimer.Namespace() == "" {
		return errors.New("balance history archive reclaimer namespace is required")
	}
	if config.MaxSegmentBytes == 0 {
		config.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if config.MaxSegmentBytes < 0 {
		return fmt.Errorf("balance history max segment bytes must be positive: %d", config.MaxSegmentBytes)
	}
	if uint64(config.MaxSegmentBytes) <= balancehistoryarchive.EmptyEncodedSize {
		return fmt.Errorf(
			"balance history max segment bytes must exceed archive overhead %d: %d",
			balancehistoryarchive.EmptyEncodedSize,
			config.MaxSegmentBytes,
		)
	}
	if config.MaxRunsPerPass == 0 {
		config.MaxRunsPerPass = DefaultMaxRunsPerTierPass
	}
	if config.MaxRunsPerPass < 0 {
		return fmt.Errorf("balance history max runs per tier pass must be positive: %d", config.MaxRunsPerPass)
	}

	return installTiering(s, &tieringState{
		archive:         config.Archive,
		reclaimer:       reclaimer,
		archiveIdentity: identified.DestinationIdentity(),
		minimumLevel:    config.MinimumLevel,
		retainLocalRuns: config.RetainLocalRuns,
		maxSegmentBytes: uint64(config.MaxSegmentBytes),
		maxRunsPerPass:  config.MaxRunsPerPass,
	})
}

// installTiering serializes a policy swap with every in-flight archive and
// remote collection operation. The initial state is published only after its
// durable binding succeeds, so readers can observe nil or a complete state,
// never a placeholder with a nil Archive.
func installTiering(s *tierManager, next *tieringState) error {
	for {
		current := s.tiering.Load()
		if current == nil {
			s.mutationMu.Lock()
			if s.tiering.Load() != nil {
				s.mutationMu.Unlock()

				continue
			}
			if err := s.rebindArchiveLocked(next.archiveIdentity); err != nil {
				s.mutationMu.Unlock()

				return err
			}
			next.archiveGate = &sync.RWMutex{}
			s.tiering.Store(next)
			s.mutationMu.Unlock()

			return nil
		}

		current.archiveGate.Lock()
		if s.tiering.Load() != current {
			current.archiveGate.Unlock()

			continue
		}
		next.archiveGate = current.archiveGate
		s.mutationMu.Lock()
		if err := s.rebindArchiveLocked(next.archiveIdentity); err != nil {
			s.mutationMu.Unlock()
			current.archiveGate.Unlock()

			return err
		}
		s.tiering.Store(next)
		s.mutationMu.Unlock()
		current.archiveGate.Unlock()

		return nil
	}
}

// rebindArchiveLocked requires mutationMu. When a tiering state already
// exists, the caller also holds archiveGate for writing, preserving the lock
// order archiveGate -> mutationMu.
func (s *tierManager) rebindArchiveLocked(identity string) error {
	manifest, err := readManifest(s.db)
	if err != nil {
		return fmt.Errorf("reading balance history manifest for archive binding: %w", err)
	}
	archived := false
	for _, run := range manifest.Runs {
		if run.Archived {
			archived = true

			break
		}
	}
	binding, found, err := readArchiveBindingRecord(s.db)
	if err != nil {
		return err
	}
	if archived {
		switch {
		case identity == "":
			return errors.New("balance history archive cannot be disabled while archived runs remain")
		case !found:
			return errors.New("balance history archived runs are missing their destination binding")
		case binding.DestinationIdentity != identity:
			return fmt.Errorf(
				"balance history archive destination mismatch: stored %q, configured %q",
				binding.DestinationIdentity,
				identity,
			)
		default:
			return nil
		}
	}
	if found && binding.DestinationIdentity == identity {
		return nil
	}
	if found {
		state, stateFound, err := readRemoteGCState(s.db)
		if err != nil {
			return fmt.Errorf("reading previous archive remote GC proof: %w", err)
		}
		if !stateFound || state.DestinationIdentity != binding.DestinationIdentity ||
			state.CompletedInventoryEpoch == 0 || state.CompletedInventoryEpoch != binding.MutationEpoch ||
			state.ScanEpoch != binding.MutationEpoch ||
			state.Cursor != "" ||
			state.ScanObjects != 0 || state.ScanBytes != 0 || state.InventoryObjects != 0 ||
			state.InventoryBytes != 0 || state.QueueObjects != 0 || state.QueueBytes != 0 {
			return errors.New("previous balance history archive destination has not been durably proven empty")
		}
	}
	if identity == "" {
		if !found {
			return nil
		}
		if err := s.db.Delete(archiveBindingKey(), pebble.Sync); err != nil {
			return fmt.Errorf("deleting unused balance history archive binding: %w", err)
		}

		return nil
	}
	binding = archiveBinding{
		FormatVersion:       archiveBindingFormatVersion,
		DestinationIdentity: identity,
		MutationEpoch:       1,
	}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("marshaling balance history archive binding: %w", err)
	}
	if err := s.db.Set(archiveBindingKey(), encoded, pebble.Sync); err != nil {
		return fmt.Errorf("persisting balance history archive binding: %w", err)
	}

	return nil
}

func readArchiveBinding(reader pebbleValueGetter) (string, bool, error) {
	binding, found, err := readArchiveBindingRecord(reader)

	return binding.DestinationIdentity, found, err
}

func readArchiveBindingRecord(reader pebbleValueGetter) (archiveBinding, bool, error) {
	encoded, closer, err := reader.Get(archiveBindingKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return archiveBinding{}, false, nil
		}

		return archiveBinding{}, false, fmt.Errorf("reading balance history archive binding: %w", err)
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return archiveBinding{}, false, fmt.Errorf("closing balance history archive binding: %w", err)
	}
	var binding archiveBinding
	if err := json.Unmarshal(copyEncoded, &binding); err != nil {
		return archiveBinding{}, false, fmt.Errorf("decoding balance history archive binding: %w", err)
	}
	if binding.FormatVersion != archiveBindingFormatVersion || binding.DestinationIdentity == "" || binding.MutationEpoch == 0 {
		return archiveBinding{}, false, errors.New("balance history archive binding is invalid")
	}

	return binding, true, nil
}

func (s *tierManager) advanceArchiveMutationEpoch(identity string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	binding, found, err := readArchiveBindingRecord(s.db)
	if err != nil {
		return err
	}
	if !found || binding.DestinationIdentity != identity {
		return errors.New("balance history archive mutation has no matching destination binding")
	}
	if binding.MutationEpoch == ^uint64(0) {
		return errors.New("balance history archive mutation epoch is exhausted")
	}
	binding.MutationEpoch++
	encoded, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("marshaling advanced balance history archive binding: %w", err)
	}
	if err := s.db.Set(archiveBindingKey(), encoded, pebble.Sync); err != nil {
		return fmt.Errorf("advancing balance history archive mutation epoch: %w", err)
	}

	return nil
}

func (s *tierManager) stageArchiveMutationEpochAdvance(batch *pebble.Batch) error {
	binding, found, err := readArchiveBindingRecord(s.db)
	if err != nil || !found {
		return err
	}
	if binding.MutationEpoch == ^uint64(0) {
		return errors.New("balance history archive mutation epoch is exhausted")
	}
	binding.MutationEpoch++
	encoded, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("marshaling reset balance history archive binding: %w", err)
	}
	if err := batch.Set(archiveBindingKey(), encoded, nil); err != nil {
		return fmt.Errorf("staging balance history archive mutation epoch reset: %w", err)
	}

	return nil
}

func disableTiering(s *tierManager) error {
	for {
		current := s.tiering.Load()
		if current == nil {
			s.mutationMu.Lock()
			if s.tiering.Load() != nil {
				s.mutationMu.Unlock()

				continue
			}
			err := s.rebindArchiveLocked("")
			s.mutationMu.Unlock()

			return err
		}

		current.archiveGate.Lock()
		if s.tiering.Load() != current {
			current.archiveGate.Unlock()

			continue
		}
		s.mutationMu.Lock()
		if err := s.rebindArchiveLocked(""); err != nil {
			s.mutationMu.Unlock()
			current.archiveGate.Unlock()

			return err
		}
		s.tiering.Store(nil)
		s.mutationMu.Unlock()
		current.archiveGate.Unlock()

		return nil
	}
}

// Tier archives every eligible immutable run and atomically replaces its
// local storage metadata only after the remote object has been verified. Runs
// leased by an active View are archived but remain local until a later call.
func (s *tierManager) Tier(ctx context.Context) (int, error) {
	tiering := s.tiering.Load()
	if tiering == nil || tiering.archive == nil {
		return 0, nil
	}
	manifest, err := s.Manifest()
	if err != nil {
		return 0, err
	}

	candidates := tierCandidates(manifest.Runs, tiering)
	if len(candidates) > tiering.maxRunsPerPass {
		candidates = candidates[:tiering.maxRunsPerPass]
	}
	changed := 0
	for _, run := range candidates {
		if err := ctx.Err(); err != nil {
			return changed, err
		}

		updated, err := s.tierCandidate(ctx, tiering, run)
		if errors.Is(err, errTieringReconfigured) {
			return changed, nil
		}
		if err != nil {
			return changed, err
		}
		if updated {
			changed++
		}
	}

	return changed, nil
}

func (s *tierManager) tierCandidate(ctx context.Context, tiering *tieringState, run RunRef) (bool, error) {
	if tiering.beforeGate != nil {
		tiering.beforeGate()
	}
	tiering.archiveGate.RLock()
	defer tiering.archiveGate.RUnlock()
	if s.tiering.Load() != tiering {
		// ConfigureTiering won the gate before this candidate started. Nothing
		// may be uploaded through a stale backend or namespace; the next tier
		// pass will recalculate candidates under the replacement policy.
		return false, errTieringReconfigured
	}

	if run.Archived {
		if err := verifyArchivedPartsDurable(ctx, tiering.archive, run); err != nil {
			return false, s.failArchive(err)
		}

		return s.publishArchivedRun(run, run.ArchiveParts)
	}
	// Invalidate every prior empty-inventory proof durably before the first
	// remote operation in the upload path. A crash, upload failure, or lost
	// manifest CAS can then only leave an orphan that a later full scan sees.
	if err := s.advanceArchiveMutationEpoch(tiering.archiveIdentity); err != nil {
		return false, err
	}
	if tiering.afterMutationBeforeArchive != nil {
		tiering.afterMutationBeforeArchive()
	}

	return s.archiveRun(ctx, tiering, run)
}

var errTieringReconfigured = errors.New("balance history tiering was reconfigured")

func verifyArchivedPartsDurable(
	ctx context.Context,
	archive balancehistoryarchive.Archive,
	run RunRef,
) error {
	for partIndex, part := range run.ArchiveParts {
		if err := ctx.Err(); err != nil {
			return err
		}
		exists, err := archive.Exists(ctx, part.Ref)
		if err != nil {
			return err
		}
		if !exists {
			return &ErrSourceMissing{Detail: fmt.Sprintf(
				"archived run %d part %d is no longer durable",
				run.ID,
				partIndex,
			)}
		}
	}

	return nil
}

func tierCandidates(runs []RunRef, tiering *tieringState) []RunRef {
	newest := append([]RunRef(nil), runs...)
	sort.Slice(newest, func(i, j int) bool {
		if newest[i].LastAuditSequence != newest[j].LastAuditSequence {
			return newest[i].LastAuditSequence > newest[j].LastAuditSequence
		}

		return newest[i].ID > newest[j].ID
	})
	retained := make(map[uint64]struct{}, min(tiering.retainLocalRuns, len(newest)))
	for index := 0; index < tiering.retainLocalRuns && index < len(newest); index++ {
		retained[newest[index].ID] = struct{}{}
	}

	candidates := make([]RunRef, 0, len(runs))
	for _, run := range newest {
		if _, keep := retained[run.ID]; keep || run.LocalRemoved || run.Level < tiering.minimumLevel {
			continue
		}
		candidates = append(candidates, run)
	}
	// Upload oldest runs first so an interrupted tier pass maximizes reclaimed
	// hot bytes while preserving the requested newest-run retention.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].LastAuditSequence != candidates[j].LastAuditSequence {
			return candidates[i].LastAuditSequence < candidates[j].LastAuditSequence
		}

		return candidates[i].ID < candidates[j].ID
	})

	return candidates
}

func (s *tierManager) archiveRun(
	ctx context.Context,
	tiering *tieringState,
	run RunRef,
) (bool, error) {
	snapshot := s.db.NewSnapshot()
	if err := verifyStoredRun(snapshot, run); err != nil {
		_ = snapshot.Close()

		return false, s.failArchive(err)
	}
	stream, err := newRunRecordStream(snapshot, run.ID)
	if err != nil {
		_ = snapshot.Close()

		return false, err
	}
	parts, archiveErr := archiveRunParts(ctx, tiering.archive, stream, run, tiering.maxSegmentBytes)
	streamCloseErr := stream.Close()
	snapshotCloseErr := snapshot.Close()
	if err := errors.Join(archiveErr, streamCloseErr, snapshotCloseErr); err != nil {
		return false, s.failArchive(err)
	}
	var archivedRecords uint64
	for _, part := range parts {
		archivedRecords += part.Ref.RecordCount
	}
	if archivedRecords != run.EntryCount+run.IdentityCount {
		return false, s.failArchive(&ErrCorrupt{Detail: fmt.Sprintf(
			"archived run %d record count is %d, want %d",
			run.ID,
			archivedRecords,
			run.EntryCount+run.IdentityCount,
		)})
	}
	if tiering.afterArchiveBeforePublish != nil {
		tiering.afterArchiveBeforePublish()
	}

	return s.publishArchivedRun(run, parts)
}

func archiveRunParts(
	ctx context.Context,
	archive balancehistoryarchive.Archive,
	stream balancehistoryarchive.RecordStream,
	run RunRef,
	maxBytes uint64,
) ([]ArchivePart, error) {
	parts := make([]ArchivePart, 0, 1)
	records := make([]balancehistoryarchive.Record, 0)
	partBytes := balancehistoryarchive.EmptyEncodedSize
	var lowerBound, route []byte

	flush := func(upperBound []byte) error {
		if len(records) == 0 {
			return nil
		}
		ref, err := archive.Archive(ctx, balancehistoryarchive.NewSliceStream(records))
		if err != nil {
			return err
		}
		if ref.Size > maxBytes {
			return &ErrCorrupt{Detail: fmt.Sprintf(
				"archive encoder exceeded max segment bytes for run %d: %d > %d",
				run.ID,
				ref.Size,
				maxBytes,
			)}
		}
		exists, err := archive.Exists(ctx, ref)
		if err != nil {
			return err
		}
		if !exists {
			return &ErrSourceMissing{Detail: fmt.Sprintf("archived run %d part was not durable after upload", run.ID)}
		}
		parts = append(parts, ArchivePart{
			Ref:        ref,
			LowerBound: bytes.Clone(lowerBound),
			UpperBound: bytes.Clone(upperBound),
		})
		records = records[:0]
		partBytes = balancehistoryarchive.EmptyEncodedSize
		lowerBound = nil
		route = nil

		return nil
	}

	for stream.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record := stream.Record()
		recordBytes, err := balancehistoryarchive.EncodedRecordSize(record)
		if err != nil {
			return nil, err
		}
		encodedAlone := balancehistoryarchive.EmptyEncodedSize + recordBytes
		if encodedAlone > maxBytes {
			return nil, &ErrSegmentRecordTooLarge{RunID: run.ID, EncodedBytes: encodedAlone, MaxBytes: maxBytes}
		}
		recordRoute, err := archiveRecordRoute(record.Key)
		if err != nil {
			return nil, err
		}
		routeLower, err := archiveRoutePrefix(run.ID, recordRoute)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 && (!bytes.Equal(route, recordRoute) || partBytes+recordBytes > maxBytes) {
			boundary := record.Key
			if !bytes.Equal(route, recordRoute) {
				boundary = routeLower
			}
			if err := flush(boundary); err != nil {
				return nil, err
			}
		}
		if len(records) == 0 {
			lowerBound = routeLower
			if len(parts) > 0 {
				lowerBound = bytes.Clone(parts[len(parts)-1].UpperBound)
			}
			route = bytes.Clone(recordRoute)
		}
		records = append(records, balancehistoryarchive.Record{
			Key:   bytes.Clone(record.Key),
			Value: bytes.Clone(record.Value),
		})
		partBytes += recordBytes
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	if err := flush(prefixEnd(runPrefix(prefixRunCatalog, run.ID))); err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, &ErrCorrupt{Detail: fmt.Sprintf("run %d produced no archive parts", run.ID)}
	}

	return parts, nil
}

func archiveRecordRoute(key []byte) ([]byte, error) {
	if len(key) < 9 {
		return nil, &ErrCorrupt{Detail: "archive record key is truncated"}
	}
	var identity recordIdentity
	var err error
	switch key[0] {
	case prefixRunData:
		_, identity, _, err = decodeDataKey(key)
	case prefixRunCatalog:
		identity, err = decodeCatalogKey(key)
	default:
		return nil, &ErrCorrupt{Detail: fmt.Sprintf("unsupported archive record prefix 0x%x", key[0])}
	}
	if err != nil {
		return nil, &ErrCorrupt{Detail: fmt.Sprintf("decoding archive route: %v", err)}
	}
	route := []byte{key[0], byte(identity.Axis), byte(identity.Scope)}
	route = binary.BigEndian.AppendUint32(route, identity.LedgerID)

	return route, nil
}

func archiveRoutePrefix(runID uint64, route []byte) ([]byte, error) {
	if len(route) != 7 {
		return nil, fmt.Errorf("invariant: archive route has %d bytes, want 7", len(route))
	}
	prefix := runPrefix(route[0], runID)

	return append(prefix, route[1:]...), nil
}

func (s *tierManager) publishArchivedRun(expected RunRef, parts []ArchivePart) (bool, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.ensureNotQuarantined(); err != nil {
		return false, err
	}
	current, err := readManifest(s.db)
	if err != nil {
		return false, err
	}
	runIndex := -1
	for index, run := range current.Runs {
		if run.ID == expected.ID {
			runIndex = index

			break
		}
	}
	if runIndex < 0 {
		// Compaction superseded the run while its immutable blob uploaded. The
		// content-addressed remote orphan is harmless and may be reused later.
		return false, nil
	}
	run := current.Runs[runIndex]
	if run.Checksum != expected.Checksum || run.EntryCount != expected.EntryCount || run.IdentityCount != expected.IdentityCount {
		return false, &ErrSourceGap{Detail: fmt.Sprintf("run %d changed while it was archived", run.ID)}
	}
	if run.LocalRemoved {
		return false, nil
	}
	if run.Archived && !archivePartsEqual(run.ArchiveParts, parts) {
		err := &ErrCorrupt{Detail: fmt.Sprintf("run %d has two content-addressed archive references", run.ID)}
		if quarantineErr := s.setFailureLocked(failureQuarantined, err.Error()); quarantineErr != nil {
			return false, errors.Join(err, quarantineErr)
		}

		return false, err
	}
	changed := false
	if !run.Archived {
		// Phase one durably publishes the verified content addresses while all
		// local bytes remain intact. A crash can therefore never leave a cold-
		// only manifest whose remote identity was not durable locally first.
		run.Archived = true
		run.ArchiveParts = cloneArchiveParts(parts)
		run.LocalRemoved = false
		current, err = s.commitTieredRun(current, runIndex, run, false, pebble.Sync)
		if err != nil {
			return false, err
		}
		changed = true
	}

	s.leaseMu.Lock()
	removeLocal := s.runLeases[run.ID] == 0
	s.leaseMu.Unlock()
	if !removeLocal {
		return changed, nil
	}

	// Phase two atomically switches the manifest to cold-only and tombstones
	// local records. It may be asynchronous: losing this whole WAL batch merely
	// keeps the already-safe local copy for a later tier pass.
	run.LocalRemoved = true
	if _, err := s.commitTieredRun(current, runIndex, run, true, pebble.NoSync); err != nil {
		return false, err
	}

	return true, nil
}

func (s *tierManager) commitTieredRun(
	current Manifest,
	runIndex int,
	run RunRef,
	removeLocal bool,
	writeOptions *pebble.WriteOptions,
) (Manifest, error) {
	next := cloneManifest(current)
	next.Version++
	next.Runs[runIndex] = cloneRunRef(run)
	encodedManifest, err := encodeManifest(next)
	if err != nil {
		return Manifest{}, err
	}
	next, err = decodeManifest(encodedManifest)
	if err != nil {
		return Manifest{}, err
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if run.Archived {
		tiering := s.tiering.Load()
		if tiering == nil || tiering.archiveIdentity == "" {
			return Manifest{}, errors.New("invariant: archived run publication has no destination identity")
		}
		binding, found, err := readArchiveBindingRecord(s.db)
		if err != nil {
			return Manifest{}, err
		}
		if found && binding.DestinationIdentity != tiering.archiveIdentity {
			return Manifest{}, fmt.Errorf(
				"invariant: archive binding %q does not match active destination %q",
				binding.DestinationIdentity,
				tiering.archiveIdentity,
			)
		}
		if !found {
			binding = archiveBinding{
				FormatVersion:       archiveBindingFormatVersion,
				DestinationIdentity: tiering.archiveIdentity,
				MutationEpoch:       1,
			}
			encodedBinding, err := json.Marshal(binding)
			if err != nil {
				return Manifest{}, fmt.Errorf("marshaling staged balance history archive binding: %w", err)
			}
			if err := batch.Set(archiveBindingKey(), encodedBinding, nil); err != nil {
				return Manifest{}, fmt.Errorf("staging balance history archive binding: %w", err)
			}
		}
	}
	if removeLocal {
		for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
			prefix := runPrefix(kind, run.ID)
			if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
				return Manifest{}, fmt.Errorf("staging archived run %d local deletion: %w", run.ID, err)
			}
		}
		if err := batch.Delete(runMetaKey(run.ID), nil); err != nil {
			return Manifest{}, fmt.Errorf("staging archived run %d metadata deletion: %w", run.ID, err)
		}
	} else {
		encodedRun, err := json.Marshal(run)
		if err != nil {
			return Manifest{}, fmt.Errorf("marshaling archived run %d metadata: %w", run.ID, err)
		}
		if err := batch.Set(runMetaKey(run.ID), encodedRun, nil); err != nil {
			return Manifest{}, fmt.Errorf("staging archived run %d metadata: %w", run.ID, err)
		}
	}
	if err := batch.Set(manifestKey(next.Version), encodedManifest, nil); err != nil {
		return Manifest{}, fmt.Errorf("staging archived run manifest: %w", err)
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], next.Version)
	if err := batch.Set(latestManifestKey(), version[:], nil); err != nil {
		return Manifest{}, fmt.Errorf("staging archived run manifest pointer: %w", err)
	}
	if err := batch.Commit(writeOptions); err != nil {
		return Manifest{}, fmt.Errorf("committing archived run manifest: %w", err)
	}
	s.signalChanged()

	return next, nil
}

func (s *tierManager) failArchive(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, balancehistoryarchive.ErrMissing) {
		return &ErrSourceMissing{Detail: err.Error()}
	}
	if !errors.Is(err, balancehistoryarchive.ErrCorrupt) &&
		!errors.Is(err, balancehistoryarchive.ErrInvalidReference) &&
		!errors.Is(err, balancehistoryarchive.ErrUnsupportedFormat) &&
		!isIntegrityError(err) {
		return err
	}
	corrupt := &ErrCorrupt{Detail: err.Error()}
	if quarantineErr := s.Quarantine(corrupt.Error()); quarantineErr != nil {
		return errors.Join(corrupt, quarantineErr)
	}

	return corrupt
}

type runRecordStream struct {
	iters   []*pebble.Iterator
	index   int
	started bool
	record  balancehistoryarchive.Record
}

func newRunRecordStream(snapshot *pebble.Snapshot, runID uint64) (*runRecordStream, error) {
	stream := &runRecordStream{}
	for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
		prefix := runPrefix(kind, runID)
		iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			_ = stream.Close()

			return nil, fmt.Errorf("opening run %d archive iterator: %w", runID, err)
		}
		stream.iters = append(stream.iters, iter)
	}

	return stream, nil
}

func (s *runRecordStream) Next() bool {
	for s.index < len(s.iters) {
		iter := s.iters[s.index]
		var valid bool
		if s.started {
			valid = iter.Next()
		} else {
			s.started = true
			valid = iter.First()
		}
		if valid {
			s.record = balancehistoryarchive.Record{Key: iter.Key(), Value: iter.Value()}

			return true
		}
		s.index++
		s.started = false
	}
	s.record = balancehistoryarchive.Record{}

	return false
}

func (s *runRecordStream) Record() balancehistoryarchive.Record {
	return s.record
}

func (s *runRecordStream) Err() error {
	for _, iter := range s.iters {
		if err := iter.Error(); err != nil {
			return err
		}
	}

	return nil
}

func (s *runRecordStream) Close() error {
	var errs []error
	for _, iter := range s.iters {
		errs = append(errs, iter.Close())
	}
	s.iters = nil

	return errors.Join(errs...)
}

var _ balancehistoryarchive.RecordStream = (*runRecordStream)(nil)
