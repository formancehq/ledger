package balancehistorystore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

type verifier struct {
	*storeCore
}

// Verify checks the current manifest, reducer cursor, every referenced run
// descriptor, and every immutable data/catalog byte. An integrity failure is
// persisted as a quarantine before it is returned, so later reads cannot serve
// a projection that this process already knows is invalid.
func (s *verifier) Verify() error {
	return s.VerifyContext(context.Background())
}

// VerifyContext performs the full hot and cold projection verification. Store
// startup deliberately calls verifyLatest instead: dependency injection wires
// the archive after New, while a public verification must certify every
// archived data and catalog part before declaring the projection healthy.
func (s *verifier) VerifyContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ensureNotQuarantined(); err != nil {
		return err
	}

	err := s.verifyLatestContext(ctx)
	if err == nil {
		err = s.verifyArchivedRuns(ctx)
	}

	return s.finishVerification(err)
}

// VerificationStats reports the bounded physical archive work performed by
// VerifyBoundedContext. NextOffset is an opaque circular cursor over the
// current manifest; callers persist it only for scheduling fairness.
type VerificationStats struct {
	ManifestVersion uint64
	HotRuns         int
	HotBytes        uint64
	ArchiveParts    int
	ArchiveBytes    uint64
	NextOffset      uint64
	Complete        bool
}

// VerifyBoundedContext verifies all manifest/run descriptors, then at most
// maxArchiveParts physical targets. A hot target checks its first/last data
// and catalog records; a cold target verifies one complete bounded blob. This
// is a rotating health sample, not a certification pass: VerifyContext remains
// the full checksum/count/catalog verification.
func (s *verifier) VerifyBoundedContext(
	ctx context.Context,
	startOffset uint64,
	maxArchiveParts int,
) (VerificationStats, error) {
	if maxArchiveParts <= 0 {
		return VerificationStats{}, errors.New("maximum archive parts must be positive")
	}
	if err := s.ensureNotQuarantined(); err != nil {
		return VerificationStats{}, err
	}
	snapshot := s.db.NewSnapshot()
	defer func() { _ = snapshot.Close() }()
	manifest, err := verifyManifestSnapshot(ctx, snapshot, false)
	if err != nil {
		return VerificationStats{}, s.finishVerification(err)
	}
	stats := VerificationStats{ManifestVersion: manifest.Version}
	targets := flattenVerificationTargets(manifest.Runs)
	if len(targets) == 0 {
		stats.Complete = true

		return stats, nil
	}
	tiering := s.tiering.Load()

	start := int(startOffset % uint64(len(targets)))
	limit := min(maxArchiveParts, len(targets))
	for checked := range limit {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		target := targets[(start+checked)%len(targets)]
		if target.hot {
			bytesChecked, err := verifyHotRunSample(ctx, snapshot, target.run)
			if err != nil {
				return stats, s.finishVerification(err)
			}
			stats.HotRuns++
			stats.HotBytes = saturatingAdd(stats.HotBytes, bytesChecked)

			continue
		}
		if tiering == nil {
			return stats, &ErrSourceMissing{Detail: "archived runs cannot be sampled without an archive"}
		}
		verifier := newRunRecordVerifier(target.run.ID)
		var previousKey []byte
		if err := verifyArchivedPart(
			ctx,
			tiering.archive,
			target.run.ID,
			target.partIndex,
			target.part,
			verifier,
			&previousKey,
		); err != nil {
			return stats, s.finishVerification(mapArchiveError(err))
		}
		stats.ArchiveParts++
		stats.ArchiveBytes = saturatingAdd(stats.ArchiveBytes, target.part.Ref.Size)
	}
	stats.NextOffset = uint64((start + limit) % len(targets))
	stats.Complete = limit == len(targets)

	return stats, nil
}

type verificationTarget struct {
	run       RunRef
	hot       bool
	partIndex int
	part      ArchivePart
}

func flattenVerificationTargets(runs []RunRef) []verificationTarget {
	var count int
	for _, run := range runs {
		if !run.LocalRemoved {
			count++
		}
		if run.Archived {
			count += len(run.ArchiveParts)
		}
	}
	targets := make([]verificationTarget, 0, count)
	for _, run := range runs {
		if !run.LocalRemoved {
			targets = append(targets, verificationTarget{run: run, hot: true})
		}
		if run.Archived {
			for partIndex, part := range run.ArchiveParts {
				targets = append(targets, verificationTarget{run: run, partIndex: partIndex, part: part})
			}
		}
	}

	return targets
}

func saturatingAdd(left, right uint64) uint64 {
	if left > ^uint64(0)-right {
		return ^uint64(0)
	}

	return left + right
}

func verifyHotRunSample(
	ctx context.Context,
	snapshot *pebble.Snapshot,
	run RunRef,
) (uint64, error) {
	verifier := newRunRecordVerifier(run.ID)
	var checkedBytes uint64
	for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
		if err := ctx.Err(); err != nil {
			return checkedBytes, err
		}
		prefix := runPrefix(kind, run.ID)
		iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return checkedBytes, fmt.Errorf("opening run %d bounded verifier: %w", run.ID, err)
		}
		if !iter.First() {
			iterErr := iter.Error()
			closeErr := iter.Close()
			if iterErr != nil || closeErr != nil {
				return checkedBytes, errors.Join(iterErr, closeErr)
			}

			return checkedBytes, &ErrCorrupt{Detail: fmt.Sprintf("run %d sampled prefix 0x%x is empty", run.ID, kind)}
		}
		firstKey := bytes.Clone(iter.Key())
		firstValue := bytes.Clone(iter.Value())
		if err := verifier.add(firstKey, firstValue); err != nil {
			_ = iter.Close()

			return checkedBytes, err
		}
		checkedBytes = saturatingAdd(checkedBytes, uint64(len(firstKey)+len(firstValue)))
		if iter.Last() && !bytes.Equal(iter.Key(), firstKey) {
			if err := verifier.add(iter.Key(), iter.Value()); err != nil {
				_ = iter.Close()

				return checkedBytes, err
			}
			checkedBytes = saturatingAdd(checkedBytes, uint64(len(iter.Key())+len(iter.Value())))
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil || closeErr != nil {
			return checkedBytes, errors.Join(iterErr, closeErr)
		}
	}

	return checkedBytes, nil
}

func (s *verifier) finishVerification(err error) error {
	if err == nil || !isIntegrityError(err) {
		return err
	}
	if quarantineErr := s.Quarantine(err.Error()); quarantineErr != nil {
		return errors.Join(err, quarantineErr)
	}

	return err
}

func (s *verifier) verifyArchivedRuns(ctx context.Context) error {
	manifest, err := s.Manifest()
	if err != nil {
		return err
	}
	tiering := s.tiering.Load()
	for _, run := range manifest.Runs {
		if !run.Archived {
			continue
		}
		if tiering == nil {
			return &ErrSourceMissing{Detail: fmt.Sprintf("cold run %d cannot be verified without an archive", run.ID)}
		}
		if err := verifyArchivedRun(ctx, tiering.archive, run); err != nil {
			return mapArchiveError(err)
		}
	}

	return nil
}

func (s *verifier) verifyLatest() error {
	return s.verifyLatestContext(context.Background())
}

func (s *verifier) verifyLatestContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot := s.db.NewSnapshot()
	defer func() { _ = snapshot.Close() }()
	_, err := verifyManifestSnapshot(ctx, snapshot, true)

	return err
}

func verifyManifestSnapshot(
	ctx context.Context,
	snapshot *pebble.Snapshot,
	verifyHotContent bool,
) (Manifest, error) {
	manifest, err := readManifest(snapshot)
	if err != nil {
		return Manifest{}, err
	}
	if err := verifyManifestStructure(manifest); err != nil {
		return Manifest{}, err
	}

	seen := make(map[uint64]struct{}, len(manifest.Runs))
	maxRunID := uint64(0)
	for _, run := range manifest.Runs {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		if _, exists := seen[run.ID]; exists {
			return Manifest{}, &ErrCorrupt{Detail: fmt.Sprintf("run %d is referenced more than once", run.ID)}
		}
		seen[run.ID] = struct{}{}
		if run.ID > maxRunID {
			maxRunID = run.ID
		}

		if err := verifyRunDescriptor(manifest, run); err != nil {
			return Manifest{}, err
		}
		if run.LocalRemoved {
			// Cold verification is deferred until the archive dependency is
			// configured after Store.New. OpenView still fails SOURCE_MISSING
			// when that dependency is absent; it never falls back to partial data.
			continue
		}
		if err := verifyStoredRunMetadata(snapshot, run); err != nil {
			return Manifest{}, err
		}
		if verifyHotContent {
			if err := verifyStoredRunContentContext(ctx, snapshot, run); err != nil {
				return Manifest{}, err
			}
		}
	}
	if manifest.NextRunID == 0 || manifest.NextRunID <= maxRunID {
		return Manifest{}, &ErrCorrupt{Detail: fmt.Sprintf(
			"next run id %d does not follow maximum referenced run %d",
			manifest.NextRunID,
			maxRunID,
		)}
	}

	if err := verifyRunCoverage(manifest.Runs); err != nil {
		return Manifest{}, err
	}

	return manifest, nil
}

func verifyManifestStructure(manifest Manifest) error {
	if manifest.Version == 0 {
		if manifest.AuditWatermark != 0 || manifest.LogWatermark != 0 || len(manifest.Runs) != 0 {
			return &ErrCorrupt{Detail: "initial manifest contains published coverage or runs"}
		}
	}
	if manifest.EffectiveFloor != 0 || manifest.InsertionFloor != 0 {
		return &ErrCorrupt{Detail: "non-zero history floor is unsupported without a chain-bound or signed base import authority"}
	}
	if _, err := balancehistory.NewReducerFromState(manifest.ReducerState); err != nil {
		return &ErrCorrupt{Detail: fmt.Sprintf("invalid reducer state: %v", err)}
	}
	if manifest.ReducerState.HasLast &&
		(manifest.ReducerState.Last.AuditSequence > manifest.AuditWatermark ||
			manifest.ReducerState.Last.LogSequence > manifest.LogWatermark) {
		return &ErrCorrupt{Detail: "reducer cursor exceeds manifest watermarks"}
	}

	return nil
}

func verifyRunDescriptor(manifest Manifest, run RunRef) error {
	if run.ID == 0 {
		return &ErrCorrupt{Detail: "manifest references run id zero"}
	}
	if run.FirstAuditSequence == 0 ||
		run.FirstAuditSequence > run.LastAuditSequence ||
		run.LastAuditSequence > manifest.AuditWatermark {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d has invalid audit coverage", run.ID)}
	}
	if run.MaxLogSequence == 0 || run.MaxLogSequence > manifest.LogWatermark {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d has invalid log coverage", run.ID)}
	}
	if run.EntryCount == 0 || run.IdentityCount == 0 {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d is empty", run.ID)}
	}
	if run.Checksum == ([32]byte{}) {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d has an empty checksum", run.ID)}
	}
	if run.LocalRemoved && !run.Archived {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d was removed locally without an archive", run.ID)}
	}
	if run.Archived && len(run.ArchiveParts) == 0 {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d is archived without bounded parts", run.ID)}
	}
	if !run.Archived && len(run.ArchiveParts) != 0 {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d has archive parts without archived state", run.ID)}
	}
	if run.Archived {
		if err := verifyArchiveParts(run); err != nil {
			return err
		}
	}

	return nil
}

func verifyArchiveParts(run RunRef) error {
	var recordCount uint64
	var previousUpper []byte
	runLower := runPrefix(prefixRunData, run.ID)
	runUpper := prefixEnd(runPrefix(prefixRunCatalog, run.ID))

	for index, part := range run.ArchiveParts {
		if part.Ref.Version != balancehistoryarchive.FormatVersion ||
			part.Ref.SHA256 == ([32]byte{}) ||
			part.Ref.Size <= balancehistoryarchive.EmptyEncodedSize ||
			part.Ref.RecordCount == 0 {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d archive part %d has an invalid reference", run.ID, index)}
		}
		if len(part.LowerBound) == 0 || len(part.UpperBound) == 0 || bytes.Compare(part.LowerBound, part.UpperBound) >= 0 {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d archive part %d has invalid bounds", run.ID, index)}
		}
		if bytes.Compare(part.LowerBound, runLower) < 0 || bytes.Compare(part.UpperBound, runUpper) > 0 {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d archive part %d is outside the run keyspace", run.ID, index)}
		}
		if index > 0 && !bytes.Equal(previousUpper, part.LowerBound) {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d archive parts %d and %d are not contiguous", run.ID, index-1, index)}
		}
		if recordCount > ^uint64(0)-part.Ref.RecordCount {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d archive record count overflows", run.ID)}
		}
		recordCount += part.Ref.RecordCount
		previousUpper = part.UpperBound
	}
	if !bytes.Equal(previousUpper, runUpper) {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d archive parts do not cover the end of the run keyspace", run.ID)}
	}
	if recordCount != run.EntryCount+run.IdentityCount {
		return &ErrCorrupt{Detail: fmt.Sprintf(
			"run %d archive record count is %d, want %d",
			run.ID,
			recordCount,
			run.EntryCount+run.IdentityCount,
		)}
	}

	return nil
}

func verifyStoredRun(snapshot *pebble.Snapshot, run RunRef) error {
	return verifyStoredRunContext(context.Background(), snapshot, run)
}

func verifyStoredRunContext(ctx context.Context, snapshot *pebble.Snapshot, run RunRef) error {
	if err := verifyStoredRunMetadata(snapshot, run); err != nil {
		return err
	}

	return verifyStoredRunContentContext(ctx, snapshot, run)
}

func verifyStoredRunMetadata(snapshot *pebble.Snapshot, run RunRef) error {
	encoded, closer, err := snapshot.Get(runMetaKey(run.ID))
	if err != nil {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d metadata is missing: %v", run.ID, err)}
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return fmt.Errorf("closing run %d metadata: %w", run.ID, err)
	}
	stored, err := decodeRunRef(copyEncoded)
	if err != nil {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d metadata cannot be decoded: %v", run.ID, err)}
	}
	if !runRefsEqual(stored, run) {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d metadata differs from manifest", run.ID)}
	}

	return nil
}

func verifyStoredRunContentContext(ctx context.Context, snapshot *pebble.Snapshot, run RunRef) error {
	checksum, entries, identities, err := verifyRunRecordsContext(ctx, snapshot, run.ID)
	if err != nil {
		return err
	}
	if checksum != run.Checksum || entries != run.EntryCount || identities != run.IdentityCount {
		return &ErrCorrupt{Detail: fmt.Sprintf(
			"run %d integrity mismatch (entries=%d/%d identities=%d/%d checksum=%x/%x)",
			run.ID, entries, run.EntryCount, identities, run.IdentityCount, checksum, run.Checksum,
		)}
	}

	return nil
}

func decodeRunRef(encoded []byte) (RunRef, error) {
	var run RunRef
	if err := json.Unmarshal(encoded, &run); err != nil {
		return RunRef{}, err
	}

	return run, nil
}

func verifyRunRecordsContext(
	ctx context.Context,
	reader *pebble.Snapshot,
	runID uint64,
) ([32]byte, uint64, uint64, error) {
	verifier := newRunRecordVerifier(runID)

	for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
		prefix := runPrefix(kind, runID)
		iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return [32]byte{}, 0, 0, fmt.Errorf("opening run %d verifier iterator: %w", runID, err)
		}

		for valid := iter.First(); valid; valid = iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()

				return [32]byte{}, 0, 0, err
			}
			if err := verifier.add(iter.Key(), iter.Value()); err != nil {
				_ = iter.Close()

				return [32]byte{}, 0, 0, err
			}
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return [32]byte{}, 0, 0, fmt.Errorf("verifying run %d: %w", runID, iterErr)
		}
		if closeErr != nil {
			return [32]byte{}, 0, 0, fmt.Errorf("closing run %d verifier iterator: %w", runID, closeErr)
		}
	}

	return verifier.finish()
}

type runRecordVerifier struct {
	runID                 uint64
	digest                hash.Hash
	catalogIdentityDigest hash.Hash
	dataIdentityDigest    hash.Hash
	currentDataIdentity   recordIdentity
	previousValue         cumulativeValue
	previousKey           []byte
	entries               uint64
	identities            uint64
	dataIdentities        uint64
	hasDataIdentity       bool
}

func newRunRecordVerifier(runID uint64) *runRecordVerifier {
	return &runRecordVerifier{
		runID:                 runID,
		digest:                sha256.New(),
		catalogIdentityDigest: sha256.New(),
		dataIdentityDigest:    sha256.New(),
	}
}

func (v *runRecordVerifier) add(key, value []byte) error {
	if v.previousKey != nil && bytes.Compare(v.previousKey, key) >= 0 {
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d records are not strictly ordered", v.runID)}
	}
	v.previousKey = bytes.Clone(key)
	writeCanonicalRecordHash(v.digest, key, value)

	switch {
	case len(key) >= 9 && key[0] == prefixRunCatalog:
		if binary.BigEndian.Uint64(key[1:9]) != v.runID {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d has a catalog key for another run", v.runID)}
		}
		_, err := decodeCatalogKey(key)
		if err != nil {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d has invalid catalog key: %v", v.runID, err)}
		}
		if len(value) != 0 {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d catalog value is not empty", v.runID)}
		}
		writeSemanticField(v.catalogIdentityDigest, key[9:])
		v.identities++

		return nil
	case len(key) >= 9 && key[0] == prefixRunData:
		decodedRunID, identity, _, err := decodeDataKey(key)
		if err != nil || decodedRunID != v.runID {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d has invalid data key: %v", v.runID, err)}
		}
		current, err := decodeCumulative(value)
		if err != nil {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d has invalid cumulative value: %v", v.runID, err)}
		}
		if !v.hasDataIdentity || identity != v.currentDataIdentity {
			v.currentDataIdentity = identity
			v.previousValue = newCumulativeValue()
			v.hasDataIdentity = true
			v.dataIdentities++
			writeSemanticField(v.dataIdentityDigest, key[9:len(key)-8])
		}
		if current.input.Cmp(v.previousValue.input) < 0 || current.output.Cmp(v.previousValue.output) < 0 ||
			(current.input.Cmp(v.previousValue.input) == 0 && current.output.Cmp(v.previousValue.output) == 0) {
			return &ErrCorrupt{Detail: fmt.Sprintf("run %d cumulative timeline did not strictly increase", v.runID)}
		}
		v.previousValue = current.clone()
		v.entries++

		return nil
	default:
		return &ErrCorrupt{Detail: fmt.Sprintf("run %d contains a key outside data and catalog", v.runID)}
	}
}

func (v *runRecordVerifier) finish() ([32]byte, uint64, uint64, error) {
	if v.identities != v.dataIdentities ||
		!bytes.Equal(v.catalogIdentityDigest.Sum(nil), v.dataIdentityDigest.Sum(nil)) {
		return [32]byte{}, 0, 0, &ErrCorrupt{Detail: fmt.Sprintf("run %d catalog/data identity cardinality differs", v.runID)}
	}

	var checksum [32]byte
	copy(checksum[:], v.digest.Sum(nil))

	return checksum, v.entries, v.identities, nil
}

func verifyArchivedRun(
	ctx context.Context,
	archive balancehistoryarchive.Archive,
	run RunRef,
) error {
	verifier := newRunRecordVerifier(run.ID)
	var previousKey []byte
	for index, part := range run.ArchiveParts {
		if err := verifyArchivedPart(ctx, archive, run.ID, index, part, verifier, &previousKey); err != nil {
			return err
		}
	}

	checksum, entries, identities, err := verifier.finish()
	if err != nil {
		return err
	}
	if checksum != run.Checksum || entries != run.EntryCount || identities != run.IdentityCount {
		return &ErrCorrupt{Detail: fmt.Sprintf(
			"archived run %d integrity mismatch (entries=%d/%d identities=%d/%d checksum=%x/%x)",
			run.ID, entries, run.EntryCount, identities, run.IdentityCount, checksum, run.Checksum,
		)}
	}

	return nil
}

func verifyArchivedPart(
	ctx context.Context,
	archive balancehistoryarchive.Archive,
	runID uint64,
	partIndex int,
	part ArchivePart,
	verifier *runRecordVerifier,
	previousKey *[]byte,
) (retErr error) {
	lease, err := archive.Fetch(ctx, part.Ref)
	if err != nil {
		return err
	}
	reader, err := lease.Open()
	if err != nil {
		return errors.Join(err, lease.Close())
	}
	defer func() {
		retErr = errors.Join(retErr, reader.Close(), lease.Close())
	}()

	var firstKey, lastKey []byte
	recordCount := uint64(0)
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		record := reader.Record()
		if recordCount == 0 {
			firstKey = bytes.Clone(record.Key)
		}
		if bytes.Compare(record.Key, part.LowerBound) < 0 || bytes.Compare(record.Key, part.UpperBound) >= 0 {
			return &ErrCorrupt{Detail: fmt.Sprintf(
				"archived run %d part %d contains a record outside its bounds",
				runID,
				partIndex,
			)}
		}
		if *previousKey != nil && bytes.Compare(*previousKey, record.Key) >= 0 {
			return &ErrCorrupt{Detail: fmt.Sprintf("archived run %d parts are not strictly ordered", runID)}
		}
		if err := verifier.add(record.Key, record.Value); err != nil {
			return err
		}
		*previousKey = bytes.Clone(record.Key)
		lastKey = bytes.Clone(record.Key)
		recordCount++
	}
	if err := reader.Err(); err != nil {
		return err
	}
	if recordCount != part.Ref.RecordCount {
		return &ErrCorrupt{Detail: fmt.Sprintf(
			"archived run %d part %d record count is %d, want %d",
			runID,
			partIndex,
			recordCount,
			part.Ref.RecordCount,
		)}
	}
	route, err := archiveRecordRoute(firstKey)
	if err != nil {
		return err
	}
	routeLower, err := archiveRoutePrefix(runID, route)
	if err != nil {
		return err
	}
	if !bytes.Equal(firstKey, part.LowerBound) && !bytes.Equal(routeLower, part.LowerBound) {
		return &ErrCorrupt{Detail: fmt.Sprintf("archived run %d part %d lower bound differs from its first key range", runID, partIndex)}
	}
	if len(lastKey) == 0 || bytes.Compare(lastKey, part.UpperBound) >= 0 {
		return &ErrCorrupt{Detail: fmt.Sprintf("archived run %d part %d has an invalid final key", runID, partIndex)}
	}

	return nil
}

func mapArchiveError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, balancehistoryarchive.ErrMissing) {
		return &ErrSourceMissing{Detail: err.Error()}
	}
	if errors.Is(err, balancehistoryarchive.ErrCorrupt) ||
		errors.Is(err, balancehistoryarchive.ErrInvalidReference) ||
		errors.Is(err, balancehistoryarchive.ErrUnsupportedFormat) ||
		isIntegrityError(err) {
		return &ErrCorrupt{Detail: err.Error()}
	}

	return err
}

func verifyRunCoverage(runs []RunRef) error {
	ordered := append([]RunRef(nil), runs...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].FirstAuditSequence != ordered[j].FirstAuditSequence {
			return ordered[i].FirstAuditSequence < ordered[j].FirstAuditSequence
		}

		return ordered[i].ID < ordered[j].ID
	})
	for index := 1; index < len(ordered); index++ {
		if ordered[index].FirstAuditSequence <= ordered[index-1].LastAuditSequence {
			return &ErrCorrupt{Detail: fmt.Sprintf(
				"runs %d and %d have overlapping audit coverage",
				ordered[index-1].ID,
				ordered[index].ID,
			)}
		}
	}

	return nil
}
