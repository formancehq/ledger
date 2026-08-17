package balancehistorystore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

type verifier struct {
	*storeCore
}

// Verify checks only structural invariants of the rebuildable projection.
// Physical block checksums belong to Pebble; semantic authority belongs to the
// audit log. A malformed projection is quarantined and rebuilt by the builder.
func (s *verifier) Verify() error {
	return s.VerifyContext(context.Background())
}

func (s *verifier) VerifyContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ensureNotQuarantined(); err != nil {
		return err
	}
	err := s.verifyLatestContext(ctx, true)
	if err == nil || !isIntegrityError(err) {
		return err
	}
	if quarantineErr := s.Quarantine(err.Error()); quarantineErr != nil {
		return errors.Join(err, quarantineErr)
	}

	return err
}

func (s *verifier) verifyLatest() error {
	return s.verifyLatestContext(context.Background(), false)
}

func (s *verifier) verifyLatestContext(ctx context.Context, verifyRecords bool) error {
	snapshot := s.db.NewSnapshot()
	defer func() { _ = snapshot.Close() }()

	manifest, err := readManifest(snapshot)
	if err != nil {
		return err
	}
	if err := verifyManifestStructure(manifest); err != nil {
		return err
	}
	seen := make(map[uint64]struct{}, len(manifest.Segments))
	var maxSegmentID uint64
	for _, segment := range manifest.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, exists := seen[segment.ID]; exists {
			return &ErrCorrupt{Detail: fmt.Sprintf("segment %d is referenced more than once", segment.ID)}
		}
		seen[segment.ID] = struct{}{}
		maxSegmentID = max(maxSegmentID, segment.ID)
		if err := verifySegmentDescriptor(manifest, segment); err != nil {
			return err
		}
		if err := verifyStoredSegmentMetadata(snapshot, segment); err != nil {
			return err
		}
		if verifyRecords {
			if err := verifyStoredSegmentRecords(ctx, snapshot, segment); err != nil {
				return err
			}
		}
	}
	if manifest.NextSegmentID == 0 || manifest.NextSegmentID <= maxSegmentID {
		return &ErrCorrupt{Detail: fmt.Sprintf("next segment id %d does not follow maximum referenced segment %d", manifest.NextSegmentID, maxSegmentID)}
	}

	return nil
}

func verifyStoredSegmentRecords(ctx context.Context, snapshot *pebble.Snapshot, segment SegmentRef) error {
	catalog := make(map[recordIdentity]struct{}, segment.IdentityCount)
	catalogPrefix := runPrefix(prefixRunCatalog, segment.ID)
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: catalogPrefix, UpperBound: prefixEnd(catalogPrefix)})
	if err != nil {
		return fmt.Errorf("opening segment %d catalog: %w", segment.ID, err)
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()

			return err
		}
		identity, err := decodeCatalogKey(iter.Key())
		if err != nil {
			_ = iter.Close()

			return &ErrCorrupt{Detail: fmt.Sprintf("segment %d catalog key cannot be decoded: %v", segment.ID, err)}
		}
		catalog[identity] = struct{}{}
	}
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		return fmt.Errorf("iterating segment %d catalog: %w", segment.ID, err)
	}
	if uint64(len(catalog)) != segment.IdentityCount {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d identity count is %d, expected %d", segment.ID, len(catalog), segment.IdentityCount)}
	}

	dataPrefix := runPrefix(prefixRunData, segment.ID)
	iter, err = snapshot.NewIter(&pebble.IterOptions{LowerBound: dataPrefix, UpperBound: prefixEnd(dataPrefix)})
	if err != nil {
		return fmt.Errorf("opening segment %d data: %w", segment.ID, err)
	}
	var count uint64
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()

			return err
		}
		runID, identity, _, err := decodeDataKey(iter.Key())
		if err != nil || runID != segment.ID {
			_ = iter.Close()

			return &ErrCorrupt{Detail: fmt.Sprintf("segment %d data key cannot be decoded: %v", segment.ID, err)}
		}
		if _, ok := catalog[identity]; !ok {
			_ = iter.Close()

			return &ErrCorrupt{Detail: fmt.Sprintf("segment %d data row has no catalog identity", segment.ID)}
		}
		if _, err := decodeCumulative(iter.Value()); err != nil {
			_ = iter.Close()

			return &ErrCorrupt{Detail: fmt.Sprintf("segment %d cumulative value cannot be decoded: %v", segment.ID, err)}
		}
		count++
	}
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		return fmt.Errorf("iterating segment %d data: %w", segment.ID, err)
	}
	if count != segment.EntryCount {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d entry count is %d, expected %d", segment.ID, count, segment.EntryCount)}
	}

	return nil
}

func verifyManifestStructure(manifest Manifest) error {
	if manifest.Version == 0 && (manifest.AuditWatermark != 0 || manifest.LogWatermark != 0 || len(manifest.Segments) != 0) {
		return &ErrCorrupt{Detail: "initial manifest contains published coverage or segments"}
	}
	if !slices.IsSorted(manifest.Ledgers) || hasDuplicateStrings(manifest.Ledgers) {
		return &ErrCorrupt{Detail: "configured ledger names are not unique and sorted"}
	}
	if _, err := balancehistory.NewReducerFromState(manifest.ReducerState); err != nil {
		return &ErrCorrupt{Detail: fmt.Sprintf("invalid reducer state: %v", err)}
	}
	if manifest.ReducerState.HasLast &&
		(manifest.ReducerState.Last.AuditSequence > manifest.AuditWatermark || manifest.ReducerState.Last.LogSequence > manifest.LogWatermark) {
		return &ErrCorrupt{Detail: "reducer cursor exceeds manifest watermarks"}
	}

	return nil
}

func hasDuplicateStrings(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}

	return false
}

func verifySegmentDescriptor(manifest Manifest, segment SegmentRef) error {
	if segment.ID == 0 {
		return &ErrCorrupt{Detail: "manifest references segment id zero"}
	}
	if segment.FirstAuditSequence == 0 || segment.FirstAuditSequence > segment.LastAuditSequence || segment.LastAuditSequence > manifest.AuditWatermark {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d has invalid audit coverage", segment.ID)}
	}
	if segment.MaxLogSequence == 0 || segment.MaxLogSequence > manifest.LogWatermark {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d has invalid log coverage", segment.ID)}
	}
	if segment.EntryCount == 0 || segment.IdentityCount == 0 {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d is empty", segment.ID)}
	}

	return nil
}

func verifyStoredSegmentMetadata(snapshot *pebble.Snapshot, segment SegmentRef) error {
	encoded, closer, err := snapshot.Get(runMetaKey(segment.ID))
	if err != nil {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d metadata is missing: %v", segment.ID, err)}
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return fmt.Errorf("closing segment %d metadata: %w", segment.ID, err)
	}
	var stored SegmentRef
	if err := json.Unmarshal(copyEncoded, &stored); err != nil {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d metadata cannot be decoded: %v", segment.ID, err)}
	}
	if stored != segment {
		return &ErrCorrupt{Detail: fmt.Sprintf("segment %d metadata differs from manifest", segment.ID)}
	}

	return nil
}
