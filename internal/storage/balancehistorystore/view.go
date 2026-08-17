package balancehistorystore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

type viewManager struct {
	*storeCore
}

// View pins one immutable manifest and the Pebble sequence containing all of
// its logical history segments. Later compaction cannot change the result.
type View struct {
	ctx        context.Context
	store      *viewManager
	snapshot   *pebble.Snapshot
	manifest   Manifest
	generation uint64
	close      sync.Once
	closeErr   error
}

const viewContextCheckStride = 256

func (v *View) checkContext(index int) error {
	if index%viewContextCheckStride != 0 {
		return nil
	}

	return v.ctx.Err()
}

func (v *View) newSemanticRunCursor(ctx context.Context, segmentID uint64) (*semanticRunCursor, error) {
	prefix := runPrefix(prefixRunData, segmentID)
	iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, fmt.Errorf("opening segment %d for compaction: %w", segmentID, err)
	}

	return &semanticRunCursor{
		view:    v,
		runID:   segmentID,
		records: &hotSemanticRecordCursor{iter: iter},
	}, nil
}

// OpenView pins the latest published history. minLogSequence provides the
// same read-after-write contract as the live query path.
func (s *viewManager) OpenView(minLogSequence uint64) (*View, error) {
	return s.OpenViewContext(context.Background(), minLogSequence)
}

func (s *viewManager) OpenViewContext(ctx context.Context, minLogSequence uint64) (*View, error) {
	return s.openView(ctx, minLogSequence)
}

func (s *viewManager) openView(ctx context.Context, minLogSequence uint64) (*View, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.viewFailure(); err != nil {
		return nil, err
	}
	snapshot := s.db.NewSnapshot()
	manifest, err := readManifest(snapshot)
	if err != nil {
		_ = snapshot.Close()

		return nil, err
	}
	if !manifest.SourceComplete {
		_ = snapshot.Close()

		return nil, &ErrBuilding{Current: manifest.LogWatermark, Target: minLogSequence}
	}
	if manifest.LogWatermark < minLogSequence {
		_ = snapshot.Close()

		return nil, &ErrBehind{Required: minLogSequence, Current: manifest.LogWatermark}
	}
	s.acquireManifestLease(manifest)
	view := &View{
		ctx:        ctx,
		store:      s,
		snapshot:   snapshot,
		manifest:   cloneManifest(manifest),
		generation: s.generation.Load(),
	}
	if err := view.ensureReadable(); err != nil {
		_ = view.Close()

		return nil, err
	}

	return view, nil
}

func (s *viewManager) viewFailure() error {
	failure := s.failure.Load()
	if failure == nil {
		return nil
	}
	if failure.kind == failureSourceMissing {
		return &ErrSourceMissing{Detail: failure.detail}
	}

	return &ErrQuarantined{Detail: failure.detail}
}

func (s *viewManager) acquireManifestLease(manifest Manifest) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	s.manifestLeases[manifest.Version]++
	for _, segment := range manifest.Segments {
		s.runLeases[segment.ID]++
	}
}

func (s *viewManager) releaseManifestLease(manifest Manifest) error {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	if s.manifestLeases[manifest.Version] == 0 {
		return fmt.Errorf("invariant: manifest %d lease was released without an acquisition", manifest.Version)
	}
	for _, segment := range manifest.Segments {
		if s.runLeases[segment.ID] == 0 {
			return fmt.Errorf("invariant: segment %d lease was released without an acquisition", segment.ID)
		}
	}
	if err := decrementLease(s.manifestLeases, manifest.Version); err != nil {
		return err
	}
	for _, segment := range manifest.Segments {
		if err := decrementLease(s.runLeases, segment.ID); err != nil {
			return err
		}
	}

	return nil
}

func decrementLease(leases map[uint64]uint64, id uint64) error {
	count := leases[id]
	if count == 0 {
		return fmt.Errorf("invariant: lease %d was decremented from zero", id)
	}
	if count == 1 {
		delete(leases, id)

		return nil
	}
	leases[id] = count - 1

	return nil
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.AuditHash = bytes.Clone(manifest.AuditHash)
	manifest.Ledgers = append([]string(nil), manifest.Ledgers...)
	manifest.Segments = append([]SegmentRef(nil), manifest.Segments...)
	manifest.ReducerState.Active = append([]balancehistory.IncarnationState(nil), manifest.ReducerState.Active...)
	manifest.ReducerState.Seen = append([]balancehistory.IncarnationState(nil), manifest.ReducerState.Seen...)
	manifest.ReducerState.Enabled = append([]string(nil), manifest.ReducerState.Enabled...)

	return manifest
}

func (v *View) Manifest() Manifest {
	return cloneManifest(v.manifest)
}

func (v *View) Close() error {
	v.close.Do(func() {
		v.closeErr = errors.Join(v.snapshot.Close(), v.store.releaseManifestLease(v.manifest))
	})

	return v.closeErr
}

func (v *View) ensureReadable() error {
	if err := v.ctx.Err(); err != nil {
		return err
	}
	if err := v.store.viewFailure(); err != nil {
		return err
	}
	if v.generation != v.store.generation.Load() {
		return &ErrSourceMissing{Detail: "pinned history view was invalidated by a reset or source-state change"}
	}

	return nil
}

func (v *View) corrupt(detail string) error {
	err := &ErrCorrupt{Detail: detail}
	if quarantineErr := v.store.Quarantine(err.Error()); quarantineErr != nil {
		return errors.Join(err, quarantineErr)
	}

	return err
}

func (v *View) catalogIdentities(
	iter *pebble.Iterator,
	segmentID uint64,
	temporality Temporality,
	ledgerName string,
	account *string,
) ([]recordIdentity, error) {
	prefix, err := catalogPrefix(segmentID, temporality, ledgerName, account)
	if err != nil {
		return nil, err
	}
	iter.SetBounds(prefix, prefixEnd(prefix))

	identities := make([]recordIdentity, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := v.checkContext(len(identities)); err != nil {
			return nil, err
		}
		identity, err := decodeCatalogKey(iter.Key())
		if err != nil {
			return nil, v.corrupt(fmt.Sprintf("decoding segment %d catalog: %v", segmentID, err))
		}
		identities = append(identities, identity)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating balance history catalog: %w", err)
	}

	return identities, nil
}

func (v *View) catalogIdentitiesByAccountPrefix(
	iter *pebble.Iterator,
	segmentID uint64,
	temporality Temporality,
	ledgerName, accountPrefix string,
) ([]recordIdentity, error) {
	prefix, err := catalogAccountPrefix(segmentID, temporality, ledgerName, accountPrefix)
	if err != nil {
		return nil, err
	}
	iter.SetBounds(prefix, prefixEnd(prefix))

	identities := make([]recordIdentity, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := v.checkContext(len(identities)); err != nil {
			return nil, err
		}
		identity, err := decodeCatalogKey(iter.Key())
		if err != nil {
			return nil, v.corrupt(fmt.Sprintf("decoding segment %d prefix catalog: %v", segmentID, err))
		}
		if strings.HasPrefix(identity.Account, accountPrefix) {
			identities = append(identities, identity)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating balance history prefix catalog: %w", err)
	}

	return identities, nil
}

func (v *View) readSegmentValue(
	iter *pebble.Iterator,
	segmentID uint64,
	identity recordIdentity,
	timestamp uint64,
) (cumulativeValue, bool, error) {
	prefix, err := dataIdentityPrefix(segmentID, identity)
	if err != nil {
		return cumulativeValue{}, false, err
	}
	seek := binary.BigEndian.AppendUint64(append([]byte(nil), prefix...), timestamp)
	seek = append(seek, 0)

	if !iter.SeekLT(seek) || !bytes.HasPrefix(iter.Key(), prefix) {
		return cumulativeValue{}, false, nil
	}
	value, err := decodeCumulative(iter.Value())
	if err != nil {
		return cumulativeValue{}, false, v.corrupt(fmt.Sprintf("decoding segment %d value: %v", segmentID, err))
	}

	return value, true, nil
}

// ReadVolumes returns per-account historical volumes. A nil accounts slice
// enumerates every account identity for the ledger.
func (v *View) ReadVolumes(ledgerName string, temporality Temporality, timestamp uint64, accounts []string) ([]Volume, error) {
	return v.ReadVolumesSelected(ledgerName, temporality, timestamp, accounts, nil)
}

// ReadVolumesSelected reads the union of exact accounts and account prefixes.
// A nil accounts slice with no prefixes means every historical account; when
// prefixes are present, nil accounts contributes no exact-account candidates.
// Each segment opens one reusable catalog iterator and one reusable data
// iterator regardless of candidate cardinality.
func (v *View) ReadVolumesSelected(
	ledgerName string,
	temporality Temporality,
	timestamp uint64,
	accounts []string,
	accountPrefixes []string,
) ([]Volume, error) {
	if err := v.ensureReadable(); err != nil {
		return nil, err
	}
	if !temporality.valid() {
		return nil, fmt.Errorf("invalid balance history temporality %d", temporality)
	}

	requested := deduplicateSelection(accounts)
	prefixes := deduplicateSelection(accountPrefixes)
	fullScan := accounts == nil && len(prefixes) == 0

	totals := make(map[recordIdentity]cumulativeValue)
	for segmentIndex, segment := range v.manifest.Segments {
		if err := v.checkContext(segmentIndex); err != nil {
			return nil, err
		}
		identities, err := v.readSegmentCatalogSelection(
			segment.ID,
			temporality,
			ledgerName,
			requested,
			prefixes,
			fullScan,
		)
		if err != nil {
			return nil, err
		}
		if err := v.addSegmentVolumes(segment.ID, timestamp, identities, totals); err != nil {
			return nil, err
		}
	}

	return volumesFromTotals(totals), nil
}

func (v *View) ReadVolumesByPrefix(ledgerName string, temporality Temporality, timestamp uint64, accountPrefix string) ([]Volume, error) {
	return v.ReadVolumesSelected(ledgerName, temporality, timestamp, nil, []string{accountPrefix})
}

func (v *View) readSegmentCatalogSelection(
	segmentID uint64,
	temporality Temporality,
	ledgerName string,
	accounts, accountPrefixes []string,
	fullScan bool,
) (identities []recordIdentity, err error) {
	prefix := runPrefix(prefixRunCatalog, segmentID)
	iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, fmt.Errorf("opening balance history segment %d catalog iterator: %w", segmentID, err)
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	if fullScan {
		return v.catalogIdentities(iter, segmentID, temporality, ledgerName, nil)
	}

	seen := make(map[recordIdentity]struct{})
	for _, account := range accounts {
		rows, err := v.catalogIdentities(iter, segmentID, temporality, ledgerName, &account)
		if err != nil {
			return nil, err
		}
		identities = appendUniqueIdentities(identities, rows, seen)
	}
	for _, accountPrefix := range accountPrefixes {
		rows, err := v.catalogIdentitiesByAccountPrefix(iter, segmentID, temporality, ledgerName, accountPrefix)
		if err != nil {
			return nil, err
		}
		identities = appendUniqueIdentities(identities, rows, seen)
	}

	return identities, nil
}

func deduplicateSelection(values []string) []string {
	if values == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}

	return unique
}

func appendUniqueIdentities(
	destination []recordIdentity,
	rows []recordIdentity,
	seen map[recordIdentity]struct{},
) []recordIdentity {
	for _, identity := range rows {
		if _, exists := seen[identity]; exists {
			continue
		}
		seen[identity] = struct{}{}
		destination = append(destination, identity)
	}

	return destination
}

func volumesFromTotals(totals map[recordIdentity]cumulativeValue) []Volume {
	result := make([]Volume, 0, len(totals))
	for identity, total := range totals {
		result = append(result, Volume{
			Account:        identity.Account,
			AssetBase:      identity.AssetBase,
			AssetPrecision: identity.AssetPrecision,
			Color:          identity.Color,
			Input:          new(big.Int).Set(total.input),
			Output:         new(big.Int).Set(total.output),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.Account != right.Account {
			return left.Account < right.Account
		}
		if left.AssetBase != right.AssetBase {
			return left.AssetBase < right.AssetBase
		}
		if left.AssetPrecision != right.AssetPrecision {
			return left.AssetPrecision < right.AssetPrecision
		}

		return left.Color < right.Color
	})

	return result
}

func (v *View) addSegmentVolumes(
	segmentID, timestamp uint64,
	identities []recordIdentity,
	totals map[recordIdentity]cumulativeValue,
) (err error) {
	if len(identities) == 0 {
		return nil
	}
	prefix := runPrefix(prefixRunData, segmentID)
	iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return fmt.Errorf("opening balance history segment %d value iterator: %w", segmentID, err)
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	for identityIndex, identity := range identities {
		if err := v.checkContext(identityIndex); err != nil {
			return err
		}
		value, found, err := v.readSegmentValue(iter, segmentID, identity, timestamp)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		total, exists := totals[identity]
		if !exists {
			total = newCumulativeValue()
		}
		total.add(value.input, value.output)
		totals[identity] = total
	}

	return nil
}
