package balancehistorystore

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

type viewManager struct {
	*tierManager
}

// View pins one immutable manifest and the Pebble sequence that contains all
// of its runs. Later compaction may delete old run keys from the live DB, but
// Pebble retains them until this snapshot closes.
type View struct {
	ctx                context.Context
	store              *viewManager
	snapshot           *pebble.Snapshot
	manifest           Manifest
	generation         uint64
	allowSourceMissing bool
	coldRuns           map[uint64]*coldRunView
	close              sync.Once
	closeErr           error
}

type coldRunView struct {
	mu      sync.Mutex
	archive balancehistoryarchive.Archive
	parts   []coldPartView
}

type coldPartView struct {
	meta   ArchivePart
	lease  *balancehistoryarchive.Lease
	reader *balancehistoryarchive.IndexedReader
}

const viewContextCheckStride = 256

func (v *View) checkContext(index int) error {
	if index%viewContextCheckStride != 0 {
		return nil
	}

	return v.ctx.Err()
}

// SemanticDigest hashes the monetary history represented by this pinned view.
// Each physical run is decoded from cumulative values back into deltas, then
// all runs are merged by logical identity and timestamp. The digest therefore
// does not depend on run IDs, publication batching, compaction, or hot/cold
// placement. Memory is bounded by one decoded record per manifest run.
func (v *View) SemanticDigest(ctx context.Context) (digest [32]byte, retErr error) {
	if err := ctx.Err(); err != nil {
		return [32]byte{}, err
	}
	if err := v.ensureReadable(); err != nil {
		return [32]byte{}, err
	}

	cursors := make([]*semanticRunCursor, 0, len(v.manifest.Runs))
	defer func() {
		for _, cursor := range cursors {
			retErr = errors.Join(retErr, cursor.Close())
		}
	}()

	queue := make(semanticCursorHeap, 0, len(v.manifest.Runs))
	for _, run := range v.manifest.Runs {
		cursor, err := v.newSemanticRunCursor(ctx, run.ID)
		if err != nil {
			return [32]byte{}, err
		}
		cursors = append(cursors, cursor)

		valid, err := cursor.Advance(ctx)
		if err != nil {
			return [32]byte{}, err
		}
		if valid {
			heap.Push(&queue, cursor)
		}
	}

	hasher := sha256.New()
	writeSemanticField(hasher, []byte("formance.balance-history.semantic.v1"))
	for queue.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, err
		}

		cursor := heap.Pop(&queue).(*semanticRunCursor)
		key := append([]byte(nil), cursor.key...)
		input := new(big.Int).Set(cursor.input)
		output := new(big.Int).Set(cursor.output)
		if err := advanceSemanticCursor(ctx, &queue, cursor); err != nil {
			return [32]byte{}, err
		}

		for queue.Len() > 0 && bytes.Equal(queue[0].key, key) {
			duplicate := heap.Pop(&queue).(*semanticRunCursor)
			input.Add(input, duplicate.input)
			output.Add(output, duplicate.output)
			if err := advanceSemanticCursor(ctx, &queue, duplicate); err != nil {
				return [32]byte{}, err
			}
		}

		// A redundant zero-delta record has no effect on any served PIT view and
		// must therefore not make the semantic digest physical-layout-sensitive.
		if input.Sign() == 0 && output.Sign() == 0 {
			continue
		}
		writeSemanticField(hasher, key)
		writeSemanticField(hasher, input.Bytes())
		writeSemanticField(hasher, output.Bytes())
	}

	if err := v.ensureReadable(); err != nil {
		return [32]byte{}, err
	}
	copy(digest[:], hasher.Sum(nil))

	return digest, nil
}

func (v *View) newSemanticRunCursor(ctx context.Context, runID uint64) (*semanticRunCursor, error) {
	prefix := runPrefix(prefixRunData, runID)
	var records semanticRecordCursor
	if cold := v.coldRuns[runID]; cold != nil {
		records = newColdSemanticRecordCursor(ctx, v, cold, prefix)
	} else {
		iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return nil, fmt.Errorf("opening run %d for semantic digest: %w", runID, err)
		}
		records = &hotSemanticRecordCursor{iter: iter}
	}

	return &semanticRunCursor{view: v, runID: runID, records: records}, nil
}

// OpenView pins the latest published history. minLogSequence provides the same
// read-after-write contract as the live query path; the method fails closed
// instead of falling back to a partial or live result.
func (s *viewManager) OpenView(minLogSequence uint64) (*View, error) {
	return s.OpenViewContext(context.Background(), minLogSequence)
}

// OpenViewContext opens a view without touching the remote archive. Cold parts
// are fetched and verified lazily when a query intersects their bounded key
// range; the resulting archive leases remain held for the full View lifetime.
func (s *viewManager) OpenViewContext(ctx context.Context, minLogSequence uint64) (*View, error) {
	return s.openView(ctx, minLogSequence, false)
}

// OpenVerificationView bypasses persisted SOURCE_MISSING and REBUILDING
// markers so the verifier can certify repaired data before ClearFailure or
// CompleteRebuild. It never bypasses a terminal corruption quarantine,
// manifest completeness, watermark, or integrity.
func (s *viewManager) OpenVerificationView(ctx context.Context, minLogSequence uint64) (*View, error) {
	return s.openView(ctx, minLogSequence, true)
}

func (s *viewManager) openView(ctx context.Context, minLogSequence uint64, allowSourceMissing bool) (*View, error) {
	s.mutationMu.Lock()
	if err := s.viewFailure(allowSourceMissing); err != nil {
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
	if !manifest.SourceComplete {
		_ = snapshot.Close()
		s.mutationMu.Unlock()

		return nil, &ErrBuilding{Current: manifest.LogWatermark, Target: minLogSequence}
	}
	if manifest.LogWatermark < minLogSequence {
		_ = snapshot.Close()
		s.mutationMu.Unlock()

		return nil, &ErrBehind{Required: minLogSequence, Current: manifest.LogWatermark}
	}
	s.acquireManifestLease(manifest)
	view := &View{
		ctx:                ctx,
		store:              s,
		snapshot:           snapshot,
		manifest:           cloneManifest(manifest),
		generation:         s.generation.Load(),
		allowSourceMissing: allowSourceMissing,
		coldRuns:           make(map[uint64]*coldRunView),
	}
	tiering := s.tiering.Load()
	s.mutationMu.Unlock()

	for _, run := range manifest.Runs {
		if !run.LocalRemoved {
			continue
		}
		if err := ctx.Err(); err != nil {
			_ = view.Close()

			return nil, err
		}
		if !run.Archived || tiering == nil {
			_ = view.Close()

			return nil, &ErrSourceMissing{Detail: fmt.Sprintf("cold run %d is not configured or has no archive", run.ID)}
		}
		cold := &coldRunView{archive: tiering.archive, parts: make([]coldPartView, len(run.ArchiveParts))}
		for index, part := range run.ArchiveParts {
			cold.parts[index].meta = ArchivePart{
				Ref:        part.Ref,
				LowerBound: bytes.Clone(part.LowerBound),
				UpperBound: bytes.Clone(part.UpperBound),
			}
		}
		view.coldRuns[run.ID] = cold
	}

	if err := view.ensureReadable(); err != nil {
		_ = view.Close()

		return nil, err
	}

	return view, nil
}

func (s *viewManager) viewFailure(allowSourceMissing bool) error {
	failure := s.failure.Load()
	if failure == nil {
		return nil
	}
	if allowSourceMissing && (failure.kind == failureSourceMissing || failure.kind == failureRebuilding) {
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
	for _, run := range manifest.Runs {
		s.runLeases[run.ID]++
	}
}

func (s *viewManager) releaseManifestLease(manifest Manifest) error {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	if s.manifestLeases[manifest.Version] == 0 {
		return fmt.Errorf("invariant: manifest %d lease was released without an acquisition", manifest.Version)
	}
	for _, run := range manifest.Runs {
		if s.runLeases[run.ID] == 0 {
			return fmt.Errorf("invariant: run %d lease was released without an acquisition", run.ID)
		}
	}
	if err := decrementLease(s.manifestLeases, manifest.Version); err != nil {
		return err
	}
	for _, run := range manifest.Runs {
		if err := decrementLease(s.runLeases, run.ID); err != nil {
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
	if count <= 1 {
		delete(leases, id)

		return nil
	}
	leases[id] = count - 1

	return nil
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.AuditHash = append([]byte(nil), manifest.AuditHash...)
	runs := manifest.Runs
	manifest.Runs = make([]RunRef, len(runs))
	for index, run := range runs {
		manifest.Runs[index] = cloneRunRef(run)
	}
	manifest.ReducerState.Active = append([]balancehistory.IncarnationState(nil), manifest.ReducerState.Active...)
	manifest.ReducerState.Seen = append([]balancehistory.IncarnationState(nil), manifest.ReducerState.Seen...)

	return manifest
}

func (v *View) Manifest() Manifest {
	return cloneManifest(v.manifest)
}

func (v *View) Close() error {
	v.close.Do(func() {
		var errs []error
		for _, cold := range v.coldRuns {
			cold.mu.Lock()
			for index := range cold.parts {
				if cold.parts[index].reader != nil {
					errs = append(errs, cold.parts[index].reader.Close())
				}
				if cold.parts[index].lease != nil {
					errs = append(errs, cold.parts[index].lease.Close())
				}
			}
			cold.mu.Unlock()
		}
		errs = append(errs, v.snapshot.Close())
		errs = append(errs, v.store.releaseManifestLease(v.manifest))
		v.closeErr = errors.Join(errs...)
	})

	return v.closeErr
}

func (v *View) ensureReadable() error {
	if err := v.ctx.Err(); err != nil {
		return err
	}
	if err := v.store.viewFailure(v.allowSourceMissing); err != nil {
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

func (v *View) catalogIdentities(runID uint64, axis Axis, scope recordScope, ledgerID uint32, account *string) ([]recordIdentity, error) {
	prefix, err := catalogPrefix(runID, axis, scope, ledgerID, account)
	if err != nil {
		return nil, err
	}

	if cold := v.coldRuns[runID]; cold != nil {
		return v.coldCatalogIdentities(runID, cold, prefix)
	}

	iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, fmt.Errorf("opening balance history catalog iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	identities := make([]recordIdentity, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := v.checkContext(len(identities)); err != nil {
			return nil, err
		}
		identity, err := decodeCatalogKey(iter.Key())
		if err != nil {
			return nil, v.corrupt(fmt.Sprintf("decoding run %d catalog: %v", runID, err))
		}
		identities = append(identities, identity)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating balance history catalog: %w", err)
	}

	return identities, nil
}

func (v *View) coldCatalogIdentities(runID uint64, cold *coldRunView, prefix []byte) ([]recordIdentity, error) {
	cold.mu.Lock()
	defer cold.mu.Unlock()

	identities := make([]recordIdentity, 0)
	upper := prefixEnd(prefix)
	for partIndex := range cold.parts {
		if err := v.checkContext(partIndex); err != nil {
			return nil, err
		}
		if !partIntersects(cold.parts[partIndex].meta, prefix, upper) {
			continue
		}
		reader, err := v.coldPartReader(v.ctx, cold, partIndex)
		if err != nil {
			return nil, err
		}
		if !reader.SeekGE(prefix) {
			if err := reader.Err(); err != nil {
				return nil, v.store.failArchive(err)
			}

			continue
		}
		for {
			if err := v.checkContext(len(identities)); err != nil {
				return nil, err
			}
			record := reader.Record()
			if !bytes.HasPrefix(record.Key, prefix) {
				break
			}
			identity, err := decodeCatalogKey(record.Key)
			if err != nil {
				return nil, v.corrupt(fmt.Sprintf("decoding cold run %d catalog: %v", runID, err))
			}
			if len(record.Value) != 0 {
				return nil, v.corrupt(fmt.Sprintf("cold run %d catalog value is not empty", runID))
			}
			identities = append(identities, identity)
			if !reader.Next() {
				if err := reader.Err(); err != nil {
					return nil, v.store.failArchive(err)
				}

				break
			}
		}
	}

	return identities, nil
}

func (v *View) catalogIdentitiesByAccountPrefix(runID uint64, axis Axis, ledgerID uint32, accountPrefix string) ([]recordIdentity, error) {
	prefix, err := catalogAccountPrefix(runID, axis, ledgerID, accountPrefix)
	if err != nil {
		return nil, err
	}
	if cold := v.coldRuns[runID]; cold != nil {
		identities, err := v.coldCatalogIdentities(runID, cold, prefix)
		if err != nil {
			return nil, err
		}
		filtered := identities[:0]
		for index, identity := range identities {
			if err := v.checkContext(index); err != nil {
				return nil, err
			}
			if strings.HasPrefix(identity.Account, accountPrefix) {
				filtered = append(filtered, identity)
			}
		}

		return filtered, nil
	}

	iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, fmt.Errorf("opening balance history prefix catalog iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	identities := make([]recordIdentity, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := v.checkContext(len(identities)); err != nil {
			return nil, err
		}
		identity, err := decodeCatalogKey(iter.Key())
		if err != nil {
			return nil, v.corrupt(fmt.Sprintf("decoding run %d prefix catalog: %v", runID, err))
		}
		identities = append(identities, identity)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating balance history prefix catalog: %w", err)
	}

	return identities, nil
}

func (v *View) readRunValue(runID uint64, identity recordIdentity, timestamp uint64) (cumulativeValue, bool, error) {
	prefix, err := dataIdentityPrefix(runID, identity)
	if err != nil {
		return cumulativeValue{}, false, err
	}

	seek := binary.BigEndian.AppendUint64(append([]byte(nil), prefix...), timestamp)
	seek = append(seek, 0)
	if cold := v.coldRuns[runID]; cold != nil {
		cold.mu.Lock()
		defer cold.mu.Unlock()

		for partIndex := range slices.Backward(cold.parts) {
			part := cold.parts[partIndex].meta
			if !partIntersects(part, prefix, prefixEnd(prefix)) || bytes.Compare(part.LowerBound, seek) >= 0 {
				continue
			}
			reader, err := v.coldPartReader(v.ctx, cold, partIndex)
			if err != nil {
				return cumulativeValue{}, false, err
			}
			if !reader.SeekLT(seek) || !bytes.HasPrefix(reader.Record().Key, prefix) {
				if err := reader.Err(); err != nil {
					return cumulativeValue{}, false, v.store.failArchive(err)
				}

				continue
			}
			value, err := decodeCumulative(reader.Record().Value)
			if err != nil {
				return cumulativeValue{}, false, v.corrupt(fmt.Sprintf("decoding cold run %d value: %v", runID, err))
			}

			return value, true, nil
		}

		return cumulativeValue{}, false, nil
	}

	iter, err := v.snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return cumulativeValue{}, false, fmt.Errorf("opening balance history value iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.SeekLT(seek) || !bytes.HasPrefix(iter.Key(), prefix) {
		return cumulativeValue{}, false, nil
	}

	value, err := decodeCumulative(iter.Value())
	if err != nil {
		return cumulativeValue{}, false, v.corrupt(fmt.Sprintf("decoding run %d value: %v", runID, err))
	}

	return value, true, nil
}

func (v *View) coldPartReader(
	ctx context.Context,
	cold *coldRunView,
	partIndex int,
) (*balancehistoryarchive.IndexedReader, error) {
	part := &cold.parts[partIndex]
	if part.reader != nil {
		return part.reader, nil
	}
	lease, err := cold.archive.Fetch(ctx, part.meta.Ref)
	if err != nil {
		mapped := mapArchiveError(err)
		var missing *ErrSourceMissing
		if errors.As(mapped, &missing) {
			return nil, errors.Join(mapped, v.store.MarkSourceMissing(mapped.Error()))
		}

		return nil, v.store.failArchive(mapped)
	}
	reader, err := lease.OpenIndexed()
	if err != nil {
		_ = lease.Close()

		return nil, v.store.failArchive(err)
	}
	part.lease = lease
	part.reader = reader

	return reader, nil
}

func partIntersects(part ArchivePart, lower, upper []byte) bool {
	if len(upper) != 0 && bytes.Compare(part.LowerBound, upper) >= 0 {
		return false
	}
	if len(part.UpperBound) != 0 && bytes.Compare(part.UpperBound, lower) <= 0 {
		return false
	}

	return true
}

// AggregateAll uses ledger-wide asset summaries, avoiding account enumeration.
func (v *View) AggregateAll(ledgerID uint32, axis Axis, timestamp uint64) ([]AssetVolume, error) {
	if err := v.ensureReadable(); err != nil {
		return nil, err
	}
	if !axis.valid() {
		return nil, fmt.Errorf("invalid balance history axis %d", axis)
	}
	if err := v.checkFloor(axis, timestamp); err != nil {
		return nil, err
	}

	totals := make(map[recordIdentity]cumulativeValue)
	for runIndex, run := range v.manifest.Runs {
		if err := v.checkContext(runIndex); err != nil {
			return nil, err
		}
		identities, err := v.catalogIdentities(run.ID, axis, scopeAsset, ledgerID, nil)
		if err != nil {
			return nil, err
		}
		for identityIndex, identity := range identities {
			if err := v.checkContext(identityIndex); err != nil {
				return nil, err
			}
			value, found, err := v.readRunValue(run.ID, identity, timestamp)
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}

			total, ok := totals[identity]
			if !ok {
				total = newCumulativeValue()
			}
			total.add(value.input, value.output)
			totals[identity] = total
		}
	}

	result := make([]AssetVolume, 0, len(totals))
	for identity, total := range totals {
		result = append(result, AssetVolume{
			AssetBase:      identity.AssetBase,
			AssetPrecision: identity.AssetPrecision,
			Color:          identity.Color,
			Input:          new(big.Int).Set(total.input),
			Output:         new(big.Int).Set(total.output),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return assetVolumeLess(result[i], result[j])
	})

	return result, nil
}

func assetVolumeLess(left, right AssetVolume) bool {
	if left.AssetBase != right.AssetBase {
		return left.AssetBase < right.AssetBase
	}
	if left.AssetPrecision != right.AssetPrecision {
		return left.AssetPrecision < right.AssetPrecision
	}

	return left.Color < right.Color
}

// ReadVolumes returns per-account historical volumes. A nil accounts slice
// enumerates every identity; a non-nil slice restricts to those exact accounts.
func (v *View) ReadVolumes(ledgerID uint32, axis Axis, timestamp uint64, accounts []string) ([]Volume, error) {
	if err := v.ensureReadable(); err != nil {
		return nil, err
	}
	if !axis.valid() {
		return nil, fmt.Errorf("invalid balance history axis %d", axis)
	}
	if err := v.checkFloor(axis, timestamp); err != nil {
		return nil, err
	}

	requested := accounts
	if accounts != nil {
		seen := make(map[string]struct{}, len(accounts))
		requested = make([]string, 0, len(accounts))
		for accountIndex, account := range accounts {
			if err := v.checkContext(accountIndex); err != nil {
				return nil, err
			}
			if _, ok := seen[account]; ok {
				continue
			}
			seen[account] = struct{}{}
			requested = append(requested, account)
		}
	}

	totals := make(map[recordIdentity]cumulativeValue)
	for runIndex, run := range v.manifest.Runs {
		if err := v.checkContext(runIndex); err != nil {
			return nil, err
		}
		if requested == nil {
			identities, err := v.catalogIdentities(run.ID, axis, scopeVolume, ledgerID, nil)
			if err != nil {
				return nil, err
			}
			if err := v.addRunVolumes(run.ID, timestamp, identities, totals); err != nil {
				return nil, err
			}

			continue
		}

		for accountIndex, account := range requested {
			if err := v.checkContext(accountIndex); err != nil {
				return nil, err
			}
			identities, err := v.catalogIdentities(run.ID, axis, scopeVolume, ledgerID, &account)
			if err != nil {
				return nil, err
			}
			if err := v.addRunVolumes(run.ID, timestamp, identities, totals); err != nil {
				return nil, err
			}
		}
	}

	return volumesFromTotals(totals), nil
}

// ReadVolumesByPrefix returns historical volumes whose account address starts
// with accountPrefix. The account key encoding preserves raw prefixes, so the
// method performs one bounded catalog range seek per manifest run instead of
// enumerating every historical account identity.
func (v *View) ReadVolumesByPrefix(ledgerID uint32, axis Axis, timestamp uint64, accountPrefix string) ([]Volume, error) {
	if err := v.ensureReadable(); err != nil {
		return nil, err
	}
	if !axis.valid() {
		return nil, fmt.Errorf("invalid balance history axis %d", axis)
	}
	if err := v.checkFloor(axis, timestamp); err != nil {
		return nil, err
	}

	totals := make(map[recordIdentity]cumulativeValue)
	for runIndex, run := range v.manifest.Runs {
		if err := v.checkContext(runIndex); err != nil {
			return nil, err
		}
		identities, err := v.catalogIdentitiesByAccountPrefix(run.ID, axis, ledgerID, accountPrefix)
		if err != nil {
			return nil, err
		}
		if err := v.addRunVolumes(run.ID, timestamp, identities, totals); err != nil {
			return nil, err
		}
	}

	return volumesFromTotals(totals), nil
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

func (v *View) checkFloor(axis Axis, timestamp uint64) error {
	floor := v.manifest.EffectiveFloor
	if axis == AxisInsertion {
		floor = v.manifest.InsertionFloor
	}
	if timestamp < floor {
		return &ErrExpired{Requested: timestamp, Floor: floor}
	}

	return nil
}

func (v *View) addRunVolumes(runID, timestamp uint64, identities []recordIdentity, totals map[recordIdentity]cumulativeValue) error {
	for identityIndex, identity := range identities {
		if err := v.checkContext(identityIndex); err != nil {
			return err
		}
		value, found, err := v.readRunValue(runID, identity, timestamp)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		total, ok := totals[identity]
		if !ok {
			total = newCumulativeValue()
		}
		total.add(value.input, value.output)
		totals[identity] = total
	}

	return nil
}
