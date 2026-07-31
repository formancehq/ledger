package balancehistorystore

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"slices"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

type publisher struct {
	*storeCore
}

type timedDelta struct {
	timestamp     uint64
	auditSequence uint64
	orderIndex    uint32
	logSequence   uint64
	input         *big.Int
	output        *big.Int
}

type runRecord struct {
	key   []byte
	value []byte
}

func effectIdentity(effect balancehistory.Effect, axis Axis, scope recordScope) recordIdentity {
	identity := recordIdentity{
		Axis:           axis,
		Scope:          scope,
		LedgerID:       effect.LedgerID,
		AssetBase:      effect.AssetBase,
		AssetPrecision: effect.AssetPrecision,
		Color:          effect.Color,
	}
	if scope == scopeVolume {
		identity.Account = effect.Account
	}

	return identity
}

func effectTimestamp(effect balancehistory.Effect, axis Axis) uint64 {
	if axis == AxisInsertion {
		return effect.InsertedAt
	}

	return effect.EffectiveAt
}

func buildRunRecords(runID uint64, effects []balancehistory.Effect) ([]runRecord, uint64, uint64, [32]byte, error) {
	groups := make(map[recordIdentity][]timedDelta, len(effects)*4)

	for _, effect := range effects {
		input := effect.Input.BigInt()
		output := effect.Output.BigInt()
		for _, axis := range []Axis{AxisEffective, AxisInsertion} {
			for _, scope := range []recordScope{scopeVolume, scopeAsset} {
				identity := effectIdentity(effect, axis, scope)
				groups[identity] = append(groups[identity], timedDelta{
					timestamp:     effectTimestamp(effect, axis),
					auditSequence: effect.AuditSequence,
					orderIndex:    effect.OrderIndex,
					logSequence:   effect.LogSequence,
					input:         new(big.Int).Set(input),
					output:        new(big.Int).Set(output),
				})
			}
		}
	}

	return buildRunRecordsFromGroups(runID, groups)
}

func buildRunRecordsFromGroups(runID uint64, groups map[recordIdentity][]timedDelta) ([]runRecord, uint64, uint64, [32]byte, error) {
	compareIdentities := func(left, right recordIdentity) int {
		return firstNonZero(
			cmp.Compare(left.Axis, right.Axis),
			cmp.Compare(left.Scope, right.Scope),
			cmp.Compare(left.LedgerID, right.LedgerID),
			cmp.Compare(left.Account, right.Account),
			cmp.Compare(left.AssetBase, right.AssetBase),
			cmp.Compare(left.AssetPrecision, right.AssetPrecision),
			cmp.Compare(left.Color, right.Color),
		)
	}

	type encodedIdentity struct {
		identity   recordIdentity
		catalogKey []byte
	}
	encodedIdentities := make([]encodedIdentity, 0, len(groups))
	var (
		encodingFailure         error
		encodingFailureIdentity recordIdentity
	)
	for identity := range groups {
		key, err := catalogKey(runID, identity)
		if err != nil {
			// Map iteration is random. Select the smallest invalid identity so
			// every build returns the same encoding error before sorting records.
			if encodingFailure == nil || compareIdentities(identity, encodingFailureIdentity) < 0 {
				encodingFailure = err
				encodingFailureIdentity = identity
			}

			continue
		}
		encodedIdentities = append(encodedIdentities, encodedIdentity{
			identity:   identity,
			catalogKey: key,
		})
	}
	if encodingFailure != nil {
		return nil, 0, 0, [32]byte{}, fmt.Errorf("encoding balance history catalog key: %w", encodingFailure)
	}
	slices.SortFunc(encodedIdentities, func(left, right encodedIdentity) int {
		return bytes.Compare(left.catalogKey, right.catalogKey)
	})

	recordCapacity := len(groups)
	for _, deltas := range groups {
		recordCapacity += len(deltas)
	}
	records := make([]runRecord, 0, recordCapacity)
	var dataCount uint64
	for _, encoded := range encodedIdentities {
		identity := encoded.identity
		records = append(records, runRecord{key: encoded.catalogKey})

		deltas := groups[identity]
		slices.SortFunc(deltas, func(a, b timedDelta) int {
			return firstNonZero(
				compareUint64(a.timestamp, b.timestamp),
				compareUint64(a.auditSequence, b.auditSequence),
				compareUint32(a.orderIndex, b.orderIndex),
				compareUint64(a.logSequence, b.logSequence),
			)
		})

		cumulative := newCumulativeValue()
		for index := 0; index < len(deltas); {
			timestamp := deltas[index].timestamp
			for index < len(deltas) && deltas[index].timestamp == timestamp {
				cumulative.add(deltas[index].input, deltas[index].output)
				index++
			}

			key := append([]byte(nil), encoded.catalogKey...)
			key[0] = prefixRunData
			key = binary.BigEndian.AppendUint64(key, timestamp)
			records = append(records, runRecord{key: key, value: encodeCumulative(cumulative)})
			dataCount++
		}
	}

	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].key, records[j].key) < 0 })
	checksum := checksumRecords(records)

	return records, dataCount, uint64(len(encodedIdentities)), checksum, nil
}

func checksumRecords(records []runRecord) [32]byte {
	hash := sha256.New()
	for _, record := range records {
		writeCanonicalRecordHash(hash, record.key, record.value)
	}

	var checksum [32]byte
	copy(checksum[:], hash.Sum(nil))

	return checksum
}

// writeCanonicalRecordHash excludes the physical run ID while preserving the
// record kind and complete logical key/value bytes. Consequently Checksum is a
// stable content identity even when replicas choose different run IDs.
func writeCanonicalRecordHash(digest hash.Hash, key, value []byte) {
	canonicalKeyLength := len(key)
	if len(key) >= 9 && (key[0] == prefixRunData || key[0] == prefixRunCatalog) {
		canonicalKeyLength -= 8
	}

	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(canonicalKeyLength))
	_, _ = digest.Write(length[:])
	if len(key) >= 9 && (key[0] == prefixRunData || key[0] == prefixRunCatalog) {
		_, _ = digest.Write(key[:1])
		_, _ = digest.Write(key[9:])
	} else {
		_, _ = digest.Write(key)
	}
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareUint32(a, b uint32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}

	return 0
}

func validatePublication(current Manifest, publication Publication) error {
	coverage := publication.Coverage
	if coverage.AuditSequence < current.AuditWatermark {
		return &ErrSourceGap{Detail: fmt.Sprintf("audit watermark moved backward from %d to %d", current.AuditWatermark, coverage.AuditSequence)}
	}
	if coverage.LogSequence < current.LogWatermark {
		return &ErrSourceGap{Detail: fmt.Sprintf("log watermark moved backward from %d to %d", current.LogWatermark, coverage.LogSequence)}
	}
	if coverage.EffectiveFloor != 0 || coverage.InsertionFloor != 0 {
		return &ErrSourceGap{Detail: "non-zero history floors require a chain-bound or signed base import authority"}
	}
	if current.SourceComplete && !coverage.SourceComplete {
		return &ErrSourceGap{Detail: "source completeness cannot be revoked without resetting the store"}
	}
	if coverage.AuditSequence == current.AuditWatermark && len(publication.Effects) > 0 {
		return &ErrSourceGap{Detail: "effects cannot be appended without advancing audit coverage"}
	}
	if coverage.AuditSequence == current.AuditWatermark && coverage.LogSequence != current.LogWatermark {
		return &ErrSourceGap{Detail: "log coverage cannot advance without advancing audit coverage"}
	}
	if coverage.AuditSequence == current.AuditWatermark && !bytes.Equal(coverage.AuditHash, current.AuditHash) {
		return &ErrSourceGap{Detail: "audit hash changed without advancing audit coverage"}
	}

	for index, effect := range publication.Effects {
		if err := validateEffect(effect); err != nil {
			return fmt.Errorf("invalid monetary effect %d: %w", index, err)
		}
		if effect.AuditSequence <= current.AuditWatermark || effect.AuditSequence > coverage.AuditSequence {
			return &ErrSourceGap{Detail: fmt.Sprintf("effect audit sequence %d is outside (%d,%d]", effect.AuditSequence, current.AuditWatermark, coverage.AuditSequence)}
		}
		if effect.LogSequence > coverage.LogSequence {
			return &ErrSourceGap{Detail: fmt.Sprintf("effect log sequence %d exceeds watermark %d", effect.LogSequence, coverage.LogSequence)}
		}
	}

	return nil
}

// Publish atomically makes effects and their consecutive source coverage
// visible. Run bytes, run metadata, manifest, and the latest pointer share one
// Pebble WAL batch; a crash cannot expose a manifest without its referenced
// run. The peer projection is rebuildable, so durability is asynchronous:
// SyncWAL bounds the suffix that a process or power failure may replay.
func (s *publisher) Publish(publication Publication) (Manifest, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.ensureNotQuarantined(); err != nil {
		return Manifest{}, err
	}

	current, err := readManifest(s.db)
	if err != nil {
		return Manifest{}, err
	}
	if err := validatePublication(current, publication); err != nil {
		return Manifest{}, err
	}
	if publication.Coverage.AuditSequence == current.AuditWatermark &&
		publication.Coverage.LogSequence == current.LogWatermark &&
		publication.Coverage.SourceComplete == current.SourceComplete &&
		bytes.Equal(publication.Coverage.AuditHash, current.AuditHash) {
		return current, nil
	}

	next := current
	next.Version++
	next.AuditWatermark = publication.Coverage.AuditSequence
	next.LogWatermark = publication.Coverage.LogSequence
	next.SourceComplete = publication.Coverage.SourceComplete
	next.AuditHash = append([]byte(nil), publication.Coverage.AuditHash...)
	next.ReducerState = publication.ReducerState
	next.LogicalDigest, err = AdvanceLogicalDigest(current.LogicalDigest, current.AuditWatermark, publication.Coverage.AuditSequence, publication.Effects)
	if err != nil {
		return Manifest{}, err
	}

	var (
		records []runRecord
		newRun  *RunRef
	)
	if len(publication.Effects) > 0 {
		runID := next.NextRunID
		if runID == 0 {
			runID = 1
		}
		if runID == ^uint64(0) {
			return Manifest{}, errors.New("balance history run id space exhausted")
		}
		next.NextRunID = runID + 1

		var entryCount, identityCount uint64
		var checksum [32]byte
		records, entryCount, identityCount, checksum, err = buildRunRecords(runID, publication.Effects)
		if err != nil {
			return Manifest{}, err
		}

		firstAudit := publication.Effects[0].AuditSequence
		for _, effect := range publication.Effects[1:] {
			if effect.AuditSequence < firstAudit {
				firstAudit = effect.AuditSequence
			}
		}
		run := RunRef{
			ID:                 runID,
			Level:              0,
			FirstAuditSequence: firstAudit,
			LastAuditSequence:  publication.Coverage.AuditSequence,
			MaxLogSequence:     publication.Coverage.LogSequence,
			EntryCount:         entryCount,
			IdentityCount:      identityCount,
			Checksum:           checksum,
		}
		next.Runs = append(next.Runs, run)
		newRun = &run
	}

	next, err = s.commitPublicationLocked(next, records, newRun, "publication")
	if err != nil {
		return Manifest{}, err
	}
	s.signalChanged()

	return next, nil
}

// commitPublicationLocked is the only manifest-publication primitive. Callers
// must hold mutationMu and fully validate the logical transition first. Run
// bytes, optional run metadata, immutable manifest, and latest pointer share
// one Pebble batch, so no source cursor can become visible without its complete
// referenced data.
func (s *publisher) commitPublicationLocked(
	next Manifest,
	records []runRecord,
	newRun *RunRef,
	operation string,
) (Manifest, error) {
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
	for _, record := range records {
		if err := batch.Set(record.key, record.value, nil); err != nil {
			return Manifest{}, fmt.Errorf("staging balance history %s run record: %w", operation, err)
		}
	}
	if newRun != nil {
		encodedRun, err := json.Marshal(newRun)
		if err != nil {
			return Manifest{}, fmt.Errorf("marshaling balance history %s run metadata: %w", operation, err)
		}
		if err := batch.Set(runMetaKey(newRun.ID), encodedRun, nil); err != nil {
			return Manifest{}, fmt.Errorf("staging balance history %s run metadata: %w", operation, err)
		}
	}
	if err := batch.Set(manifestKey(next.Version), encodedManifest, nil); err != nil {
		return Manifest{}, fmt.Errorf("staging balance history %s manifest: %w", operation, err)
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], next.Version)
	if err := batch.Set(latestManifestKey(), version[:], nil); err != nil {
		return Manifest{}, fmt.Errorf("staging latest balance history %s manifest pointer: %w", operation, err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return Manifest{}, fmt.Errorf("committing balance history %s: %w", operation, err)
	}

	return next, nil
}

// AdvanceLogicalDigest folds one consecutive audit range into the canonical
// projection digest. It is independent of publication batching, physical run
// IDs, and compaction layout, and is shared by ingestion and source replay.
func AdvanceLogicalDigest(current [32]byte, after, through uint64, effects []balancehistory.Effect) ([32]byte, error) {
	if through < after {
		return [32]byte{}, &ErrSourceGap{Detail: fmt.Sprintf("logical digest range moved backward from %d to %d", after, through)}
	}
	if through == after {
		return current, nil
	}

	byAudit := make(map[uint64][][]byte)
	for _, effect := range effects {
		encoded, err := encodeEffectCanonical(effect)
		if err != nil {
			return [32]byte{}, err
		}
		byAudit[effect.AuditSequence] = append(byAudit[effect.AuditSequence], encoded)
	}

	var fixed [8]byte
	for sequence := after + 1; ; sequence++ {
		encodedEffects := byAudit[sequence]
		sort.Slice(encodedEffects, func(i, j int) bool { return bytes.Compare(encodedEffects[i], encodedEffects[j]) < 0 })

		hash := sha256.New()
		_, _ = hash.Write(current[:])
		binary.BigEndian.PutUint64(fixed[:], sequence)
		_, _ = hash.Write(fixed[:])
		binary.BigEndian.PutUint64(fixed[:], uint64(len(encodedEffects)))
		_, _ = hash.Write(fixed[:])
		for _, encoded := range encodedEffects {
			binary.BigEndian.PutUint64(fixed[:], uint64(len(encoded)))
			_, _ = hash.Write(fixed[:])
			_, _ = hash.Write(encoded)
		}
		copy(current[:], hash.Sum(nil))
		if sequence == through {
			break
		}
	}

	return current, nil
}

func encodeEffectCanonical(effect balancehistory.Effect) ([]byte, error) {
	encoded := make([]byte, 0, 128+len(effect.Account)+len(effect.AssetBase)+len(effect.Color))
	encoded = binary.BigEndian.AppendUint32(encoded, effect.LedgerID)
	encoded = binary.BigEndian.AppendUint64(encoded, effect.AuditSequence)
	encoded = binary.BigEndian.AppendUint32(encoded, effect.OrderIndex)
	encoded = binary.BigEndian.AppendUint64(encoded, effect.LogSequence)
	encoded = binary.BigEndian.AppendUint64(encoded, effect.EffectiveAt)
	encoded = binary.BigEndian.AppendUint64(encoded, effect.InsertedAt)
	var err error
	encoded, err = appendString(encoded, effect.Account)
	if err != nil {
		return nil, fmt.Errorf("encoding canonical effect account: %w", err)
	}
	encoded, err = appendString(encoded, effect.AssetBase)
	if err != nil {
		return nil, fmt.Errorf("encoding canonical effect asset: %w", err)
	}
	encoded = append(encoded, effect.AssetPrecision)
	encoded, err = appendString(encoded, effect.Color)
	if err != nil {
		return nil, fmt.Errorf("encoding canonical effect color: %w", err)
	}
	encoded = append(encoded, effect.Input[:]...)
	encoded = append(encoded, effect.Output[:]...)

	return encoded, nil
}
