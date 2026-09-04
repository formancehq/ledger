package check

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Ordering classes for the emitted findings. Events are sorted by
// (class, ledger, message) so two runs over the same store produce a
// byte-identical event stream.
const (
	reverseMapClassOrphan = iota
	reverseMapClassUnknownLedger
	reverseMapClassMalformed
)

// reverseMapFieldKey identifies one aggregation bucket on the orphan class: the
// reverse-map rows of a single (ledger, namespace, metadata key) triple. The
// namespace is part of the identity because the same metadata key can be
// indexed for accounts and not for transactions (or the reverse) — the two
// halves are independent indexes.
type reverseMapFieldKey struct {
	ledger    string
	namespace string
	metaKey   string
}

// reverseMapAggregate accumulates a row count plus one representative sample
// for a single bucket. Only the first sample is retained: dropping a metadata
// field on a large ledger can strand millions of rows, so the pass must stay
// O(distinct buckets) in memory and O(distinct buckets) in emitted events —
// never O(rows). Pebble iterates in key order, so "first sample" is
// deterministic for a given store.
// reverseMapVerdict caches one field's resolved verdict: whether its rows are
// orphans, and — when they are — which lifecycle path best labels the finding's
// diagnostics.
type reverseMapVerdict struct {
	orphan   bool
	missedBy string
}

type reverseMapAggregate struct {
	rows   uint64
	sample string
}

// reverseMapFinding is one event to emit, held until the whole scan is done so
// the emission order can be made deterministic.
type reverseMapFinding struct {
	class   int
	ledger  string
	message string
}

// observeReverseMapRow folds one scanned row into its aggregation bucket,
// creating the bucket on first sight.
func observeReverseMapRow[K comparable](buckets map[K]*reverseMapAggregate, key K, sample string) {
	agg, ok := buckets[key]
	if !ok {
		agg = &reverseMapAggregate{}
		buckets[key] = agg
	}

	agg.rows++

	if agg.rows == 1 {
		agg.sample = sample
	}
}

// reverseMapOrphanScope carries the peer snapshot under judgement plus every
// oracle term compareReverseMapOrphans compares it against.
type reverseMapOrphanScope struct {
	// reader is the primary-store snapshot the index registry is read from.
	reader dal.PebbleReader
	// peer is the peer read-index snapshot, pinned by Check() BEFORE the primary
	// one so the peer cursor can never appear ahead. nil when no readstore is
	// attached.
	peer *pebble.Snapshot
	// lastSequence is the last log sequence the replay verified.
	lastSequence uint64
	// Every oracle term below is frozen at lastSequence, which is why the pass
	// only reaches a verdict on an exactly aligned peer view.
	liveLedgers     map[string]struct{}
	replayedSchemas map[string]*commonpb.MetadataSchema
}

// compareReverseMapOrphans reports reverse-map (rmap, prefix 0x03) rows in the
// peer read-index store whose metadata field is no longer indexed.
//
// Reverse-map keys are field/version-first:
// [ledger][ns][metadataKey\x00][version][entity]. All rows for one field are
// therefore contiguous, and purgeReverseMapForKey removes them across every
// version with one field-bounded DeleteRange. The reverse map remains the peer
// projection this checker inspects: compareIndexes verifies the SubAttrIndex
// registry in the primary store, not the read-index rows that registry drives.
// Keeping this pass catches corruption and lifecycle-contract violations even
// though a correct field purge no longer has a row-by-row partial-success mode.
//
// This is a peer-store rebuild-health check, not an invariant-#8 main-store
// projection compare: the primary store is the oracle here, and the read index
// is the data being judged.
//
// A row is an orphan when no index is currently registered for its (ledger,
// target, metadata key). The registry decides alone. Both ends of the index
// lifecycle purge the reverse map — RemovedMetadataFieldType always ran
// purgeReverseMapForKey, and handleDroppedIndexLog now runs the same purge on
// DropIndex — so at an aligned peer view a live row with no registry entry is a
// lifecycle invariant violation. It may indicate a broken purge path or direct
// read-store corruption. Evaluated only on an exactly aligned peer view; the
// malformed-key class needs no oracle at all and always runs. See ALIGNMENT
// below.
//
// Schema declaration legitimises NOTHING. The old rule tolerated
// declared-but-unregistered rows as DropIndex residue (the pre-purge leak,
// EN-1621), and that tolerance was precisely the blind spot that would have
// hidden a regression in the drop purge — plus stale rows for a removed field
// that was later re-declared. The replayed schema survives only as the
// finding's diagnostic classification: a still-declared orphan is labelled
// against the DropIndex lifecycle, while an undeclared one is labelled against
// RemovedMetadataFieldType. It is not the liveness oracle and does not rule out
// corruption outside either lifecycle path.
//
// The schema used for that label MUST be the audit-derived replayed schema,
// never the stored LedgerInfo.MetadataSchema — an oracle input must never come
// from the data it judges. Same reasoning as sourcing liveLedgers from the
// replay.
//
// The registry read here is the STORED SubAttrIndex registry — a primary-store
// projection, itself verified against the audit replay by compareIndexes. An
// `indexed == true` entry can suppress an orphan verdict, so a stale or
// tampered registry row is a masking channel: orphaned rmap rows plus a
// lingering registry row would pass Check() together. compareIndexes closes it
// — every entry the replay never touched is reported as INDEX_MISMATCH in its
// own right. Deriving the liveness set from the audit replay directly, rather
// than through the stored registry, would remove that coupling; it is
// deliberately left as a separate change so its behavioral diff can be
// reviewed on its own.
//
// Version is deliberately ignored. Current and pending forward-encoding
// versions legitimately coexist while a per-replica schema rewrite runs, and
// versions outside that live pair are reclaimed at boot by purgeOrphanVersions.
// A row at ANY version whose field is still indexed is not an orphan; flagging
// on version would report every in-flight rewrite.
//
// ALIGNMENT. lastSequence is the last log sequence the checker verified, and
// every oracle term — the index registry read off `reader`, the replayed schemas,
// the live-ledger set, the removed-field set — is frozen at exactly that point.
// The peer read index is an independent store folded asynchronously by the index
// builder, so a verdict is only sound when the peer has folded exactly that log
// range: `indexedSequence == lastSequence`. The two other positions are skips,
// and they are not symmetric.
//
//   - BEHIND is the ordinary state on a live cluster. The registry is written at
//     Raft apply while the rmap folds later, so between apply and fold a
//     legitimately-removed field has no registry entry but still has live rmap
//     rows. Nothing can be concluded, so no verdict is reached.
//
//   - AHEAD cannot happen by race. The builder folds FROM the primary log stream
//     and writes its cursor for logs it has already read out of the primary
//     store, so progress(t) <= maxLogSeq(t) at every instant; Check() pins the
//     peer snapshot BEFORE the primary one precisely so that ordering is
//     inherited by the two pinned values. See the comment on the pins in Check().
//
//     Nor is it reachable at runtime by any other route. RestoreCheckpoint —
//     the only thing that replaces the primary store wholesale — has a single
//     production caller (dal.incomingRestoreFactory.Run, reached through
//     state.Synchronizer.SynchronizeWithLeader), and it installs a checkpoint
//     fetched FROM the leader: a follower only syncs because it is behind, a
//     leader compacts only up to a snapshot it can serve, and an applied index
//     can never exceed the leader's log. Every path moves the node forward.
//
//     So an ahead cursor means the deployment is already broken — the classic
//     shape being an offline restore of an older backup into a data directory
//     whose read-indexes/ survived, leaving rows folded from logs the primary
//     no longer has. That state never self-heals (the index builder, unlike
//     usagebuilder, has no rollback reset), and it is exactly the corruption
//     this pass exists to surface, so it is REPORTED rather than skipped:
//     silently limiting the check to key decoding would leave the one
//     read-index limb the checker covers unverified, with an INFO log as the
//     only trace (invariant #7).
//
// This is why the pass needs no cross-store atomicity: an ordering that can only
// leave the peer behind is sufficient, because behind is already a skip.
func (c *Checker) compareReverseMapOrphans(
	scope reverseMapOrphanScope,
	callback func(*servicepb.CheckStoreEvent),
) {
	if c.readStore == nil || scope.peer == nil {
		c.logger.Infof("Reverse-map orphan check skipped: no peer read-index store is attached to this checker")

		return
	}

	indexedSequence, err := c.readStore.LastIndexedSequenceFrom(scope.peer)
	if err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("reading the read-index progress cursor: %v", err),
			0, "", "", "",
		))

		return
	}

	// See ALIGNMENT above. Every oracle term is frozen at lastSequence, so a
	// verdict is only sound when the peer folded exactly that range. Malformed
	// keys need no oracle and are reported whatever this says.
	aligned := indexedSequence == scope.lastSequence

	switch {
	case indexedSequence < scope.lastSequence:
		c.logger.WithFields(map[string]any{
			"indexedSequence": indexedSequence,
			"lastSequence":    scope.lastSequence,
		}).Infof("Reverse-map orphan check limited to key decoding: the read index has not folded the whole verified log range")
	case indexedSequence > scope.lastSequence:
		// No runtime path produces this — see ALIGNMENT. Getting here means the
		// primary store was replaced beneath a surviving read index, so the
		// peer holds rows derived from logs the primary no longer has.
		assert.Unreachable("check: read index ahead of the verified log range", map[string]any{
			"indexedSequence": indexedSequence,
			"lastSequence":    scope.lastSequence,
		})

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf(
				"read index is ahead of the verified log range (indexed %d > verified %d): the primary store was replaced beneath a surviving read index, so the read index holds rows for logs the primary no longer has",
				indexedSequence, scope.lastSequence,
			),
			0, "", "", "",
		))
	}

	// The registry is only consulted when a verdict can be reached at all, so an
	// unaligned run does not pay for building the oracle.
	indexedFields := map[domain.IndexKey]struct{}{}
	if aligned {
		var ok bool

		indexedFields, ok = c.collectIndexedFields(scope.reader, callback)
		if !ok {
			return
		}
	}

	lower := []byte{readstore.PrefixReverseMap}

	iter, err := scope.peer.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: readstore.IncrementBytes(lower),
	})
	if err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("opening the reverse-map iterator: %v", err),
			0, "", "", "",
		))

		return
	}

	defer func() { _ = iter.Close() }()

	orphaned := make(map[reverseMapFieldKey]*reverseMapAggregate)
	unknownLedgers := make(map[string]*reverseMapAggregate)
	malformed := make(map[string]*reverseMapAggregate)

	// Per-field verdict cache. The oracle lookup allocates an IndexID and a
	// canonical string, so resolving it once per distinct triple rather than
	// once per row keeps the scan's *work*, not only its memory, proportional
	// to the number of distinct fields. Bounded by the same set of triples the
	// aggregates are.
	verdicts := make(map[reverseMapFieldKey]reverseMapVerdict)
	orphanMissedBy := make(map[reverseMapFieldKey]string)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		parsed, err := readstore.ParseReverseMapKey(key)
		if err != nil {
			// The sentinel names which shape check failed, so the operator
			// learns *which* corruption this is — carry it in the sample
			// alongside a bounded prefix of the raw key.
			observeReverseMapRow(malformed, bestEffortReverseMapLedger(key),
				fmt.Sprintf("key %s: %v", renderReverseMapKeyPrefix(key), err))

			continue
		}

		if _, live := scope.liveLedgers[parsed.Ledger]; !live {
			// The live set is the single oracle for this class, and it is the
			// audit-derived one: DeleteLedger removes the name from it (and
			// range-deletes the whole [0x03][ledger] span at apply), a later
			// CreateLedger of the same name puts it back. Deriving the verdict from
			// liveness alone — rather than from a separate append-only "was
			// deleted" set consulted first — is what keeps a recreated ledger's
			// rows legitimate without depending on an external guarantee that the
			// lifecycle is unreachable.
			//
			// It is absence-based, so it needs the aligned view: a ledger created
			// after lastSequence is missing from liveLedgers for a perfectly
			// healthy reason.
			if aligned {
				observeReverseMapRow(unknownLedgers, parsed.Ledger, renderReverseMapEntity(parsed))
			}

			continue
		}

		target, known := targetTypeForReverseMapNamespace(parsed.Namespace)
		if !known {
			// Impossible by design: ParseReverseMapKey rejects every namespace
			// other than NamespaceAccount / NamespaceTransaction, so a parsed
			// key cannot carry a third one. Report it loudly (invariant #7)
			// instead of skipping, but keep it aggregated so a hypothetical
			// mass occurrence cannot flood the run.
			observeReverseMapRow(malformed, parsed.Ledger,
				fmt.Sprintf("invariant: key %s decoded with unsupported namespace %q",
					renderReverseMapKeyPrefix(key), parsed.Namespace))

			continue
		}

		field := reverseMapFieldKey{
			ledger:    parsed.Ledger,
			namespace: parsed.Namespace,
			metaKey:   parsed.MetadataKey,
		}

		verdict, decided := verdicts[field]
		if !decided {
			if aligned {
				// The registry decides, alone: every reverse-map row must
				// belong to a currently registered index. Both lifecycle ends
				// purge — RemovedMetadataFieldType and DropIndex both use the
				// field-bounded purge — so at an aligned view a row with no
				// registry entry violates the lifecycle invariant. Schema
				// declaration legitimises NOTHING: tolerating
				// declared-but-unregistered rows is exactly the blind spot that
				// hid dropped-index leaks and stale rows later made plausible by
				// re-declaring the field.
				_, indexed := indexedFields[indexes.KeyFor(parsed.Ledger, indexes.MetadataID(target, parsed.MetadataKey))]
				verdict.orphan = !indexed

				if verdict.orphan {
					// The schema term survives only as a diagnostic lifecycle
					// classification. A declared field is labelled against
					// DropIndex; an undeclared (or removal-recorded) one against
					// RemovedMetadataFieldType. The registry remains the sole
					// liveness oracle.
					_, declared := commonpb.SchemaFieldForTarget(scope.replayedSchemas[parsed.Ledger], target, parsed.MetadataKey)
					if declared != nil {
						verdict.missedBy = "DropIndex purge"
					} else {
						verdict.missedBy = "RemovedMetadataFieldType purge"
					}
				}
			}

			verdicts[field] = verdict
		}

		if !verdict.orphan {
			continue
		}

		observeReverseMapRow(orphaned, field, renderReverseMapEntity(parsed))
		orphanMissedBy[field] = verdict.missedBy
	}

	if err := iter.Error(); err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("scanning the reverse map: %v", err),
			0, "", "", "",
		))
	}

	emitReverseMapFindings(orphaned, orphanMissedBy, unknownLedgers, malformed, callback)
}

// collectIndexedFields builds the oracle: every index key present in the
// primary store's SubAttrIndex registry. Presence is all that matters — a
// registered index that is still building legitimately has rmap rows.
//
// The second return value is false when the oracle could not be built
// completely; the caller must then abort rather than report orphans, since
// every unseen registry entry would turn its live rmap rows into false
// positives.
func (c *Checker) collectIndexedFields(
	reader dal.PebbleReader,
	callback func(*servicepb.CheckStoreEvent),
) (map[domain.IndexKey]struct{}, bool) {
	iter, err := c.attrs.Index.NewStreamingIter(reader, nil)
	if err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("opening index registry iterator: %v", err),
			0, "", "", "",
		))

		return nil, false
	}

	defer func() { _ = iter.Close() }()

	indexed := make(map[domain.IndexKey]struct{})

	for iter.Next() {
		entry := iter.Entry()

		if entry.Value == nil || entry.Value.GetId() == nil {
			continue
		}

		var key domain.IndexKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			// An unparsable registry key is itself a reported problem. The
			// resulting hole in the oracle may add an orphan report for the
			// same field, which is a second symptom of the same corruption
			// rather than an independent false positive.
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
				fmt.Sprintf("stored index has unparsable canonical key %x: %v", entry.CanonicalKey, err),
				0, entry.Value.GetLedger(), "", "",
			))

			continue
		}

		// Bucket-scoped entries can never match a ledger-scoped rmap row: an
		// rmap key always carries a non-empty ledger name (ParseReverseMapKey
		// rejects an empty one).
		if key.LedgerName == "" {
			continue
		}

		indexed[key] = struct{}{}
	}

	if err := iter.Err(); err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("scanning index registry: %v", err),
			0, "", "", "",
		))

		return nil, false
	}

	return indexed, true
}

// emitReverseMapFindings renders every aggregation bucket into one event and
// emits them in a deterministic order.
func emitReverseMapFindings(
	orphaned map[reverseMapFieldKey]*reverseMapAggregate,
	orphanMissedBy map[reverseMapFieldKey]string,
	unknownLedgers map[string]*reverseMapAggregate,
	malformed map[string]*reverseMapAggregate,
	callback func(*servicepb.CheckStoreEvent),
) {
	findings := make([]reverseMapFinding, 0, len(orphaned)+len(unknownLedgers)+len(malformed))

	for key, agg := range orphaned {
		findings = append(findings, reverseMapFinding{
			class:  reverseMapClassOrphan,
			ledger: key.ledger,
			message: fmt.Sprintf(
				"reverse-map rows survive for metadata field %q (namespace %q) on ledger %q with no registered index — classified against the %s lifecycle: rows=%d, sample %s",
				key.metaKey, key.namespace, key.ledger, orphanMissedBy[key], agg.rows, agg.sample),
		})
	}

	for ledger, agg := range unknownLedgers {
		findings = append(findings, reverseMapFinding{
			class:  reverseMapClassUnknownLedger,
			ledger: ledger,
			message: fmt.Sprintf(
				"reverse-map rows survive for ledger %q, which is absent from the live ledger set: rows=%d, sample %s",
				ledger, agg.rows, agg.sample),
		})
	}

	for ledger, agg := range malformed {
		findings = append(findings, reverseMapFinding{
			class:  reverseMapClassMalformed,
			ledger: ledger,
			message: fmt.Sprintf(
				"reverse-map keys do not decode for ledger %q: rows=%d, sample %s",
				ledger, agg.rows, agg.sample),
		})
	}

	slices.SortFunc(findings, func(a, b reverseMapFinding) int {
		return cmp.Or(
			cmp.Compare(a.class, b.class),
			cmp.Compare(a.ledger, b.ledger),
			cmp.Compare(a.message, b.message),
		)
	})

	for _, finding := range findings {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			finding.message,
			0, finding.ledger, "", "",
		))
	}
}

// targetTypeForReverseMapNamespace maps a reverse-map namespace to the index
// target it addresses. The returned target is meaningless when ok is false.
func targetTypeForReverseMapNamespace(ns string) (commonpb.TargetType, bool) {
	switch ns {
	case readstore.NamespaceAccount:
		return commonpb.TargetType_TARGET_TYPE_ACCOUNT, true
	case readstore.NamespaceTransaction:
		return commonpb.TargetType_TARGET_TYPE_TRANSACTION, true
	}

	return commonpb.TargetType_TARGET_TYPE_ACCOUNT, false
}

// renderReverseMapEntity renders a decoded row's entity for an operator: a
// transaction as its decimal id, an account as its address.
//
// The transaction branch is safe to index unconditionally because
// ParseReverseMapKey guarantees an exactly-8-byte big-endian EntityID for
// NamespaceTransaction — it rejects anything shorter as truncated.
func renderReverseMapEntity(parsed readstore.ParsedReverseMapKey) string {
	if parsed.Namespace == readstore.NamespaceTransaction {
		return "transaction " + strconv.FormatUint(binary.BigEndian.Uint64(parsed.EntityID), 10)
	}

	return "account " + string(parsed.EntityID)
}

// reverseMapKeyHexPrefixBytes bounds how much of a malformed key is rendered into
// a finding. Enough to identify the corrupted shape (prefix byte + the
// fixed-width ledger-name block + the start of the namespace and entity) without
// letting key length drive the message size.
const reverseMapKeyHexPrefixBytes = 64

// renderReverseMapKeyPrefix hex-renders at most reverseMapKeyHexPrefixBytes of a
// key, appending the full byte length when it truncates.
//
// The malformed bucket keeps one sample per ledger, so an unbounded dump is not a
// memory-blowup risk — but the sample is also copied verbatim into the emitted
// event, and a key long enough to matter requires direct Pebble write access, at
// which point message size is the least of the problems. This is about keeping the
// operator-facing message readable and its size independent of the input.
func renderReverseMapKeyPrefix(key []byte) string {
	if len(key) <= reverseMapKeyHexPrefixBytes {
		return hex.EncodeToString(key)
	}

	return fmt.Sprintf("%x… (%d bytes total)", key[:reverseMapKeyHexPrefixBytes], len(key))
}

// bestEffortReverseMapLedger extracts the ledger name from a reverse-map key
// whose decode failed, so the corruption report can still be attributed to a
// ledger. It reads ONLY the fixed-width zero-padded name block — the offsets
// every ledger-scoped read-store key shares — and returns "" when that block is
// absent or itself corrupt.
//
// ParseReverseMapKey remains the chokepoint for decoding a well-formed key;
// this exists solely because it (correctly) returns a zero value on error and
// so cannot name the ledger of a key it rejected.
func bestEffortReverseMapLedger(key []byte) string {
	const nameStart = 1

	if len(key) < nameStart+dal.LedgerNameFixedSize {
		return ""
	}

	name := bytes.TrimRight(key[nameStart:nameStart+dal.LedgerNameFixedSize], "\x00")
	if bytes.IndexByte(name, 0x00) >= 0 {
		return ""
	}

	return string(name)
}
