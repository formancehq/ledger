package check

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"

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

// removedSchemaFieldKey identifies a metadata field the replay observed a
// RemovedMetadataFieldType log for. Keyed by target type rather than by
// reverse-map namespace because that is what the log carries; the pass maps its
// namespace to a target before looking up.
type removedSchemaFieldKey struct {
	ledger  string
	target  commonpb.TargetType
	metaKey string
}

// reverseMapAggregate accumulates a row count plus one representative sample
// for a single bucket. Only the first sample is retained: dropping a metadata
// field on a large ledger can strand millions of rows, so the pass must stay
// O(distinct buckets) in memory and O(distinct buckets) in emitted events —
// never O(rows). Pebble iterates in key order, so "first sample" is
// deterministic for a given store.
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
	// peer is the peer read-index snapshot, pinned by Check() alongside the
	// primary one. nil when no readstore is attached.
	peer *pebble.Snapshot
	// lastSequence is the last log sequence the replay verified.
	lastSequence uint64
	// liveLedgers, pendingCleanupLedgers, replayedSchemas are absence-based
	// oracle terms: all three are frozen at lastSequence.
	liveLedgers           map[string]struct{}
	pendingCleanupLedgers map[string]struct{}
	replayedSchemas       map[string]*commonpb.MetadataSchema
	// removedFields and deletedLedgers are the positive-evidence terms: logs
	// the replay actually observed, so they are immune to cursor skew.
	removedFields  map[removedSchemaFieldKey]struct{}
	deletedLedgers map[string]struct{}
}

// compareReverseMapOrphans reports reverse-map (rmap, prefix 0x03) rows in the
// peer read-index store whose metadata field is no longer indexed.
//
// The reverse map is the one read-index limb that cannot be range-deleted per
// field: its metadata key sits *after* a fixed-width 4-byte version block
// ([ledger][ns][entity][version][metaKey]), so there is no prefix covering
// "every row of this field". Removing a field therefore scans the namespace and
// point-deletes row by row (purgeReverseMapForKey), and a row that scan misses
// is a permanent orphan — the forward index (0x01) and entity-exists index
// (0x02) limbs, both range-deletable by field, cannot leak this way. No pass
// has ever looked at the rmap: compareIndexes verifies the SubAttrIndex
// registry in the primary store, not the peer index it drives.
//
// This is a peer-store rebuild-health check, not an invariant-#8 main-store
// projection compare: the primary store is the oracle here, and the read index
// is the data being judged.
//
// A row is an orphan when its field is no longer indexed and no longer declared.
// That verdict is reached one of two ways, and the distinction is load-bearing:
//
//   - POSITIVE EVIDENCE (skew-immune). The replay observed a
//     RemovedMetadataFieldType for the field and nothing re-declared it. This is
//     exactly EN-1458's bug: RemovedMetadataFieldType is the single log that both
//     removes the schema field type and runs purgeReverseMapForKey, so "the audit
//     says this field was removed + rows are still live" means "the point-delete
//     scan missed rows". Because the verdict rests on a log the replay *saw*, it
//     holds no matter where the peer cursor sits relative to lastSequence.
//
//   - ABSENCE (needs an exactly aligned view). The field is in neither the index
//     registry nor the replayed schema. This catches removals whose log has been
//     archived away, and rows for fields that were never audited at all — but it
//     infers "removed" from "not present in a view frozen at lastSequence", so a
//     field created *after* that point looks identical to a removed one. It is
//     therefore evaluated only when the peer cursor is exactly aligned with the
//     verified sequence. See the alignment discussion below.
//
// The same split applies to the unknown-ledger class: a replayed DeleteLedger is
// positive evidence, while mere absence from liveLedgers requires alignment.
// The malformed-key class needs no oracle at all and always runs.
//
// Residue from a plain DropIndex is deliberately NOT flagged. DropIndex removes
// the registry entry but leaves the schema field declared, and
// indexbuilder.handleDroppedIndexLog reclaims nothing — so the rows survive
// forever on a healthy cluster. That leak is real but it is a different, broader
// bug: it strands all three read-index limbs, not just the one that cannot be
// range-deleted. It is tracked as EN-1621
// (https://formance-team.atlassian.net/browse/EN-1621). Flagging it here would
// make Check() permanently fail on any cluster that has ever dropped a metadata
// index — including the restore and bootstrap validation gates, which have no
// warning channel — and a check that is permanently red on a legitimate
// operator action trains operators to ignore red.
//
// KNOWN COVERAGE LIMIT, revisit when EN-1621 lands: once DropIndex purges rows,
// a regression in that new purge path would strand rows while the schema field
// is still declared, and this oracle would NOT catch it. The conjunction does
// not solve that case and is not intended to — it trades that coverage for not
// being permanently red today. When EN-1621 makes DropIndex purge, the schema
// term stops being a safe proxy and the oracle must be reconsidered.
//
// The schema MUST be the audit-derived replayed schema, never the stored
// LedgerInfo.MetadataSchema. Reading the stored row would make the pass
// self-referential: a tampered or injected schema row could legitimise its own
// orphaned rmap rows. Same reasoning as sourcing liveLedgers from the replay
// rather than from stored LedgerInfo rows — an oracle must never come from the
// data it judges.
//
// The registry term is, today, redundant: validateIndexTarget requires
// SetMetadataFieldType before a metadata index can be created, so
// schema-declared is a superset of indexed. It is written as an explicit
// conjunction anyway because it degrades safely — toward FEWER false positives —
// if that guarantee ever breaks, whereas a bare schema check would start
// flagging rows of genuinely-indexed fields.
//
// Version is deliberately ignored. Current and pending forward-encoding
// versions legitimately coexist while a per-replica schema rewrite runs, and
// versions outside that live pair are reclaimed at boot by purgeOrphanVersions.
// A row at ANY version whose field is still indexed is not an orphan; flagging
// on version would report every in-flight rewrite.
//
// ALIGNMENT. lastSequence is the last log sequence the checker verified, and
// every oracle term — the index registry read off `reader`, the replayed schemas,
// the live-ledger set — is frozen at exactly that point. The peer read index is
// an independent store folded asynchronously by the index builder, so its cursor
// can sit on either side of lastSequence:
//
//   - BEHIND. The registry is written at Raft apply while the rmap folds later,
//     so between apply and fold a legitimately-removed field has no registry
//     entry but still has live rmap rows. Both verdict paths are suppressed
//     (positive evidence too: the peer has not folded the removal yet).
//
//   - AHEAD. Check() runs on a live node — CheckStore passes the live readStore
//     and there is no quiescing — so by the time the scan runs, the builder may
//     have folded rows for a ledger or field created after lastSequence. Judging
//     those against oracles pinned earlier reports a healthy cluster as corrupt.
//     Positive-evidence verdicts are unaffected (a field created after the pin
//     was never removed); absence-based verdicts are suppressed.
//
// scope.peer is pinned by Check() next to the primary snapshot so the two views
// are as close together as two separate Pebble stores allow, which keeps the
// aligned case reachable. It is not atomic, which is precisely why correctness
// rests on the evidence split above rather than on the cursors matching.
//
// Rows belonging to a ledger in pendingCleanupLedgers are skipped: like the
// other passes, the deferred-purge window is tolerated rather than flagged.
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

	// See ALIGNMENT above. `folded` admits the positive-evidence verdicts,
	// `aligned` additionally admits the absence-based ones. Malformed keys need
	// no oracle and are reported whatever these say.
	folded := indexedSequence >= scope.lastSequence
	aligned := indexedSequence == scope.lastSequence

	if !folded {
		c.logger.WithFields(map[string]any{
			"indexedSequence": indexedSequence,
			"lastSequence":    scope.lastSequence,
		}).Infof("Reverse-map orphan check limited to key decoding: the read index has not folded the whole verified log range")
	} else if !aligned {
		c.logger.WithFields(map[string]any{
			"indexedSequence": indexedSequence,
			"lastSequence":    scope.lastSequence,
		}).Infof("Reverse-map orphan check limited to audit-observed removals: the read index is ahead of the verified log range")
	}

	// The registry is only consulted by the absence-based path.
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
	verdicts := make(map[reverseMapFieldKey]bool)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		parsed, err := readstore.ParseReverseMapKey(key)
		if err != nil {
			// The sentinel names which shape check failed, so the operator
			// learns *which* corruption this is — carry it in the sample
			// alongside the raw key.
			observeReverseMapRow(malformed, bestEffortReverseMapLedger(key), fmt.Sprintf("key %x: %v", key, err))

			continue
		}

		if _, awaiting := scope.pendingCleanupLedgers[parsed.Ledger]; awaiting {
			continue
		}

		if _, deleted := scope.deletedLedgers[parsed.Ledger]; deleted {
			// Positive evidence: DeleteLedger range-deletes the whole
			// [0x03][ledger] span unconditionally, the replay saw that log, and
			// the peer has folded at least that far — so a survivor is a real
			// leak whatever the cursor's exact position.
			if folded {
				observeReverseMapRow(unknownLedgers, parsed.Ledger, renderReverseMapEntity(parsed))
			}

			continue
		}

		if _, live := scope.liveLedgers[parsed.Ledger]; !live {
			// Absence-based: a ledger created after lastSequence is missing from
			// liveLedgers for a perfectly healthy reason, and is indistinguishable
			// from a leaked one on an unaligned view.
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
				fmt.Sprintf("invariant: key %x decoded with unsupported namespace %q", key, parsed.Namespace))

			continue
		}

		field := reverseMapFieldKey{
			ledger:    parsed.Ledger,
			namespace: parsed.Namespace,
			metaKey:   parsed.MetadataKey,
		}

		orphan, decided := verdicts[field]
		if !decided {
			_, declared := commonpb.SchemaFieldForTarget(scope.replayedSchemas[parsed.Ledger], target, parsed.MetadataKey)
			_, removed := scope.removedFields[removedSchemaFieldKey{
				ledger:  parsed.Ledger,
				target:  target,
				metaKey: parsed.MetadataKey,
			}]

			switch {
			case declared != nil:
				// Still declared, so the rows are legitimate — including when an
				// earlier removal is on record and a later SetMetadataFieldType
				// re-declared the field. Checking this first is what makes the
				// removal set safe to keep append-only.
				orphan = false
			case removed && folded:
				// Positive evidence, see ALIGNMENT. Also the only path that
				// survives an ahead cursor.
				orphan = true
			case aligned:
				// Absence-based fallback: covers a removal whose log was archived
				// away, and rows for a field the audit never declared at all.
				_, indexed := indexedFields[indexes.KeyFor(parsed.Ledger, indexes.MetadataID(target, parsed.MetadataKey))]
				orphan = !indexed
			default:
				orphan = false
			}

			verdicts[field] = orphan
		}

		if !orphan {
			continue
		}

		observeReverseMapRow(orphaned, field, renderReverseMapEntity(parsed))
	}

	if err := iter.Error(); err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("scanning the reverse map: %v", err),
			0, "", "", "",
		))
	}

	emitReverseMapFindings(orphaned, unknownLedgers, malformed, callback)
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
				"reverse-map rows survive for metadata field %q (namespace %q) on ledger %q, which the audit-derived metadata schema no longer declares: rows=%d, sample %s",
				key.metaKey, key.namespace, key.ledger, agg.rows, agg.sample),
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
