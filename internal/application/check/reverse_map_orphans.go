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
// A row is an orphan only if its field is BOTH absent from the index registry
// AND absent from the ledger's replayed metadata schema. The schema term is
// what makes the pass detect EN-1458's bug and nothing else:
// RemovedMetadataFieldType is the single log that both removes the schema field
// type and runs purgeReverseMapForKey, so "field absent from the replayed
// schema + live rmap rows" is exactly "the point-delete scan missed rows".
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
// lastSequence is the last log sequence the checker verified. The pass is a
// no-op unless the read index has folded at least that far: the registry is
// written at Raft apply while the rmap is folded asynchronously by the index
// builder, so between apply and fold a legitimately-removed field has no
// registry entry but still has live rmap rows. Without that gate the pass
// false-positives on every healthy cluster mid-fold.
//
// Rows belonging to a ledger in pendingCleanupLedgers are skipped: like the
// other passes, the deferred-purge window is tolerated rather than flagged.
func (c *Checker) compareReverseMapOrphans(
	reader dal.PebbleReader,
	lastSequence uint64,
	liveLedgers map[string]struct{},
	pendingCleanupLedgers map[string]struct{},
	replayedSchemas map[string]*commonpb.MetadataSchema,
	callback func(*servicepb.CheckStoreEvent),
) {
	if c.readStore == nil {
		c.logger.Infof("Reverse-map orphan check skipped: no peer read-index store is attached to this checker")

		return
	}

	// One snapshot for both the progress cursor and the scan, so the lag gate
	// is evaluated against exactly the rows that are about to be judged.
	snap := c.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	indexedSequence, err := c.readStore.LastIndexedSequenceFrom(snap)
	if err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
			fmt.Sprintf("reading the read-index progress cursor: %v", err),
			0, "", "", "",
		))

		return
	}

	if indexedSequence < lastSequence {
		c.logger.WithFields(map[string]any{
			"indexedSequence": indexedSequence,
			"lastSequence":    lastSequence,
		}).Infof("Reverse-map orphan check skipped: the read index has not folded the whole verified log range")

		return
	}

	indexedFields, ok := c.collectIndexedFields(reader, callback)
	if !ok {
		return
	}

	lower := []byte{readstore.PrefixReverseMap}

	iter, err := snap.NewIter(&pebble.IterOptions{
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

		if _, awaiting := pendingCleanupLedgers[parsed.Ledger]; awaiting {
			continue
		}

		if _, live := liveLedgers[parsed.Ledger]; !live {
			// DeleteLedger range-deletes the whole [0x03][ledger] span
			// unconditionally, and the lag gate above guarantees that delete
			// has been folded — so a survivor is a real leak.
			observeReverseMapRow(unknownLedgers, parsed.Ledger, renderReverseMapEntity(parsed))

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
			_, indexed := indexedFields[indexes.KeyFor(parsed.Ledger, indexes.MetadataID(target, parsed.MetadataKey))]
			_, declared := commonpb.SchemaFieldForTarget(replayedSchemas[parsed.Ledger], target, parsed.MetadataKey)
			orphan = !indexed && declared == nil
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
				"reverse-map rows survive for metadata field %q (namespace %q) on ledger %q, which is neither in the index registry nor declared in the audit-derived metadata schema: rows=%d, sample %s",
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
