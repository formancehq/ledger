package readstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	reverseMapBenchmarkLedger   = "benchmark"
	reverseMapBenchmarkEntities = 10_000
	reverseMapBenchmarkFields   = 12
	reverseMapBenchmarkVersions = 2
	reverseMapBenchmarkVersion  = uint32(7)
)

var reverseMapBenchmarkValue = []byte("Sbenchmark-value\x00")

type reverseMapBenchmarkLayout struct {
	name       string
	fieldFirst bool
}

var reverseMapBenchmarkLayouts = []reverseMapBenchmarkLayout{
	{name: "entity_first_baseline"},
	{name: "field_first_production", fieldFirst: true},
}

type reverseMapBenchmarkTarget struct {
	name      string
	namespace string
	entity    func(int) []byte
}

var reverseMapBenchmarkTargets = []reverseMapBenchmarkTarget{
	{
		name:      "account",
		namespace: NamespaceAccount,
		entity:    reverseMapBenchmarkAccountEntity,
	},
	{
		name:      "transaction",
		namespace: NamespaceTransaction,
		entity: func(id int) []byte {
			entity := make([]byte, 8)
			binary.BigEndian.PutUint64(entity, uint64(id+1))

			return entity
		},
	},
}

// BenchmarkReverseMapKeying compares the former entity-first reverse-map
// layout with the production field/version-first layout against a populated
// Pebble database. The fixture deliberately contains several fields and two
// versions so the baseline rewrite and purge paths pay their namespace scan
// amplification.
//
// PurgeFieldPlan measures construction of the atomic write batch, not its
// commit or later compaction: committing would destroy the shared fixture and
// rebuilding it inside the timed loop would dominate the result. It therefore
// isolates the decision this keying change affects: namespace scan + point
// tombstones versus one field-bounded range tombstone.
//
// This fixture is package-local. The indexbuilder fold benchmark models a
// different workload and may use different entity-ID widths; only layout
// comparisons within this suite are valid, not ratios across the two suites.
func BenchmarkReverseMapKeying(b *testing.B) {
	fields := reverseMapBenchmarkFieldNames()
	versions := []uint32{reverseMapBenchmarkVersion, reverseMapBenchmarkVersion + 1}
	targetField := fields[len(fields)/2]

	for _, target := range reverseMapBenchmarkTargets {
		b.Run(target.name, func(b *testing.B) {
			for _, layout := range reverseMapBenchmarkLayouts {
				b.Run(layout.name, func(b *testing.B) {
					db := openReverseMapBenchmarkDB(b, layout, target, fields, versions)
					lookupKeys := reverseMapBenchmarkLookupKeys(layout, target, fields)

					b.Run("PointLookupOneField", func(b *testing.B) {
						benchmarkReverseMapPointLookups(b, db, lookupKeys, 1)
					})

					b.Run("PointLookupAllFields", func(b *testing.B) {
						benchmarkReverseMapPointLookups(b, db, lookupKeys, len(fields))
					})

					b.Run("RewriteScan", func(b *testing.B) {
						benchmarkReverseMapRewriteScan(b, db, layout, target, targetField)
					})

					b.Run("PurgeFieldPlan", func(b *testing.B) {
						benchmarkReverseMapPurgeFieldPlan(b, db, layout, target, targetField)
					})
				})
			}
		})
	}
}

// BenchmarkReverseMapKeyingMultiFieldInsert exercises the possible downside
// of field-first keying: metadata for one entity no longer occupies one
// contiguous key range. Every iteration commits one entity with all benchmark
// fields. This isolates the reverse-map portion of an indexbuilder batch and
// does not charge an fsync (the read store has its WAL disabled in production
// too).
func BenchmarkReverseMapKeyingMultiFieldInsert(b *testing.B) {
	fields := reverseMapBenchmarkFieldNames()

	for _, target := range reverseMapBenchmarkTargets {
		b.Run(target.name, func(b *testing.B) {
			for _, layout := range reverseMapBenchmarkLayouts {
				b.Run(layout.name, func(b *testing.B) {
					db := openEmptyReverseMapBenchmarkDB(b)
					kb := dal.NewKeyBuilder()
					entityID := 0

					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						entity := target.entity(entityID)
						entityID++
						batch := db.NewBatch()

						for _, field := range fields {
							key := reverseMapBenchmarkKey(kb, layout, target, entity, field, reverseMapBenchmarkVersion)
							if err := batch.Set(key, reverseMapBenchmarkValue, nil); err != nil {
								b.Fatal(err)
							}
						}

						if err := batch.Commit(pebble.NoSync); err != nil {
							b.Fatal(err)
						}
						if err := batch.Close(); err != nil {
							b.Fatal(err)
						}
					}

					b.ReportMetric(float64(len(fields)), "rows/op")
				})
			}
		})
	}
}

func benchmarkReverseMapPointLookups(b *testing.B, db *pebble.DB, keys [][]byte, fieldsPerEntity int) {
	b.Helper()

	iteration := 0
	fieldCount := reverseMapBenchmarkFields
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// A full-period stride avoids benchmarking one permanently-hot entity.
		entity := (iteration * 7_919) % reverseMapBenchmarkEntities
		iteration++
		start := entity * fieldCount

		for field := range fieldsPerEntity {
			value, closer, err := db.Get(keys[start+field])
			if err != nil {
				b.Fatal(err)
			}
			if len(value) == 0 {
				b.Fatal("empty reverse-map value")
			}
			if err := closer.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(fieldsPerEntity), "lookups/op")
}

func benchmarkReverseMapRewriteScan(
	b *testing.B,
	db *pebble.DB,
	layout reverseMapBenchmarkLayout,
	target reverseMapBenchmarkTarget,
	field string,
) {
	b.Helper()

	kb := dal.NewKeyBuilder()
	lower := reverseMapBenchmarkNamespacePrefix(kb, target.namespace)
	if layout.fieldFirst {
		lower = reverseMapBenchmarkVersionPrefix(kb, target, field, reverseMapBenchmarkVersion)
	}
	upper := IncrementBytes(lower)
	var totalScanned, totalMatched int64
	operations := 0

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			b.Fatal(err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			totalScanned++
			if layout.fieldFirst {
				if len(iter.Key()) <= len(lower) {
					b.Fatalf("field-first reverse-map key has no entity suffix: %x", iter.Key())
				}
				totalMatched++

				continue
			}

			parsed, ok := reverseMapBenchmarkParseEntityFirst(iter.Key())
			if !ok {
				b.Fatalf("malformed entity-first reverse-map key: %x", iter.Key())
			}
			if parsed.MetadataKey == field && parsed.Version == reverseMapBenchmarkVersion {
				totalMatched++
			}
		}

		if err := iter.Error(); err != nil {
			b.Fatal(err)
		}
		if err := iter.Close(); err != nil {
			b.Fatal(err)
		}
		operations++
	}

	b.ReportMetric(float64(totalScanned)/float64(operations), "scanned_rows/op")
	b.ReportMetric(float64(totalMatched)/float64(operations), "matched_rows/op")
}

func benchmarkReverseMapPurgeFieldPlan(
	b *testing.B,
	db *pebble.DB,
	layout reverseMapBenchmarkLayout,
	target reverseMapBenchmarkTarget,
	field string,
) {
	b.Helper()

	kb := dal.NewKeyBuilder()
	lower := reverseMapBenchmarkNamespacePrefix(kb, target.namespace)
	if layout.fieldFirst {
		lower = reverseMapBenchmarkFieldPrefix(kb, target, field)
	}
	upper := IncrementBytes(lower)
	var totalScanned, totalTombstones int64
	operations := 0

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		batch := db.NewBatch()

		if layout.fieldFirst {
			if err := batch.DeleteRange(lower, upper, nil); err != nil {
				b.Fatal(err)
			}
			totalTombstones++
		} else {
			iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
			if err != nil {
				b.Fatal(err)
			}

			for iter.First(); iter.Valid(); iter.Next() {
				totalScanned++
				parsed, ok := reverseMapBenchmarkParseEntityFirst(iter.Key())
				if !ok {
					b.Fatalf("malformed entity-first reverse-map key: %x", iter.Key())
				}
				if parsed.MetadataKey != field {
					continue
				}
				if err := batch.Delete(iter.Key(), nil); err != nil {
					b.Fatal(err)
				}
				totalTombstones++
			}

			if err := iter.Error(); err != nil {
				b.Fatal(err)
			}
			if err := iter.Close(); err != nil {
				b.Fatal(err)
			}
		}

		if err := batch.Close(); err != nil {
			b.Fatal(err)
		}
		operations++
	}

	b.ReportMetric(float64(totalScanned)/float64(operations), "scanned_rows/op")
	b.ReportMetric(float64(totalTombstones)/float64(operations), "tombstones/op")
}

func openReverseMapBenchmarkDB(
	b *testing.B,
	layout reverseMapBenchmarkLayout,
	target reverseMapBenchmarkTarget,
	fields []string,
	versions []uint32,
) *pebble.DB {
	b.Helper()

	db := openEmptyReverseMapBenchmarkDB(b)
	kb := dal.NewKeyBuilder()
	batch := db.NewBatch()

	for entityID := range reverseMapBenchmarkEntities {
		entity := target.entity(entityID)
		for _, version := range versions {
			for _, field := range fields {
				key := reverseMapBenchmarkKey(kb, layout, target, entity, field, version)
				if err := batch.Set(key, reverseMapBenchmarkValue, nil); err != nil {
					b.Fatal(err)
				}

				if batch.Len() >= 16<<20 {
					commitReverseMapBenchmarkBatch(b, batch)
					batch = db.NewBatch()
				}
			}
		}
	}

	commitReverseMapBenchmarkBatch(b, batch)
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}

	return db
}

func openEmptyReverseMapBenchmarkDB(b *testing.B) *pebble.DB {
	b.Helper()

	db, err := pebble.Open(b.TempDir(), &pebble.Options{
		Comparer:           ReadStoreComparer,
		DisableWAL:         true,
		FormatMajorVersion: pebble.FormatNewest,
		MemTableSize:       64 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})

	return db
}

func commitReverseMapBenchmarkBatch(b *testing.B, batch *pebble.Batch) {
	b.Helper()

	if batch.Len() > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
}

func reverseMapBenchmarkLookupKeys(
	layout reverseMapBenchmarkLayout,
	target reverseMapBenchmarkTarget,
	fields []string,
) [][]byte {
	keys := make([][]byte, 0, reverseMapBenchmarkEntities*len(fields))
	kb := dal.NewKeyBuilder()

	for entityID := range reverseMapBenchmarkEntities {
		entity := target.entity(entityID)
		for _, field := range fields {
			keys = append(keys, reverseMapBenchmarkKey(
				kb, layout, target, entity, field, reverseMapBenchmarkVersion,
			))
		}
	}

	return keys
}

func reverseMapBenchmarkFieldNames() []string {
	fields := make([]string, reverseMapBenchmarkFields)
	for i := range fields {
		fields[i] = fmt.Sprintf("metadata_%02d", i)
	}

	return fields
}

func reverseMapBenchmarkAccountEntity(id int) []byte {
	const prefix = "accounts:"

	entity := make([]byte, len(prefix)+10)
	copy(entity, prefix)

	for i := len(entity) - 1; i >= len(prefix); i-- {
		entity[i] = byte('0' + id%10)
		id /= 10
	}

	return entity
}

type reverseMapBenchmarkParsedEntityFirst struct {
	EntityID    []byte
	Version     uint32
	MetadataKey string
}

// reverseMapBenchmarkParseEntityFirst freezes the parsing work required by
// the former layout. Keeping it benchmark-local makes the comparison
// independent of the production field-first parser.
func reverseMapBenchmarkParseEntityFirst(key []byte) (reverseMapBenchmarkParsedEntityFirst, bool) {
	header := ledgerScopedPrefixLen + namespaceSize
	if len(key) < header || key[0] != PrefixReverseMap {
		return reverseMapBenchmarkParsedEntityFirst{}, false
	}

	namespace := string(key[ledgerScopedPrefixLen:header])
	rest := key[header:]
	var entity []byte

	switch namespace {
	case NamespaceAccount:
		end := bytes.IndexByte(rest, 0x00)
		if end <= 0 {
			return reverseMapBenchmarkParsedEntityFirst{}, false
		}
		entity = bytes.Clone(rest[:end])
		rest = rest[end+1:]
	case NamespaceTransaction:
		if len(rest) < 8 {
			return reverseMapBenchmarkParsedEntityFirst{}, false
		}
		entity = bytes.Clone(rest[:8])
		rest = rest[8:]
	default:
		return reverseMapBenchmarkParsedEntityFirst{}, false
	}

	if len(rest) <= 4 || bytes.IndexByte(rest[4:], 0x00) >= 0 {
		return reverseMapBenchmarkParsedEntityFirst{}, false
	}

	return reverseMapBenchmarkParsedEntityFirst{
		EntityID:    entity,
		Version:     binary.BigEndian.Uint32(rest[:4]),
		MetadataKey: string(rest[4:]),
	}, true
}

func reverseMapBenchmarkKey(
	kb *dal.KeyBuilder,
	layout reverseMapBenchmarkLayout,
	target reverseMapBenchmarkTarget,
	entity []byte,
	field string,
	version uint32,
) []byte {
	if layout.fieldFirst {
		switch target.namespace {
		case NamespaceAccount:
			return AccountReverseMapKeyV(kb, reverseMapBenchmarkLedger, string(entity), field, version)
		case NamespaceTransaction:
			return TransactionReverseMapKeyV(kb, reverseMapBenchmarkLedger, binary.BigEndian.Uint64(entity), field, version)
		default:
			panic("unsupported benchmark namespace")
		}
	}

	kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed(reverseMapBenchmarkLedger).
		PutNamespace(target.namespace).
		PutBytes(entity)
	if target.namespace == NamespaceAccount {
		kb.PutByte(0x00)
	}

	return kb.PutUint32(version).
		PutString(field).
		Build()
}

func reverseMapBenchmarkFieldPrefix(
	kb *dal.KeyBuilder,
	target reverseMapBenchmarkTarget,
	field string,
) []byte {
	return ReverseMapFieldPrefix(kb, reverseMapBenchmarkLedger, target.namespace, field)
}

func reverseMapBenchmarkNamespacePrefix(kb *dal.KeyBuilder, namespace string) []byte {
	return kb.Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed(reverseMapBenchmarkLedger).
		PutNamespace(namespace).
		Snapshot()
}

func reverseMapBenchmarkVersionPrefix(
	kb *dal.KeyBuilder,
	target reverseMapBenchmarkTarget,
	field string,
	version uint32,
) []byte {
	return ReverseMapVersionPrefix(kb, reverseMapBenchmarkLedger, target.namespace, field, version)
}
