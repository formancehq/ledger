package indexbuilder

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

const (
	reverseMapFoldBenchmarkLedger       = "benchmark"
	reverseMapFoldBenchmarkSeedEntities = 10_000
	reverseMapFoldBenchmarkHotEntities  = 100
	reverseMapFoldBenchmarkVersion      = uint32(1)
)

type reverseMapFoldBenchmarkLayout struct {
	name       string
	fieldFirst bool
}

var reverseMapFoldBenchmarkLayouts = []reverseMapFoldBenchmarkLayout{
	{name: "entity_first_baseline"},
	{name: "field_first_production", fieldFirst: true},
}

type reverseMapFoldBenchmarkTarget struct {
	name       string
	namespace  string
	targetType commonpb.TargetType
	entity     func(uint64) []byte
}

var reverseMapFoldBenchmarkTargets = []reverseMapFoldBenchmarkTarget{
	{
		name:       "account",
		namespace:  readstore.NamespaceAccount,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		entity:     reverseMapFoldBenchmarkAccountEntity,
	},
	{
		name:       "transaction",
		namespace:  readstore.NamespaceTransaction,
		targetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
		entity:     reverseMapFoldBenchmarkTransactionEntity,
	},
}

var reverseMapFoldBenchmarkValues = []*commonpb.MetadataValue{
	{Type: &commonpb.MetadataValue_StringValue{StringValue: "value-a"}},
	{Type: &commonpb.MetadataValue_StringValue{StringValue: "value-b"}},
}

// BenchmarkReverseMapKeyingFoldSavedMetadata measures the complete metadata
// indexing work performed for SavedMetadata: value coercion and encoding,
// authoritative reverse-map reads, forward ADD/DEL events, existence events
// when needed, reverse-map writes and overlay maintenance, plus the per-log
// index and one real Pebble commit per DefaultBatchSize logs.
//
// Each batch updates distinct entities, so every reverse-map lookup exercises
// the committed Pebble path rather than the same-batch overlay. The separate
// HotAccountOverlay benchmark below covers that branch.
//
// This fixture is package-local. The readstore keying benchmark models a
// different workload and may use different entity-ID widths; only layout
// comparisons within this suite are valid, not ratios across the two suites.
func BenchmarkReverseMapKeyingFoldSavedMetadata(b *testing.B) {
	for _, target := range reverseMapFoldBenchmarkTargets {
		b.Run(target.name, func(b *testing.B) {
			for _, fieldCount := range []int{1, 4, 12} {
				fields := reverseMapFoldBenchmarkFields(fieldCount)
				b.Run(fmt.Sprintf("fields=%d", fieldCount), func(b *testing.B) {
					for _, layout := range reverseMapFoldBenchmarkLayouts {
						b.Run(layout.name, func(b *testing.B) {
							benchmarkReverseMapFoldSavedMetadata(
								b,
								layout,
								target,
								fields,
								reverseMapFoldBenchmarkSeedEntities,
								0,
							)
						})
					}
				})
			}
		})
	}
}

// BenchmarkReverseMapKeyingFoldHotAccountOverlay models a 1,000-log batch
// repeatedly updating a small account set. The first lookup for each
// (entity, field) reads Pebble; the remaining nine resolve from WriteBatch's
// read-your-writes reverse-map overlay. This makes sure the production layout
// does not win or lose solely because the committed-read workload above omits overlay
// traffic.
func BenchmarkReverseMapKeyingFoldHotAccountOverlay(b *testing.B) {
	fields := reverseMapFoldBenchmarkFields(12)
	target := reverseMapFoldBenchmarkTargets[0]

	for _, layout := range reverseMapFoldBenchmarkLayouts {
		b.Run(layout.name, func(b *testing.B) {
			benchmarkReverseMapFoldSavedMetadata(
				b,
				layout,
				target,
				fields,
				reverseMapFoldBenchmarkHotEntities,
				0.9,
			)
		})
	}
}

// BenchmarkReverseMapKeyingFoldCreatedTransaction measures freshly-minted
// transaction metadata through the production insert-known-absent helper. No
// reverse-map point read is issued. Each synthetic transaction also writes the
// unconditional ledger-log index and representative built-in/address indexes,
// so this reports the keying cost as a fraction of a transaction fold rather
// than as isolated reverse-map Set calls.
func BenchmarkReverseMapKeyingFoldCreatedTransaction(b *testing.B) {
	for _, fieldCount := range []int{1, 4, 12} {
		fields := reverseMapFoldBenchmarkFields(fieldCount)
		b.Run(fmt.Sprintf("fields=%d", fieldCount), func(b *testing.B) {
			for _, layout := range reverseMapFoldBenchmarkLayouts {
				b.Run(layout.name, func(b *testing.B) {
					benchmarkReverseMapFoldCreatedTransaction(b, layout, fields)
				})
			}
		})
	}
}

func benchmarkReverseMapFoldSavedMetadata(
	b *testing.B,
	layout reverseMapFoldBenchmarkLayout,
	target reverseMapFoldBenchmarkTarget,
	fields []string,
	entityCount uint64,
	overlayHitRatio float64,
) {
	b.Helper()

	builder := newReverseMapFoldBenchmarkBuilder(b, target, fields)
	nextSequence := seedReverseMapFoldBenchmark(b, builder, layout, target, fields, entityCount)
	nextMutation := uint64(0)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		batch := builder.readStore.NewBatch()
		builder.initBatch(batch)

		for range DefaultBatchSize {
			sequence := nextSequence
			nextSequence++
			builder.wb.SetEventSequence(sequence)

			if err := builder.wb.WriteLedgerLogIndex(builder.kb, reverseMapFoldBenchmarkLedger, sequence, sequence); err != nil {
				b.Fatal(err)
			}

			entityOrdinal := nextMutation % entityCount
			generation := nextMutation/entityCount + 1
			entityID := target.entity(entityOrdinal)
			value := reverseMapFoldBenchmarkValues[generation%uint64(len(reverseMapFoldBenchmarkValues))]

			for _, field := range fields {
				reverseKey := reverseMapFoldBenchmarkKey(
					builder.kb, layout, target, entityID, field, reverseMapFoldBenchmarkVersion,
				)
				if err := builder.writeMetadataIndexAtVersion(
					builder.kb,
					reverseMapFoldBenchmarkLedger,
					target.namespace,
					field,
					target.targetType,
					reverseMapFoldBenchmarkVersion,
					value,
					entityID,
					reverseKey,
				); err != nil {
					b.Fatal(err)
				}
			}

			nextMutation++
		}

		commitReverseMapFoldBenchmarkBatch(b, builder, nextSequence-1)
	}

	reportReverseMapFoldBenchmarkMetrics(b, len(fields), len(fields), 1+3*len(fields))
	b.ReportMetric(overlayHitRatio, "overlay_hit_ratio")
}

func benchmarkReverseMapFoldCreatedTransaction(
	b *testing.B,
	layout reverseMapFoldBenchmarkLayout,
	fields []string,
) {
	b.Helper()

	target := reverseMapFoldBenchmarkTargets[1]
	builder := newReverseMapFoldBenchmarkBuilder(b, target, fields)
	nextSequence := uint64(1)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		batch := builder.readStore.NewBatch()
		builder.initBatch(batch)

		for range DefaultBatchSize {
			sequence := nextSequence
			nextSequence++
			transactionID := sequence + 1
			builder.wb.SetEventSequence(sequence)

			if err := builder.wb.WriteLedgerLogIndex(builder.kb, reverseMapFoldBenchmarkLedger, sequence, sequence); err != nil {
				b.Fatal(err)
			}
			if err := writeReverseMapFoldBenchmarkTransactionIndexes(builder, transactionID); err != nil {
				b.Fatal(err)
			}

			entityID := target.entity(sequence)
			value := reverseMapFoldBenchmarkValues[sequence%uint64(len(reverseMapFoldBenchmarkValues))]
			for _, field := range fields {
				reverseKey := reverseMapFoldBenchmarkKey(
					builder.kb, layout, target, entityID, field, reverseMapFoldBenchmarkVersion,
				)
				if err := builder.insertMetadataIndexAtVersion(
					builder.kb,
					reverseMapFoldBenchmarkLedger,
					target.namespace,
					field,
					target.targetType,
					reverseMapFoldBenchmarkVersion,
					value,
					entityID,
					reverseKey,
				); err != nil {
					b.Fatal(err)
				}
			}
		}

		commitReverseMapFoldBenchmarkBatch(b, builder, nextSequence-1)
	}

	// Seven fixed writes: ledger log, two any-role account mappings, source,
	// destination, timestamp and inserted-at. Each fresh metadata field adds
	// forward ADD + existence ADD + reverse-map Set.
	reportReverseMapFoldBenchmarkMetrics(b, len(fields), 0, 7+3*len(fields))
}

func newReverseMapFoldBenchmarkBuilder(
	b *testing.B,
	target reverseMapFoldBenchmarkTarget,
	fields []string,
) *Builder {
	b.Helper()

	store, err := readstore.New(b.TempDir(), noopLogger{}, readstore.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})

	builder := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
	}

	for _, field := range fields {
		canonical := indexes.Canonical(indexes.MetadataID(target.targetType, field))
		builder.putVersionState(reverseMapFoldBenchmarkLedger, canonical, readstore.IndexVersionState{
			CurrentVersion: reverseMapFoldBenchmarkVersion,
			HighWater:      reverseMapFoldBenchmarkVersion,
		})
	}

	return builder
}

func seedReverseMapFoldBenchmark(
	b *testing.B,
	builder *Builder,
	layout reverseMapFoldBenchmarkLayout,
	target reverseMapFoldBenchmarkTarget,
	fields []string,
	entityCount uint64,
) uint64 {
	b.Helper()

	nextSequence := uint64(1)
	for first := uint64(0); first < entityCount; first += DefaultBatchSize {
		batch := builder.readStore.NewBatch()
		builder.initBatch(batch)
		last := min(first+DefaultBatchSize, entityCount)

		for entityOrdinal := first; entityOrdinal < last; entityOrdinal++ {
			sequence := nextSequence
			nextSequence++
			builder.wb.SetEventSequence(sequence)
			entityID := target.entity(entityOrdinal)

			for _, field := range fields {
				reverseKey := reverseMapFoldBenchmarkKey(
					builder.kb, layout, target, entityID, field, reverseMapFoldBenchmarkVersion,
				)
				if err := builder.insertMetadataIndexAtVersion(
					builder.kb,
					reverseMapFoldBenchmarkLedger,
					target.namespace,
					field,
					target.targetType,
					reverseMapFoldBenchmarkVersion,
					reverseMapFoldBenchmarkValues[0],
					entityID,
					reverseKey,
				); err != nil {
					b.Fatal(err)
				}
			}
		}

		commitReverseMapFoldBenchmarkBatch(b, builder, nextSequence-1)
	}

	if err := builder.readStore.DB().Flush(); err != nil {
		b.Fatal(err)
	}

	return nextSequence
}

func commitReverseMapFoldBenchmarkBatch(b *testing.B, builder *Builder, lastSequence uint64) {
	b.Helper()

	batch := builder.wb.Batch()
	if batch == nil {
		b.Fatal("benchmark write batch is not initialized")
	}
	if err := builder.readStore.WriteProgress(batch, lastSequence); err != nil {
		// Best-effort cleanup; the WriteProgress failure is the primary error.
		_ = batch.Cancel()
		b.Fatal(err)
	}
	if err := builder.wb.Flush(); err != nil {
		b.Fatal(err)
	}
}

func writeReverseMapFoldBenchmarkTransactionIndexes(builder *Builder, transactionID uint64) error {
	const (
		source      = "users:source"
		destination = "users:destination"
	)

	wb := builder.wb
	kb := builder.kb
	ledger := reverseMapFoldBenchmarkLedger

	if err := wb.WriteAccountTxMapping(kb, ledger, source, transactionID); err != nil {
		return err
	}
	if err := wb.WriteAccountTxMapping(kb, ledger, destination, transactionID); err != nil {
		return err
	}
	if err := wb.WriteSourceAccountTxMapping(kb, ledger, source, transactionID); err != nil {
		return err
	}
	if err := wb.WriteDestinationAccountTxMapping(kb, ledger, destination, transactionID); err != nil {
		return err
	}
	if err := wb.WriteTransactionTimestampIndex(kb, ledger, transactionID, transactionID); err != nil {
		return err
	}

	return wb.WriteTransactionInsertedAtIndex(kb, ledger, transactionID, transactionID)
}

func reportReverseMapFoldBenchmarkMetrics(b *testing.B, fields, reverseReads, indexOperations int) {
	b.Helper()

	logs := float64(b.N * DefaultBatchSize)
	nanosPerLog := float64(b.Elapsed().Nanoseconds()) / logs
	b.ReportMetric(float64(DefaultBatchSize), "logs/op")
	b.ReportMetric(float64(fields), "metadata_fields/log")
	b.ReportMetric(float64(reverseReads), "rmap_lookups/log")
	b.ReportMetric(float64(indexOperations), "index_ops/log")
	b.ReportMetric(nanosPerLog, "ns/log")
	b.ReportMetric(1e9/nanosPerLog, "logs/s")
}

func reverseMapFoldBenchmarkFields(count int) []string {
	fields := make([]string, count)
	for i := range fields {
		fields[i] = fmt.Sprintf("metadata_%02d", i)
	}

	return fields
}

func reverseMapFoldBenchmarkAccountEntity(id uint64) []byte {
	const prefix = "accounts:"

	entity := make([]byte, len(prefix)+12)
	copy(entity, prefix)
	for i := len(entity) - 1; i >= len(prefix); i-- {
		entity[i] = byte('0' + id%10)
		id /= 10
	}

	return entity
}

func reverseMapFoldBenchmarkTransactionEntity(id uint64) []byte {
	entity := make([]byte, 8)
	binary.BigEndian.PutUint64(entity, id+1)

	return entity
}

func reverseMapFoldBenchmarkKey(
	kb *dal.KeyBuilder,
	layout reverseMapFoldBenchmarkLayout,
	target reverseMapFoldBenchmarkTarget,
	entityID []byte,
	field string,
	version uint32,
) []byte {
	if layout.fieldFirst {
		return metadataReverseMapKeyV(
			kb,
			reverseMapFoldBenchmarkLedger,
			target.targetType,
			entityID,
			field,
			version,
		)
	}

	kb.Reset().
		PutByte(readstore.PrefixReverseMap).
		PutLedgerNameFixed(reverseMapFoldBenchmarkLedger).
		PutNamespace(target.namespace).
		PutBytes(entityID)
	if target.namespace == readstore.NamespaceAccount {
		kb.PutByte(0x00)
	}

	return kb.PutUint32(version).
		PutString(field).
		Build()
}
