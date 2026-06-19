package state

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// metaConvertFixture builds a ledger with a single CONVERTING account-
// metadata field and returns the fsm, the underlying dal.Store and the
// canonical key shared by every test in this file. The Machine itself
// no longer carries a Pebble read capability (#437), so tests that need
// to open a write session do it through the returned dataStore.
func metaConvertFixture(t *testing.T) (fsm *Machine, dataStore *dal.Store, canonicalKey []byte) {
	t.Helper()

	fsm, dataStore, _ = newTestMachine(t)

	ledgerKey := domain.LedgerKey{Name: "L"}
	info := &commonpb.LedgerInfo{
		Name: "L",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"role": {
					Type:   commonpb.MetadataType_METADATA_TYPE_STRING,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		},
	}

	batch := dataStore.OpenWriteSession()
	require.NoError(t, fsm.saveLedgerWithCache(batch, ledgerKey, info))
	require.NoError(t, batch.Commit())

	canonicalKey = domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"}.Bytes()

	return fsm, dataStore, canonicalKey
}

func mustMarshal(t *testing.T, v *commonpb.MetadataValue) []byte {
	t.Helper()

	b, err := v.MarshalVT()
	require.NoError(t, err)

	return b
}

// runTechApply wires a fresh WriteSet around a single tech-update handler
// call and drains the resulting overlay via Merge so the test observes the
// same post-apply state production sees. Mirrors what applyProposal does
// minus the order processing. The handler argument is `processing.Scope`
// — the same interface tech updates receive in production — so the test
// can't accidentally reach for engine fields.
func runTechApply(t *testing.T, fsm *Machine, batch *dal.WriteSession, plan *raftcmdpb.ExecutionPlan, fn func(scope processing.Scope) error) {
	t.Helper()

	fsm.writeSet.Reset(&commonpb.Timestamp{Data: 1700000000})
	buffer := fsm.writeSet

	ctx := logging.TestingContext()
	meter := noop.NewMeterProvider().Meter("test")
	miss, err := meter.Int64Counter("ledger.preload.coverage_miss")
	require.NoError(t, err)

	scope, err := NewScopeFactory(buffer, plan, logging.FromContext(ctx), miss, 0).NewProposalScope()
	require.NoError(t, err)

	require.NoError(t, fn(scope))
	require.NoError(t, buffer.Merge(batch, nil))
}

func applyConversion(t *testing.T, fsm *Machine, dataStore *dal.Store, canonicalKey []byte, entry *raftcmdpb.ConvertMetadataEntry) {
	t.Helper()

	applyBatch := dataStore.OpenWriteSession()
	plan := buildConvertPlan(t, "L", canonicalKey, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
	runTechApply(t, fsm, applyBatch, plan, func(scope processing.Scope) error {
		return fsm.applyMetadataConversionBatch(scope, applyBatch, &raftcmdpb.MetadataConversionBatch{
			Ledger:       "L",
			TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:          "role",
			ExpectedType: commonpb.MetadataType_METADATA_TYPE_STRING,
			Entries:      []*raftcmdpb.ConvertMetadataEntry{entry},
		})
	})
	require.NoError(t, applyBatch.Commit())
}

// buildConvertPlan declares the keys read by applyMetadataConversionBatch /
// applyMetadataConversionCompletion: the ledger (always) plus the canonical
// metadata key dispatched on targetType.
func buildConvertPlan(t *testing.T, ledger string, canonicalKey []byte, targetType commonpb.TargetType) *raftcmdpb.ExecutionPlan {
	t.Helper()

	ledgerID, _ := attributes.MakeKey(domain.LedgerKey{Name: ledger}.Bytes())
	metaID, _ := attributes.MakeKey(canonicalKey)

	attrCode := dal.SubAttrMetadata
	if targetType == commonpb.TargetType_TARGET_TYPE_LEDGER {
		attrCode = dal.SubAttrLedgerMetadata
	}

	return &raftcmdpb.ExecutionPlan{
		Attributes: []*raftcmdpb.AttributePlan{
			declareTestPlan(ledgerID, dal.SubAttrLedger),
			declareTestPlan(metaID, attrCode),
		},
	}
}

// TestApplyMetadataConversionBatch_WritesWhenCacheMatchesScan is the
// happy path. Cache holds the originally scanned value; the converter
// has shipped both the converted_value AND the expected raw bytes.
// Compare-and-set succeeds, the converted value is written.
func TestApplyMetadataConversionBatch_WritesWhenCacheMatchesScan(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	// Scan-time value (numeric "42" before the field went STRING).
	original := commonpb.NewStringValue("42")

	// Pre-populate the cache with the same value the converter scanned.
	seed := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.AccountMetadata.PutWithCache(
		seed,
		byte(fsm.Registry.Cache.CurrentGeneration()%2),
		canonicalKey,
		original,
	)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())

	applyConversion(t, fsm, dataStore, canonicalKey, &raftcmdpb.ConvertMetadataEntry{
		CanonicalKey:   canonicalKey,
		ConvertedValue: commonpb.NewStringValue("42-converted"),
		ExpectedValue:  mustMarshal(t, original),
	})

	got, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.NoError(t, err)
	require.Equal(t, "42-converted", got.GetStringValue())
}

// TestApplyMetadataConversionBatch_SkipsWhenCacheMutated pins the fix
// for #359. The converter scanned alice/role = "1"; a normal metadata
// command then wrote alice/role = "user-overrode-it" through the FSM;
// finally the stale conversion batch arrives. The schema state guard
// still says CONVERTING, but the per-key compare-and-set notices the
// mutation and skips the entry so the user's newer write is preserved.
func TestApplyMetadataConversionBatch_SkipsWhenCacheMutated(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	original := commonpb.NewStringValue("1")
	userOverride := commonpb.NewStringValue("user-overrode-it")

	// Cache now holds the user's newer value, not the scanned one.
	seed := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.AccountMetadata.PutWithCache(
		seed,
		byte(fsm.Registry.Cache.CurrentGeneration()%2),
		canonicalKey,
		userOverride,
	)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())

	applyConversion(t, fsm, dataStore, canonicalKey, &raftcmdpb.ConvertMetadataEntry{
		CanonicalKey:   canonicalKey,
		ConvertedValue: commonpb.NewStringValue("converted-from-stale-scan"),
		ExpectedValue:  mustMarshal(t, original),
	})

	got, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.NoError(t, err)
	require.Equal(t, "user-overrode-it", got.GetStringValue(),
		"a user mutation between scan and apply MUST NOT be clobbered by the conversion (#359)")
}

// TestApplyMetadataConversionBatch_SkipsWhenTombstoned pins the
// deletion-resurrection scenario from #359. After the converter scans
// alice/role = "1", a normal metadata command deletes alice/role
// (FSM writes a cache tombstone). The stale conversion batch arrives
// and MUST NOT resurrect the deleted value.
func TestApplyMetadataConversionBatch_SkipsWhenTombstoned(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	original := commonpb.NewStringValue("1")

	// Write, then delete the key — this is exactly the sequence the FSM
	// applies for a user write followed by a delete command.
	seed := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.AccountMetadata.PutWithCache(
		seed,
		byte(fsm.Registry.Cache.CurrentGeneration()%2),
		canonicalKey,
		original,
	)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())

	_, _, err = fsm.Registry.AccountMetadata.KeyStore().Delete(canonicalKey)
	require.NoError(t, err)

	applyConversion(t, fsm, dataStore, canonicalKey, &raftcmdpb.ConvertMetadataEntry{
		CanonicalKey:   canonicalKey,
		ConvertedValue: commonpb.NewStringValue("converted-from-stale-scan"),
		ExpectedValue:  mustMarshal(t, original),
	})

	_, _, err = fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.ErrorIs(t, err, domain.ErrNotFound,
		"a user deletion between scan and apply MUST NOT be undone by the conversion (#359)")
}

// TestApplyMetadataConversionBatch_SkipsOnCacheMiss pins the
// scan-vs-apply race tolerance: the converter scans Pebble at T0,
// enqueues canonical keys, but a user delete can land between scan
// and apply. After enough intervening proposals the cache tombstone
// rotates out and the proposer's preload finds nothing in cache or
// Pebble — so the preload payload omits this key entirely. The apply
// must treat "absent everywhere" as a stale conversion to skip, not
// as a contract violation; otherwise a normal race turns into an
// apply failure (flemzord review on #359).
func TestApplyMetadataConversionBatch_SkipsOnCacheMiss(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	// Sanity: the key is genuinely absent from the cache (no
	// MirrorPreload populated it — modelling the "delete + rotation"
	// race).
	_, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.ErrorIs(t, err, domain.ErrNotFound)

	applyBatch := dataStore.OpenWriteSession()
	plan := buildConvertPlan(t, "L", canonicalKey, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
	runTechApply(t, fsm, applyBatch, plan, func(buffer processing.Scope) error {
		return fsm.applyMetadataConversionBatch(buffer, applyBatch, &raftcmdpb.MetadataConversionBatch{
			Ledger:       "L",
			TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:          "role",
			ExpectedType: commonpb.MetadataType_METADATA_TYPE_STRING,
			Entries: []*raftcmdpb.ConvertMetadataEntry{{
				CanonicalKey:   canonicalKey,
				ConvertedValue: commonpb.NewStringValue("ghost"),
				ExpectedValue:  mustMarshal(t, commonpb.NewStringValue("1")),
			}},
		})
	})
	require.NoError(t, applyBatch.Commit())

	// Sanity: nothing was written — the conversion was silently
	// skipped, just like the tombstone branch.
	_, _, err = fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

// TestApplyMetadataConversionBatch_NeverAutoCompletes pins the design
// change that closed the cross-pass premature-COMPLETE hole raised on
// PR #359 (paul-nicolas review). The FSM no longer tracks
// TotalKeys/ConvertedKeys nor flips Status from a batch apply — even
// when every entry in the batch succeeds. The only path to COMPLETE
// is applyMetadataConversionCompletion, proposed by the converter
// after a full Pebble scan turned up zero entries needing conversion.
func TestApplyMetadataConversionBatch_NeverAutoCompletes(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	// Seed the cache so the single entry below actually writes.
	original := commonpb.NewStringValue("1")
	seed := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.AccountMetadata.PutWithCache(
		seed,
		byte(fsm.Registry.Cache.CurrentGeneration()%2),
		canonicalKey,
		original,
	)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())

	applyConversion(t, fsm, dataStore, canonicalKey, &raftcmdpb.ConvertMetadataEntry{
		CanonicalKey:   canonicalKey,
		ConvertedValue: commonpb.NewStringValue("converted"),
		ExpectedValue:  mustMarshal(t, original),
	})

	// The value was written through.
	got, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.NoError(t, err)
	require.Equal(t, "converted", got.GetStringValue())

	// But Status must stay CONVERTING — the batch apply NEVER flips
	// COMPLETE on its own (only proposeComplete does).
	info, _, err := fsm.Registry.Ledgers.Get(domain.LedgerKey{Name: "L"}.Bytes())
	require.NoError(t, err)

	field := info.GetMetadataSchema().GetAccountFields()["role"]
	require.NotNil(t, field)
	require.Equal(t, commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING, field.GetStatus(),
		"applyMetadataConversionBatch must NEVER flip Status — converter owns the COMPLETE transition (#359 paul-nicolas)")
}

// TestApplyMetadataConversionCompletion_FlipsStatusComplete pins the
// only path through which a field reaches COMPLETE on the
// ACCOUNT/LEDGER path: a MetadataConversionCompletion proposal,
// emitted by the converter once a full pass turned up zero entries
// needing conversion.
func TestApplyMetadataConversionCompletion_FlipsStatusComplete(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := metaConvertFixture(t)

	applyBatch := dataStore.OpenWriteSession()
	canonicalKey := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "role"}.Bytes()
	plan := buildConvertPlan(t, "L", canonicalKey, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
	runTechApply(t, fsm, applyBatch, plan, func(buffer processing.Scope) error {
		return fsm.applyMetadataConversionCompletion(buffer, &raftcmdpb.MetadataConversionCompletion{
			Ledger:       "L",
			TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:          "role",
			ExpectedType: commonpb.MetadataType_METADATA_TYPE_STRING,
		})
	})
	require.NoError(t, applyBatch.Commit())

	info, _, err := fsm.Registry.Ledgers.Get(domain.LedgerKey{Name: "L"}.Bytes())
	require.NoError(t, err)

	field := info.GetMetadataSchema().GetAccountFields()["role"]
	require.NotNil(t, field)
	require.Equal(t, commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE, field.GetStatus(),
		"applyMetadataConversionCompletion must flip Status to COMPLETE")
}

// TestApplyMetadataConversionBatch_StaleSchemaStillSkips documents the
// upstream staleness guard kept on top of the per-entry compare-and-set:
// a batch whose schema field is no longer CONVERTING (or whose type
// moved on) is silently dropped before even reaching the entry loop.
func TestApplyMetadataConversionBatch_StaleSchemaStillSkips(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	ledgerKey := domain.LedgerKey{Name: "L"}
	info := &commonpb.LedgerInfo{
		Name: "L",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"role": {
					Type:   commonpb.MetadataType_METADATA_TYPE_STRING,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
				},
			},
		},
	}

	batch := dataStore.OpenWriteSession()
	require.NoError(t, fsm.saveLedgerWithCache(batch, ledgerKey, info))
	require.NoError(t, batch.Commit())

	canonicalKey := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"}.Bytes()

	applyBatch := dataStore.OpenWriteSession()
	plan := buildConvertPlan(t, "L", canonicalKey, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
	runTechApply(t, fsm, applyBatch, plan, func(buffer processing.Scope) error {
		return fsm.applyMetadataConversionBatch(buffer, applyBatch, &raftcmdpb.MetadataConversionBatch{
			Ledger:       "L",
			TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:          "role",
			ExpectedType: commonpb.MetadataType_METADATA_TYPE_STRING,
			Entries: []*raftcmdpb.ConvertMetadataEntry{
				{CanonicalKey: canonicalKey, ConvertedValue: commonpb.NewStringValue("ghost")},
			},
		})
	})
	require.NoError(t, applyBatch.Commit())

	_, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.True(t, errors.Is(err, domain.ErrNotFound),
		"stale batch (schema no longer CONVERTING) must be ignored, got err=%v", err)
}

// TestApplyProposal_PreloadPopulatesCacheBeforeConvert is the
// end-to-end regression flemzord explicitly asked for on PR #359:
// "starts with a canonical metadata entry present only in Pebble
// (not in cache), runs a conversion batch through applyProposal, and
// asserts the stored value is converted in the cache."
//
// Setup: the canonical key is absent from the cache. The proposal
// carries a Preload entry (populating the cache during applyProposal's
// preload step) AND a MetadataConversionBatch (consumed by
// applyMetadataConversionBatch, which compare-and-set's against the
// now-populated cache and writes the converted value).
//
// This test exercises the full applyProposal path — not just
// applyMetadataConversionBatch — so it pins the Phase-1 reorder
// (preload before technical updates) and the per-entry CAS in one
// shot. The COMPLETE transition is the converter's job (via
// proposeComplete after a clean scan), not this batch's.
func TestApplyProposal_PreloadPopulatesCacheBeforeConvert(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	// Sanity: the key is genuinely absent from the cache. In a real
	// run, the converter's scanner would have read this value from
	// the Pebble snapshot. Here we just trust that the preload entry
	// the leader ships matches what Pebble has.
	_, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.ErrorIs(t, err, domain.ErrNotFound)

	original := commonpb.NewStringValue("scanned-value")

	// Build the AttributeID the Preload entry needs (hash of the
	// canonical key, same machinery the live preloader uses).
	u128, tag := attributes.MakeKey(canonicalKey)
	attrID := &raftcmdpb.AttributeID{Id: u128.Bytes(), Tag: tag}

	// applyMetadataConversionBatch also reads the ledger info via the
	// Plan, so the proposal must declare it — without a Declare
	// entry the View crashes the node on the read.
	ledgerID, _ := attributes.MakeKey(domain.LedgerKey{Name: "L"}.Bytes())

	proposal := &raftcmdpb.Proposal{
		Id: 1,
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
			Attributes: []*raftcmdpb.AttributePlan{
				preloadTestPlan(attrID, dal.SubAttrMetadata, rawPreload(t, dal.SubAttrMetadata, original)),
				declareTestPlan(ledgerID, dal.SubAttrLedger),
			},
		},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			// Coverage bits flag both plans[0] (account metadata) and
			// plans[1] (ledger). The MetadataBatch handler reads the
			// canonical key through scope.GetAccountMetadataEntry AND
			// reads Registry.Ledgers["L"] for the schema check.
			CoverageBits: []byte{0b00000011},
			Kind: &raftcmdpb.TechnicalUpdate_MetadataBatch{
				MetadataBatch: &raftcmdpb.MetadataConversionBatch{
					Ledger:       "L",
					TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:          "role",
					ExpectedType: commonpb.MetadataType_METADATA_TYPE_STRING,
					Entries: []*raftcmdpb.ConvertMetadataEntry{{
						CanonicalKey:   canonicalKey,
						ConvertedValue: commonpb.NewStringValue("converted"),
						ExpectedValue:  mustMarshal(t, original),
					}},
				},
			},
		}},
	}

	batch := dataStore.OpenWriteSession()
	_, err = fsm.applyProposal(context.Background(), 1, batch, proposal)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Cache must hold the converted value (preload filled it with
	// "scanned-value", then the convert CAS matched and overwrote
	// with "converted").
	got, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.NoError(t, err)
	require.Equal(t, "converted", got.GetStringValue(),
		"preload + per-entry CAS must produce the converted value in the cache (#359)")

	// Schema must STAY CONVERTING — the batch apply no longer flips
	// COMPLETE. The converter does that on a subsequent clean pass via
	// applyMetadataConversionCompletion.
	info, _, err := fsm.Registry.Ledgers.Get(domain.LedgerKey{Name: "L"}.Bytes())
	require.NoError(t, err)

	field := info.GetMetadataSchema().GetAccountFields()["role"]
	require.NotNil(t, field)
	require.Equal(t, commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING, field.GetStatus(),
		"Batch apply must not flip Status — converter owns the COMPLETE transition (#359)")
}

// TestApplyProposal_PreloadDoesNotResurrectTombstone pins the
// "preload over tombstone" race surfaced by NumaryBot on PR #359:
// a metadata delete that lands between the converter's scan and the
// conversion proposal writes a tombstone in the cache; the
// conversion proposal still carries the scanned value in its preload.
// Without protection, the preload would overwrite the tombstone,
// `applyConvertEntry` would no longer see Deleted, and the CAS would
// resurrect the metadata the user deleted.
//
// MirrorPreload now preserves tombstones (cache_snapshotter.go), so
// the preload no-ops on a tombstoned key and the CAS correctly skips
// the conversion entry.
func TestApplyProposal_PreloadDoesNotResurrectTombstone(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	original := commonpb.NewStringValue("scanned-value")

	// Step 1: simulate a normal write followed by a delete — the
	// sequence the FSM applies on a user SET-then-DELETE through the
	// metadata command path. This leaves a tombstone in the cache.
	seed := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.AccountMetadata.PutWithCache(
		seed,
		byte(fsm.Registry.Cache.CurrentGeneration()%2),
		canonicalKey,
		original,
	)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())

	_, _, err = fsm.Registry.AccountMetadata.KeyStore().Delete(canonicalKey)
	require.NoError(t, err)

	// Sanity: the key reads as deleted (tombstone present).
	_, _, err = fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Step 2: a stale conversion proposal arrives carrying both a
	// Preload entry (the scanned value, predating the delete) AND a
	// conversion batch (the converted form of that scanned value).
	u128, tag := attributes.MakeKey(canonicalKey)
	attrID := &raftcmdpb.AttributeID{Id: u128.Bytes(), Tag: tag}

	// applyMetadataConversionBatch reads view.Ledgers; declare the ledger
	// key so the view admits the read instead of crashing on coverage miss.
	ledgerU128, _ := attributes.MakeKey(domain.LedgerKey{Name: "L"}.Bytes())

	proposal := &raftcmdpb.Proposal{
		Id: 1,
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
			Attributes: []*raftcmdpb.AttributePlan{
				declareTestPlan(ledgerU128, dal.SubAttrLedger),
				preloadTestPlan(attrID, dal.SubAttrMetadata, rawPreload(t, dal.SubAttrMetadata, original)),
			},
		},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			// Coverage bits flag both plans[0] (ledger) and plans[1] (account
			// metadata). The MetadataBatch handler reads both through scope.
			CoverageBits: []byte{0b00000011},
			Kind: &raftcmdpb.TechnicalUpdate_MetadataBatch{
				MetadataBatch: &raftcmdpb.MetadataConversionBatch{
					Ledger:       "L",
					TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:          "role",
					ExpectedType: commonpb.MetadataType_METADATA_TYPE_STRING,
					Entries: []*raftcmdpb.ConvertMetadataEntry{{
						CanonicalKey:   canonicalKey,
						ConvertedValue: commonpb.NewStringValue("must-not-resurrect"),
						ExpectedValue:  mustMarshal(t, original),
					}},
				},
			},
		}},
	}

	batch := dataStore.OpenWriteSession()
	_, err = fsm.applyProposal(context.Background(), 1, batch, proposal)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// The tombstone MUST survive — the preload must not have
	// overwritten it. If it did, the CAS would have matched the
	// scanned value and written "must-not-resurrect" to the cache.
	_, _, err = fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.ErrorIs(t, err, domain.ErrNotFound,
		"tombstone must survive a stale preload + conversion batch — never resurrect a user deletion (#359)")
}

// TestApplyProposal_PreloadOverColliderTombstoneIsApplied is the regression
// guard for the U128-collision corner case flagged on NumaryBot's review:
// the tombstone-preservation path in MirrorPreload must compare both
// `existing.Deleted` AND `existing.Tag` against the incoming `attrID.Tag`.
// Without the tag check, a tombstone for canonical-key A would suppress
// the preload for canonical-key B that happens to hash to the same U128
// id, leaving the FSM apply path with a cache miss and silently skipping
// work even though Pebble still has B's value.
//
// We can't make two real canonical keys collide on U128 by construction,
// so the test injects a fake tombstone for an unrelated tag into the
// cache, then invokes MirrorPreload with a different tag for the same
// id and asserts the preload value lands.
func TestApplyProposal_PreloadOverColliderTombstoneIsApplied(t *testing.T) {
	t.Parallel()

	fsm, dataStore, canonicalKey := metaConvertFixture(t)

	// Build the AttributeID for canonicalKey (this is the key we're
	// preloading).
	u128, tag := attributes.MakeKey(canonicalKey)
	attrID := &raftcmdpb.AttributeID{Id: u128.Bytes(), Tag: tag}

	// Inject a fake tombstone at the SAME u128 but with a DIFFERENT tag
	// (simulating a collider that already had a delete applied).
	const colliderTag uint64 = 0xDEADBEEF
	require.NotEqual(t, colliderTag, tag, "test setup: pick a colliderTag distinct from the real one")

	fsm.Registry.Cache.AccountMetadata.Gen0().Put(u128, attributes.Entry[*commonpb.MetadataValue]{
		Tag:     colliderTag,
		Deleted: true,
	})

	// A proposal carrying a preload for our key. Without the tag check,
	// MirrorPreload would observe the collider's tombstone and skip the
	// preload — the cache would remain `Tag=colliderTag, Deleted=true`
	// and our `Get(canonicalKey)` would return ErrNotFound (a silent
	// drop). With the tag check, the preload writes our value through.
	preloaded := commonpb.NewStringValue("preloaded-after-collider-tombstone")

	proposal := &raftcmdpb.Proposal{
		Id: 1,
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
			Attributes: []*raftcmdpb.AttributePlan{
				preloadTestPlan(attrID, dal.SubAttrMetadata, rawPreload(t, dal.SubAttrMetadata, preloaded)),
			},
		},
	}

	batch := dataStore.OpenWriteSession()
	_, err := fsm.applyProposal(context.Background(), 1, batch, proposal)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// The cache must now have our (Tag=tag, value) — not the collider's
	// tombstone. Reading via the canonical key resolves through the tag
	// check and returns the preloaded value.
	got, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
	require.NoError(t, err)
	require.Equal(t, preloaded.GetStringValue(), got.GetStringValue(),
		"preload over a tombstone for a U128 collider must apply — "+
			"without the tag check, the preload silently no-ops")
}
