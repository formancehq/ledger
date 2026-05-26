package state

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// verifyCacheRestoreCoherence checks two invariants after RestoreFromStore:
//
//  1. 0xFF ↔ memory: every entry in the 0xFF Pebble zone is present in the
//     corresponding in-memory generation, and vice versa.
//  2. 0xF1 ↔ 0xFF: every key in the 0xF1 attributes zone has a corresponding
//     entry in the 0xFF cache zone (gen0 or gen1).
//
// Violations of invariant 1 indicate a restore bug. Violations of invariant 2
// indicate a cache gap — the key exists in Pebble but is not tracked by the
// cache. A subsequent CacheGuaranteed from the leader would assume the key is
// cached, but the follower doesn't have it, leading to volume divergence.
//
// Called only in sentinel mode. Logs violations via lifecycle.SendEvent and
// returns the total number of violations.
func (s *CacheSnapshotter) verifyCacheRestoreCoherence() int {
	currentGen := s.registry.Cache.CurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	var totalViolations int

	// ─── Invariant 1: 0xFF → memory (every Pebble key must be in memory) ───

	for _, slot := range s.slots {
		totalViolations += verifyPebbleKeysInMemory(s, gen0Byte, slot, 0, currentGen)
		totalViolations += verifyPebbleKeysInMemory(s, gen1Byte, slot, 1, currentGen)
	}

	// ─── Invariant 2: 0xF1 ↔ 0xFF ───

	attrTypes := []struct {
		name     string
		attrCode byte
		slot     cacheSnapshotSlot
	}{
		{"volumes", dal.SubAttrVolume, s.slots[0]},
		{"metadata", dal.SubAttrMetadata, s.slots[1]},
		{"ledgers", dal.SubAttrLedger, s.slots[2]},
		{"boundaries", dal.SubAttrBoundary, s.slots[3]},
		{"references", dal.SubAttrReference, s.slots[4]},
		{"transactions", dal.SubAttrTransaction, s.slots[5]},
		{"sinkConfigs", dal.SubAttrSinkConfig, s.slots[6]},
		{"numscriptVersions", dal.SubAttrNumscriptVersion, s.slots[7]},
		{"numscriptContents", dal.SubAttrNumscriptContent, s.slots[8]},
		{"preparedQueries", dal.SubAttrPreparedQuery, s.slots[9]},
		{"ledgerMetadata", dal.SubAttrLedgerMetadata, s.slots[10]},
	}

	hasher := attributes.NewKeyHasher(attributes.DefaultSeeds)

	for _, at := range attrTypes {
		missing := findAttributesNotInCache(s.dataStore, at.attrCode, gen0Byte, gen1Byte, hasher)

		if len(missing) > 0 {
			totalViolations += len(missing)
			s.logger.Errorf("SENTINEL restore check: %d %s keys in 0xF1 but NOT in 0xFF cache",
				len(missing), at.name)

			for i, key := range missing {
				if i >= 5 {
					s.logger.Errorf("  ... and %d more", len(missing)-5)

					break
				}

				s.logger.Errorf("  missing: %x", key)
			}

			lifecycle.SendEvent("restore_verify_attr_not_in_cache", map[string]any{
				"type":       at.name,
				"cacheType":  at.attrCode,
				"count":      len(missing),
				"generation": currentGen,
			})
		}
	}

	if totalViolations == 0 {
		s.logger.Infof("SENTINEL restore check: cache coherence verified (0 violations)")
	} else {
		s.logger.Errorf("SENTINEL restore check: %d violations detected", totalViolations)
	}

	lifecycle.SendEvent("restore_verify_complete", map[string]any{
		"violations": totalViolations,
		"generation": currentGen,
	})

	return totalViolations
}

// verifyPebbleKeysInMemory checks that every key in 0xFF for a given gen/type
// is also present in the corresponding in-memory generation. Returns violation count.
func verifyPebbleKeysInMemory(s *CacheSnapshotter, genByte byte, slot cacheSnapshotSlot, genIndex int, currentGen uint64) int {
	lower := []byte{dal.ZoneCache, genByte, slot.CacheType()}
	upper := []byte{dal.ZoneCache, genByte, slot.CacheType() + 1}

	iter, err := s.dataStore.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0
	}

	defer func() { _ = iter.Close() }()

	// Collect all in-memory keys for this gen into a set for O(1) lookup.
	memKeys := make(map[attributes.U128]struct{})
	for u128 := range slot.IterKeys(genIndex) {
		memKeys[u128] = struct{}{}
	}

	var pebbleCount int
	var violations int

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 3+16 {
			continue
		}

		pebbleCount++
		u128 := attributes.U128FromBytes(key[3:19])

		if _, ok := memKeys[u128]; !ok {
			violations++
			s.logger.Errorf("SENTINEL restore check: key %x in 0xFF gen%d (byte=%d, type=%02x) but NOT in memory",
				u128, genIndex, genByte, slot.CacheType())

			if violations <= 3 {
				lifecycle.SendEvent("restore_verify_key_missing_from_memory", map[string]any{
					"id":         fmt.Sprintf("%x", u128),
					"cacheType":  slot.CacheType(),
					"gen":        genIndex,
					"generation": currentGen,
				})
			}
		}

		delete(memKeys, u128)
	}

	// Also check reverse: keys in memory but NOT in Pebble
	for u128 := range memKeys {
		violations++
		s.logger.Errorf("SENTINEL restore check: key %x in memory gen%d but NOT in 0xFF (byte=%d, type=%02x)",
			u128, genIndex, genByte, slot.CacheType())

		if violations <= 6 {
			lifecycle.SendEvent("restore_verify_key_missing_from_pebble", map[string]any{
				"id":         fmt.Sprintf("%x", u128),
				"cacheType":  slot.CacheType(),
				"gen":        genIndex,
				"generation": currentGen,
			})
		}
	}

	if violations > 0 {
		lifecycle.SendEvent("restore_verify_gen_mismatch", map[string]any{
			"cacheType":  slot.CacheType(),
			"gen":        genIndex,
			"pebble":     pebbleCount,
			"memory":     pebbleCount - violations + len(memKeys),
			"violations": violations,
			"generation": currentGen,
		})
	}

	return violations
}

// findAttributesNotInCache finds keys present in 0xF1 but missing from both
// 0xFF gen0 and gen1. Returns the canonical keys of missing entries.
func findAttributesNotInCache(store *dal.Store, attrType, gen0Byte, gen1Byte byte, hasher *attributes.KeyHasher) [][]byte {
	lower := []byte{dal.ZoneAttributes, attrType}
	upper := []byte{dal.ZoneAttributes, attrType + 1}

	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil
	}

	defer func() { _ = iter.Close() }()

	var missing [][]byte

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 {
			continue
		}

		canonicalKey := key[2:]
		u128, _ := hasher.MakeKey(canonicalKey)

		if !pebbleHasCacheEntry(store, gen0Byte, attrType, u128) &&
			!pebbleHasCacheEntry(store, gen1Byte, attrType, u128) {
			missing = append(missing, append([]byte(nil), canonicalKey...))
		}
	}

	return missing
}

// pebbleHasCacheEntry checks if a U128 key exists in the 0xFF cache zone.
func pebbleHasCacheEntry(store *dal.Store, genByte, cacheType byte, u128 attributes.U128) bool {
	var key [3 + 16]byte
	key[0] = dal.ZoneCache
	key[1] = genByte
	key[2] = cacheType
	copy(key[3:], u128[:])

	_, closer, err := store.Get(key[:])
	if err != nil {
		return false
	}

	_ = closer.Close()

	return true
}

func (s *CacheSnapshotter) verifyCacheRestoreCoherenceFormatted() string {
	violations := s.verifyCacheRestoreCoherence()
	if violations == 0 {
		return ""
	}

	return fmt.Sprintf("%d cache restore violations detected", violations)
}
