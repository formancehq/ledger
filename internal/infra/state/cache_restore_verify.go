package state

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

// verifyCacheRestoreCoherence emits a diagnostic snapshot of what RestoreFromStore
// loaded into memory. This is NOT a correctness assertion — since RestoreFromStore
// populates memory from 0xFF, comparing the two is a tautology. Instead, this
// emits per-type/per-gen counts so that Antithesis logs can be compared across
// nodes to spot divergence after a restart.
func (s *CacheSnapshotter) verifyCacheRestoreCoherence() int {
	currentGen := s.registry.Cache.CurrentGeneration()

	details := map[string]any{
		"generation": currentGen,
		"gen0Base":   s.registry.Cache.BaseIndex.Gen0,
		"gen1Base":   s.registry.Cache.BaseIndex.Gen1,
	}

	for _, slot := range s.slots {
		gen0Count := uint64(0)
		for range slot.IterKeys(0) {
			gen0Count++
		}

		gen1Count := uint64(0)
		for range slot.IterKeys(1) {
			gen1Count++
		}

		details[fmt.Sprintf("type_%02x_gen0", slot.CacheType())] = gen0Count
		details[fmt.Sprintf("type_%02x_gen1", slot.CacheType())] = gen1Count
	}

	s.logger.WithFields(details).Infof("SENTINEL restore snapshot")
	lifecycle.SendEvent("restore_verify_complete", details)

	return 0
}
