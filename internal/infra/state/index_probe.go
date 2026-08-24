package state

import (
	"bytes"
	"encoding/hex"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// indexProbe returns a diagnostic closure that reports, for one index
// registry key, everything the apply-side read consulted: the entry
// state in each cache generation, the batch-local overlay, what the
// proposal's execution plan carried for the key, and the cache geometry
// at probe time. Installed on the RequestProcessor per proposal;
// handlers invoke it when a registry lookup answers absent. Each
// invocation also logs the captured state so the per-key story is
// readable off this node's log alone.
func (fsm *Machine) indexProbe(plan *raftcmdpb.ExecutionPlan, ws *WriteSet) func(key domain.IndexKey) map[string]any {
	return func(key domain.IndexKey) map[string]any {
		id, tag := attributes.MakeKey(key.Bytes())

		overlayValue, overlayDeleted := ws.Derived.Indexes.DebugOverlay(key)

		planState := "missing"

		for _, cov := range plan.GetAttributes() {
			if byte(cov.GetAttrCode()) != dal.SubAttrIndex || !bytes.Equal(cov.GetId().GetId(), id[:]) {
				continue
			}

			if cov.GetValue() != nil {
				planState = "seed"
			} else {
				planState = "declare"
			}

			if cov.GetId().GetTag() != tag {
				planState += "/tag-mismatch"
			}

			break
		}

		out := map[string]any{
			"ledger":         key.LedgerName,
			"canonical":      key.Canonical,
			"u128":           hex.EncodeToString(id[:]),
			"tagSought":      tag,
			"gen0":           describeIndexEntry(fsm.Registry.Cache.Indexes.Gen0(), id, tag),
			"gen1":           describeIndexEntry(fsm.Registry.Cache.Indexes.Gen1(), id, tag),
			"overlayValue":   overlayValue,
			"overlayDeleted": overlayDeleted,
			"plan":           planState,
			"currentGen":     fsm.Registry.Cache.CurrentGeneration(),
			"gen0Base":       fsm.Registry.Cache.BaseIndex.Gen0,
			"gen1Base":       fsm.Registry.Cache.BaseIndex.Gen1,
			"lastApplied":    fsm.State.LastAppliedIndex,
		}

		fsm.logger.WithFields(out).Infof("Index registry apply probe")

		return out
	}
}

// describeIndexEntry summarizes one generation's raw entry for a key:
// presence, tag match, tombstone flag and whether the payload is a
// typed nil (the state rawAccessor.Get normalizes to ErrNotFound).
func describeIndexEntry(gen *kv.ShardedMap[attributes.U128, attributes.Entry[*commonpb.Index]], id attributes.U128, tag uint64) map[string]any {
	entry, ok := gen.Get(id)
	if !ok {
		return map[string]any{"present": false}
	}

	return map[string]any{
		"present":  true,
		"tagMatch": entry.Tag == tag,
		"deleted":  entry.Deleted,
		"nilData":  entry.Data == nil,
	}
}
