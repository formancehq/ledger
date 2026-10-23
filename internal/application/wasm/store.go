package wasm

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func metadataPrefix() []byte {
	return []byte{dal.ZoneClusterPersistent, dal.SubGlobWasmModule, 'm'}
}

func chunkPrefix(moduleID string) []byte {
	key := append([]byte{dal.ZoneClusterPersistent, dal.SubGlobWasmModule, 'c'}, moduleID...)
	return append(key, 0)
}

// LoadModules reconstructs module records from the same Pebble store that the
// FSM writes. It is called after leader readiness, never from the FSM apply
// path.
func LoadModules(store *dal.Store) ([]*Module, error) {
	handle, err := store.NewDirectReadHandle()
	if err != nil {
		return nil, fmt.Errorf("open wasm store reader: %w", err)
	}
	defer handle.Close()

	prefix := metadataPrefix()
	end := append(append([]byte(nil), prefix[:len(prefix)-1]...), prefix[len(prefix)-1]+1)
	iter, err := handle.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: end})
	if err != nil {
		return nil, fmt.Errorf("iterate wasm metadata: %w", err)
	}
	defer iter.Close()

	var modules []*Module
	for iter.First(); iter.Valid(); iter.Next() {
		var update raftcmdpb.WasmModuleUpdate
		if err := update.UnmarshalVT(iter.Value()); err != nil {
			return nil, fmt.Errorf("decode wasm module metadata: %w", err)
		}
		module := &Module{
			ID:        update.GetModuleId(),
			Config:    append([]byte(nil), update.GetConfig()...),
			Status:    update.GetStatus(),
			LastError: update.GetError(),
			Size:      update.GetSize(),
		}
		module.Wasm, err = loadChunks(handle, module.ID)
		if err != nil {
			return nil, err
		}
		modules = append(modules, module)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate wasm metadata: %w", err)
	}

	return modules, nil
}

func loadChunks(reader dal.PebbleReader, moduleID string) ([]byte, error) {
	prefix := chunkPrefix(moduleID)
	end := append(append([]byte(nil), prefix[:len(prefix)-1]...), byte(1))
	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: end})
	if err != nil {
		return nil, fmt.Errorf("iterate wasm chunks for %s: %w", moduleID, err)
	}
	defer iter.Close()

	var wasm []byte
	for iter.First(); iter.Valid(); iter.Next() {
		wasm = append(wasm, iter.Value()...)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate wasm chunks for %s: %w", moduleID, err)
	}
	return wasm, nil
}
