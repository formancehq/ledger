package state

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// wasmModuleMetadataKey and wasmModuleChunkPrefix deliberately live in the
// cluster-persistent zone: module bytes and configuration must survive process
// restarts and in-cluster snapshots, while cross-cluster restore policy can
// discard them together with other node-local cluster state if required.
func wasmModuleMetadataKey(moduleID string) []byte {
	return append([]byte{dal.ZoneClusterPersistent, dal.SubGlobWasmModule, 'm'}, moduleID...)
}

func wasmModuleChunkPrefix(moduleID string) []byte {
	key := append([]byte{dal.ZoneClusterPersistent, dal.SubGlobWasmModule, 'c'}, moduleID...)
	return append(key, 0)
}

func wasmModuleChunkKey(moduleID string, index uint64) []byte {
	key := wasmModuleChunkPrefix(moduleID)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], index)

	return append(key, encoded[:]...)
}

func applyWasmModuleUpdate(batch *dal.WriteSession, update *raftcmdpb.WasmModuleUpdate) error {
	if update.GetModuleId() == "" {
		return fmt.Errorf("module id is required")
	}

	metadataKey := wasmModuleMetadataKey(update.GetModuleId())

	switch update.GetOperation() {
	case raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_UPLOAD_CHUNK:
		if err := batch.SetProto(metadataKey, update); err != nil {
			return fmt.Errorf("persisting wasm module metadata: %w", err)
		}
		if err := batch.SetBytes(wasmModuleChunkKey(update.GetModuleId(), update.GetChunkIndex()), update.GetChunk()); err != nil {
			return fmt.Errorf("persisting wasm module chunk: %w", err)
		}
	case raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_SET_STATUS:
		if err := batch.SetProto(metadataKey, update); err != nil {
			return fmt.Errorf("persisting wasm module status: %w", err)
		}
	case raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_DELETE:
		if err := batch.DeleteKey(metadataKey); err != nil {
			return fmt.Errorf("deleting wasm module metadata: %w", err)
		}
		prefix := wasmModuleChunkPrefix(update.GetModuleId())
		end := append(append([]byte(nil), prefix[:len(prefix)-1]...), 1)
		if err := batch.DeleteRangeNoSync(prefix, end); err != nil {
			return fmt.Errorf("deleting wasm module chunks: %w", err)
		}
	default:
		return fmt.Errorf("unsupported wasm module operation %s", update.GetOperation().String())
	}

	return nil
}
