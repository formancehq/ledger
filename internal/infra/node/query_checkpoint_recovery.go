package node

import (
	"fmt"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// recoverQueryCheckpointMainStores rebuilds the main store of every registered
// query checkpoint this replica has not marked ready, provided the live store
// still sits at the checkpoint's applied index. The applier is gated for the
// whole materialization and the spool is reset on restart, so a process that
// dies anywhere inside it reopens with the live store exactly at that index —
// the state the checkpoint was meant to freeze. The caller runs this before
// WAL replay moves the store on.
//
// A checkpoint whose applied index differs was never applied here (the replica
// joined through a later snapshot) and a restored checkpoint carries a
// source-cluster index; both stay unavailable on this replica.
func recoverQueryCheckpointMainStores(store *dal.Store, logger logging.Logger, liveAppliedIndex uint64) error {
	handle, err := store.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("opening read handle: %w", err)
	}

	checkpoints, err := query.ListQueryCheckpoints(handle)
	// The handle holds the store's read lock; release it before materializing.
	_ = handle.Close()
	if err != nil {
		return err
	}

	for _, cp := range checkpoints {
		id := cp.GetCheckpointId()
		if dal.CheckpointDirReady(store.QueryCheckpointMainDir(id)) {
			continue
		}

		fields := map[string]any{
			"checkpointID":     id,
			"checkpointIndex":  cp.GetAppliedIndex(),
			"liveAppliedIndex": liveAppliedIndex,
		}

		switch {
		case cp.GetRestoredFromBackup():
			logger.WithFields(fields).Infof("Query checkpoint main store was not restored with the backup; it stays unavailable on this replica")
		case cp.GetAppliedIndex() != liveAppliedIndex:
			logger.WithFields(fields).Infof("Query checkpoint main store was never materialized on this replica; it stays unavailable here")
		default:
			if _, err := store.CreateQueryCheckpoint(id); err != nil {
				return fmt.Errorf("rebuilding main store of query checkpoint %d: %w", id, err)
			}

			logger.WithFields(fields).Infof("Rebuilt query checkpoint main store left unmaterialized by a previous run")
		}
	}

	return nil
}
