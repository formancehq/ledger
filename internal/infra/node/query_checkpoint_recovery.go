package node

import (
	"fmt"
	"time"

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
	// Close only releases the store's read lock the materialization below
	// needs; the rows are already in hand, so its error changes nothing here.
	_ = handle.Close()
	if err != nil {
		return err
	}
	readyCount := 0
	unavailableCount := 0
	rebuiltCount := 0

	for _, cp := range checkpoints {
		id := cp.GetCheckpointId()
		if dal.CheckpointDirReady(store.QueryCheckpointMainDir(id)) {
			readyCount++

			continue
		}

		fields := map[string]any{
			"checkpointID":     id,
			"checkpointIndex":  cp.GetAppliedIndex(),
			"liveAppliedIndex": liveAppliedIndex,
		}

		switch {
		case cp.GetRestoredFromBackup():
			unavailableCount++
			logger.WithFields(fields).Infof("Query checkpoint main store was not restored with the backup; it stays unavailable on this replica")
		case cp.GetAppliedIndex() != liveAppliedIndex:
			unavailableCount++
			logger.WithFields(fields).Infof("Query checkpoint main store was never materialized on this replica; it stays unavailable here")
		default:
			start := time.Now()
			logger.WithFields(fields).Infof("Rebuilding interrupted query checkpoint main store")
			if _, err := store.CreateQueryCheckpoint(id); err != nil {
				return fmt.Errorf("rebuilding main store of query checkpoint %d: %w", id, err)
			}
			rebuiltCount++

			fields["duration"] = time.Since(start).String()
			logger.WithFields(fields).Infof("Rebuilt query checkpoint main store left unmaterialized by a previous run")
		}
	}
	logger.WithFields(map[string]any{
		"registered":  len(checkpoints),
		"ready":       readyCount,
		"unavailable": unavailableCount,
		"rebuilt":     rebuiltCount,
	}).Infof("Query checkpoint startup recovery scan completed")

	return nil
}
