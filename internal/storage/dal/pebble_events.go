package dal

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/dustin/go-humanize"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// NewPebbleEventLogger reports background work that can delay recovery or
// consume disk bandwidth. The store name distinguishes independent Pebble DBs.
func NewPebbleEventLogger(logger logging.Logger, store string) *pebble.EventListener {
	logger = logger.WithFields(map[string]any{"pebbleStore": store})
	// DiskSlow runs on Pebble's disk-health goroutine. Keep its handoff bounded
	// and let an idle worker exit so reopened stores do not retain goroutines.
	var diskSlowMu sync.Mutex
	var diskSlowQueue chan pebble.DiskSlowInfo
	logDiskSlow := func(queue chan pebble.DiskSlowInfo) {
		idle := time.NewTimer(time.Minute)
		defer idle.Stop()
		for {
			select {
			case info := <-queue:
				logger.WithFields(map[string]any{
					"operation": info.OpType.String(),
					"duration":  info.Duration.String(),
				}).Infof("Pebble disk operation slow")
				idle.Reset(time.Minute)
			case <-idle.C:
				diskSlowMu.Lock()
				if len(queue) == 0 {
					diskSlowQueue = nil
					diskSlowMu.Unlock()

					return
				}
				diskSlowMu.Unlock()
				idle.Reset(time.Minute)
			}
		}
	}
	tableBytes := func(tables []pebble.TableInfo) uint64 {
		var total uint64
		for _, table := range tables {
			total += table.Size
		}

		return total
	}
	inputBytes := func(levels []pebble.LevelInfo) uint64 {
		var total uint64
		for _, level := range levels {
			total += tableBytes(level.Tables)
		}

		return total
	}

	return &pebble.EventListener{
		CompactionBegin: func(info pebble.CompactionInfo) {
			bytes := inputBytes(info.Input)
			logger.WithFields(map[string]any{
				"jobID":          info.JobID,
				"reason":         info.Reason,
				"inputBytes":     bytes,
				"inputSizeHuman": humanize.IBytes(bytes),
			}).Infof("Pebble compaction started")
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			inBytes := inputBytes(info.Input)
			outBytes := tableBytes(info.Output.Tables)
			fields := map[string]any{
				"jobID":           info.JobID,
				"reason":          info.Reason,
				"duration":        info.Duration.String(),
				"totalDuration":   info.TotalDuration.String(),
				"inputBytes":      inBytes,
				"inputSizeHuman":  humanize.IBytes(inBytes),
				"outputBytes":     outBytes,
				"outputSizeHuman": humanize.IBytes(outBytes),
				"outputLevel":     info.Output.Level,
				"error":           info.Err,
			}
			if info.Err != nil {
				logger.WithFields(fields).Errorf("Pebble compaction failed")
			} else {
				logger.WithFields(fields).Infof("Pebble compaction completed")
			}
		},
		FlushBegin: func(info pebble.FlushInfo) {
			logger.WithFields(map[string]any{
				"jobID":          info.JobID,
				"reason":         info.Reason,
				"inputBytes":     info.InputBytes,
				"inputSizeHuman": humanize.IBytes(info.InputBytes),
			}).Debugf("Pebble flush started")
		},
		FlushEnd: func(info pebble.FlushInfo) {
			fields := map[string]any{
				"jobID":          info.JobID,
				"reason":         info.Reason,
				"inputBytes":     info.InputBytes,
				"inputSizeHuman": humanize.IBytes(info.InputBytes),
				"duration":       info.Duration.String(),
				"totalDuration":  info.TotalDuration.String(),
			}
			switch {
			case info.Err != nil && info.Err.Error() == "pebble: empty table":
				// Pebble reports a successful flush with no output table using an
				// unexported sentinel error. It is not a background failure.
				logger.WithFields(fields).Infof("Pebble empty flush completed")
			case info.Err != nil:
				fields["error"] = info.Err
				logger.WithFields(fields).Errorf("Pebble flush failed")
			case info.TotalDuration >= 10*time.Second:
				logger.WithFields(fields).Infof("Pebble slow flush completed")
			default:
				logger.WithFields(fields).Debugf("Pebble flush completed")
			}
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			diskSlowMu.Lock()
			if diskSlowQueue == nil {
				diskSlowQueue = make(chan pebble.DiskSlowInfo, 32)
				go logDiskSlow(diskSlowQueue)
			}
			select {
			case diskSlowQueue <- info:
			default: // The logging sink is blocked; never block disk monitoring.
			}
			diskSlowMu.Unlock()
		},
		BackgroundError: func(err error) {
			logger.WithFields(map[string]any{"error": err}).Errorf("Pebble background error")
		},
	}
}
