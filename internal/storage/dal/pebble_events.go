package dal

import (
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// NewPebbleEventLogger reports background work that can delay recovery or
// consume disk bandwidth. The store name distinguishes independent Pebble DBs.
func NewPebbleEventLogger(logger logging.Logger, store string) *pebble.EventListener {
	logger = logger.WithFields(map[string]any{"pebbleStore": store})
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
			logger.WithFields(map[string]any{
				"jobID":      info.JobID,
				"reason":     info.Reason,
				"inputBytes": inputBytes(info.Input),
			}).Infof("Pebble compaction started")
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			fields := map[string]any{
				"jobID":         info.JobID,
				"reason":        info.Reason,
				"duration":      info.Duration.String(),
				"totalDuration": info.TotalDuration.String(),
				"inputBytes":    inputBytes(info.Input),
				"outputBytes":   tableBytes(info.Output.Tables),
				"outputLevel":   info.Output.Level,
				"error":         info.Err,
			}
			if info.Err != nil {
				logger.WithFields(fields).Errorf("Pebble compaction failed")
			} else {
				logger.WithFields(fields).Infof("Pebble compaction completed")
			}
		},
		FlushBegin: func(info pebble.FlushInfo) {
			logger.WithFields(map[string]any{
				"jobID":      info.JobID,
				"reason":     info.Reason,
				"inputBytes": info.InputBytes,
			}).Debugf("Pebble flush started")
		},
		FlushEnd: func(info pebble.FlushInfo) {
			fields := map[string]any{
				"jobID":         info.JobID,
				"reason":        info.Reason,
				"inputBytes":    info.InputBytes,
				"duration":      info.Duration.String(),
				"totalDuration": info.TotalDuration.String(),
				"error":         info.Err,
			}
			switch {
			case info.Err != nil:
				logger.WithFields(fields).Errorf("Pebble flush failed")
			case info.TotalDuration >= 10*time.Second:
				logger.WithFields(fields).Infof("Pebble slow flush completed")
			default:
				logger.WithFields(fields).Debugf("Pebble flush completed")
			}
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			logger.WithFields(map[string]any{
				"operation": info.OpType.String(),
				"duration":  info.Duration.String(),
			}).Infof("Pebble disk operation slow")
		},
		BackgroundError: func(err error) {
			logger.WithFields(map[string]any{"error": err}).Errorf("Pebble background error")
		},
	}
}
