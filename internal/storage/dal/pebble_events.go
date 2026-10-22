package dal

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/dustin/go-humanize"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type pebbleLogEvent struct {
	level   string
	message string
	fields  map[string]any
}

// NewPebbleEventLogger reports background work that can delay recovery or
// consume disk bandwidth. The store name distinguishes independent Pebble DBs.
func NewPebbleEventLogger(logger logging.Logger, store string) *pebble.EventListener {
	logger = logger.WithFields(map[string]any{"pebbleStore": store})
	// Some callbacks run with Pebble's DB mutex held; DiskSlow runs on its
	// disk-health goroutine. Never wait for a logging sink in either path.
	// An idle worker exits so reopened stores do not retain goroutines.
	var queueMu sync.Mutex
	var eventQueue chan pebbleLogEvent
	logEvents := func(queue chan pebbleLogEvent) {
		idle := time.NewTimer(time.Minute)
		defer idle.Stop()
		for {
			select {
			case event := <-queue:
				withFields := logger.WithFields(event.fields)
				switch event.level {
				case "debug":
					withFields.Debugf("%s", event.message)
				case "info":
					withFields.Infof("%s", event.message)
				case "error":
					withFields.Errorf("%s", event.message)
				}
				idle.Reset(time.Minute)
			case <-idle.C:
				queueMu.Lock()
				if len(queue) == 0 {
					eventQueue = nil
					queueMu.Unlock()

					return
				}
				queueMu.Unlock()
				idle.Reset(time.Minute)
			}
		}
	}
	enqueue := func(event pebbleLogEvent) {
		queueMu.Lock()
		if eventQueue == nil {
			eventQueue = make(chan pebbleLogEvent, 32)
			go logEvents(eventQueue)
		}
		select {
		case eventQueue <- event:
		default: // The logging sink is blocked; never block Pebble.
		}
		queueMu.Unlock()
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
			level := "debug"
			if bytes >= 1<<30 {
				level = "info"
			}
			enqueue(pebbleLogEvent{level: level, message: "Pebble compaction started", fields: map[string]any{
				"jobID":          info.JobID,
				"reason":         info.Reason,
				"inputBytes":     bytes,
				"inputSizeHuman": humanize.IBytes(bytes),
			}})
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			// Real failures are reported through BackgroundError. Pebble does
			// not report expected cancelled compactions there.
			if info.Err != nil {
				return
			}
			inBytes := inputBytes(info.Input)
			outBytes := tableBytes(info.Output.Tables)
			level := "debug"
			if info.TotalDuration >= 10*time.Second {
				level = "info"
			}
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
			}
			enqueue(pebbleLogEvent{level: level, message: "Pebble compaction completed", fields: fields})
		},
		FlushBegin: func(info pebble.FlushInfo) {
			enqueue(pebbleLogEvent{level: "debug", message: "Pebble flush started", fields: map[string]any{
				"jobID":          info.JobID,
				"reason":         info.Reason,
				"inputBytes":     info.InputBytes,
				"inputSizeHuman": humanize.IBytes(info.InputBytes),
			}})
		},
		FlushEnd: func(info pebble.FlushInfo) {
			if info.Err != nil && info.Err.Error() == "pebble: empty table" {
				// Pebble uses an unexported sentinel for a successful no-op flush.
				info.Err = nil
			}
			// Pebble reports real flush failures through BackgroundError too.
			if info.Err != nil {
				return
			}
			fields := map[string]any{
				"jobID":          info.JobID,
				"reason":         info.Reason,
				"inputBytes":     info.InputBytes,
				"inputSizeHuman": humanize.IBytes(info.InputBytes),
				"duration":       info.Duration.String(),
				"totalDuration":  info.TotalDuration.String(),
			}
			switch {
			case info.TotalDuration >= 10*time.Second:
				enqueue(pebbleLogEvent{level: "info", message: "Pebble slow flush completed", fields: fields})
			default:
				enqueue(pebbleLogEvent{level: "debug", message: "Pebble flush completed", fields: fields})
			}
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			enqueue(pebbleLogEvent{level: "info", message: "Pebble disk operation slow", fields: map[string]any{
				"operation": info.OpType.String(),
				"duration":  info.Duration.String(),
			}})
		},
		BackgroundError: func(err error) {
			enqueue(pebbleLogEvent{level: "error", message: "Pebble background error", fields: map[string]any{"error": err.Error()}})
		},
	}
}
