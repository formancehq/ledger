package dal

import (
	"errors"
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
	// The worker exits once its queue drains, so a closed store retains no
	// goroutine and nothing waits on a timer.
	var queueMu sync.Mutex
	var eventQueue chan pebbleLogEvent
	var dropped uint64
	logEvents := func(queue chan pebbleLogEvent) {
		reportDrops := func() {
			queueMu.Lock()
			count := uint64(0)
			if len(queue) == 0 {
				count, dropped = dropped, 0
			}
			queueMu.Unlock()
			if count > 0 {
				logger.WithFields(map[string]any{"droppedEvents": count}).Errorf("Pebble event logs dropped")
			}
		}
		// enqueue starts the worker after queueing an event and only drops
		// events while the queue is full, so the receive never waits.
		for {
			event := <-queue
			withFields := logger.WithFields(event.fields)
			switch event.level {
			case "debug":
				withFields.Debugf("%s", event.message)
			case "info":
				withFields.Infof("%s", event.message)
			case "error":
				withFields.Errorf("%s", event.message)
			}
			reportDrops()
			queueMu.Lock()
			if len(queue) == 0 {
				// The next enqueue starts a new worker on a new queue.
				eventQueue = nil
				queueMu.Unlock()

				return
			}
			queueMu.Unlock()
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
			dropped++
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
			} else if !logger.Enabled(logging.DebugLevel) {
				return
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
			// not report expected cancellations there, so close their start log.
			if info.Err != nil {
				if errors.Is(info.Err, pebble.ErrCancelledCompaction) {
					inBytes := inputBytes(info.Input)
					level := "debug"
					if inBytes >= 1<<30 {
						level = "info"
					} else if !logger.Enabled(logging.DebugLevel) {
						return
					}
					enqueue(pebbleLogEvent{level: level, message: "Pebble compaction cancelled", fields: map[string]any{
						"jobID":          info.JobID,
						"reason":         info.Reason,
						"inputBytes":     inBytes,
						"inputSizeHuman": humanize.IBytes(inBytes),
					}})
				}

				return
			}
			inBytes := inputBytes(info.Input)
			level := "debug"
			if info.TotalDuration >= 10*time.Second || inBytes >= 1<<30 {
				level = "info"
			} else if !logger.Enabled(logging.DebugLevel) {
				return
			}
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
			}
			enqueue(pebbleLogEvent{level: level, message: "Pebble compaction completed", fields: fields})
		},
		FlushBegin: func(info pebble.FlushInfo) {
			if !logger.Enabled(logging.DebugLevel) {
				return
			}
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
			if info.TotalDuration < 10*time.Second && !logger.Enabled(logging.DebugLevel) {
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
