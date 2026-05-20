package processing

import "github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"

// logByteEntry tracks the position of one log's pre-marshaled bytes inside the arena.
type logByteEntry struct {
	orderIdx int
	start    int
}

// logBytesArena is a reusable arena for pre-marshaled log bytes.
// It accumulates bytes for all logs in a batch into a single contiguous buffer,
// eliminating per-log heap allocations after warmup.
type logBytesArena struct {
	buf     []byte
	entries []logByteEntry
}

func newLogBytesArena() logBytesArena {
	return logBytesArena{
		buf:     make([]byte, 0, 4096),
		entries: make([]logByteEntry, 0, 16),
	}
}

// Reset prepares the arena for a new batch.
func (a *logBytesArena) Reset() {
	a.buf = a.buf[:0]
	a.entries = a.entries[:0]
}

// Append adds pre-marshaled log bytes to the arena. base contains the
// deterministic marshal bytes; the remaining fields (hash, hashVersion,
// signature) are appended from log via AppendLogFieldsForPersist.
func (a *logBytesArena) Append(orderIdx int, base []byte, log *commonpb.Log) {
	start := len(a.buf)
	a.buf = append(a.buf, base...)
	a.buf = AppendLogFieldsForPersist(a.buf, log)
	a.entries = append(a.entries, logByteEntry{orderIdx: orderIdx, start: start})
}

// AssignSlices creates sub-slices from the stable arena into dst.
// Must be called after all Append calls (the backing array is stable).
// Each sub-slice has its cap set to prevent cross-entry corruption.
func (a *logBytesArena) AssignSlices(dst [][]byte) {
	for j, entry := range a.entries {
		end := len(a.buf)
		if j+1 < len(a.entries) {
			end = a.entries[j+1].start
		}

		dst[entry.orderIdx] = a.buf[entry.start:end:end]
	}
}
