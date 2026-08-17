package readstore

import (
	"errors"

	"fmt"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"os"
	"sync"
)

// traceKey enables MIDXTRACE event logging for one metadata key ("*" = all;
// debug only). Lines go to MIDXTRACE_FILE (default /tmp/midxtrace.log) —
// a file, not the process logger, which the structured-logging stack owns.
var traceKey = os.Getenv("MIDXTRACE")

var traceFile = func() *os.File {
	if traceKey == "" {
		return nil
	}

	path := os.Getenv("MIDXTRACE_FILE")
	if path == "" {
		path = "/tmp/midxtrace.log"
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil
	}

	return f
}()

var traceMu sync.Mutex

func tracef(format string, args ...any) {
	if traceFile == nil {
		return
	}

	traceMu.Lock()
	fmt.Fprintf(traceFile, format+"\n", args...)
	traceMu.Unlock()
}

// Write side of the event-suffixed metadata index (see event_keys.go). The
// builder folds one log at a time and declares its raft sequence via
// SetEventSequence; every index mutation becomes one or two APPENDS, never a
// delete:
//
//	set   k=v   (fresh)   → ADD in v's range
//	set   k=v2  (was v1)  → ADD in v2's range + DEL in v1's range
//	unset k     (was v1)  → DEL in v1's range
//
// The exists index (eidx) gets the same treatment keyed on its nullFlag: an
// event is emitted only when the flag transitions (fresh set, null↔non-null
// move, unset) — a value change within the same flag leaves the standing ADD
// as the group's verdict at every pin.
//
// The old value still comes from the reverse map, exactly like today's
// path — the rmap keeps its role and its prompt deletion semantics (the
// checker's reverse-map pass is untouched).

// eventSequence returns the declared raft sequence for sequence-stamped
// writes — metadata events and the single-stamp rows (account-by-asset,
// reverted_at) — failing loudly when unset: stamping a zero or stale
// sequence would silently corrupt pinned resolution for every future reader.
func (wb *WriteBatch) eventSequence() (uint64, error) {
	if wb.eventSeq == 0 {
		return 0, errors.New("invariant: sequence-stamped index write without SetEventSequence")
	}

	return wb.eventSeq, nil
}

func (wb *WriteBatch) appendMetadataIndexEvent(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metadataKey string,
	version uint32,
	encodedValue []byte,
	entityID []byte,
	op byte,
) error {
	seq, err := wb.eventSequence()
	if err != nil {
		return err
	}

	if traceKey != "" && (traceKey == "*" || metadataKey == traceKey) {
		tracef("MIDXTRACE midx ledger=%s ns=%s v=%d value=%x entity=%s seq=%d op=%d", ledgerName, ns, version, encodedValue, entityID, seq, op)
	}

	return wb.put(MetadataIndexEventKeyV(kb, ledgerName, ns, metadataKey, version, encodedValue, entityID, seq, op), nil)
}

func (wb *WriteBatch) appendEntityExistsEvent(
	kb *dal.KeyBuilder,
	ledgerName string,
	ns, metaKey string,
	version uint32,
	isNull bool,
	entityID []byte,
	op byte,
) error {
	seq, err := wb.eventSequence()
	if err != nil {
		return err
	}

	if traceKey != "" && (traceKey == "*" || metaKey == traceKey) {
		tracef("MIDXTRACE eidx ledger=%s ns=%s v=%d null=%v entity=%s seq=%d op=%d", ledgerName, ns, version, isNull, entityID, seq, op)
	}

	return wb.put(EntityExistsEventKeyV(kb, ledgerName, ns, metaKey, version, isNull, entityID, seq, op), nil)
}
