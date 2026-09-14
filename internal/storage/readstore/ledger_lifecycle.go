package readstore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const ledgerLifecycleValueSize = 5

// LedgerLifecycle identifies the ledger incarnation represented by a read
// projection. Active is false after DeleteLedger; the tombstone remains until
// a same-name CreateLedger replaces it with the new numeric ID.
type LedgerLifecycle struct {
	ID     uint32
	Active bool
}

// ReadLedgerLifecycle reads one ledger's lifecycle through the caller's fixed
// projection snapshot. The boolean is false when no lifecycle was projected.
func ReadLedgerLifecycle(reader dal.PebbleGetter, kb *dal.KeyBuilder, ledgerName string) (LedgerLifecycle, bool, error) {
	value, closer, err := reader.Get(LedgerLifecycleKey(kb, ledgerName))
	if errors.Is(err, pebble.ErrNotFound) {
		return LedgerLifecycle{}, false, nil
	}
	if err != nil {
		return LedgerLifecycle{}, false, fmt.Errorf("reading ledger lifecycle for %q: %w", ledgerName, err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) != ledgerLifecycleValueSize || value[4] > 1 {
		return LedgerLifecycle{}, false, fmt.Errorf("corrupt ledger lifecycle for %q", ledgerName)
	}

	return LedgerLifecycle{
		ID:     binary.BigEndian.Uint32(value[:4]),
		Active: value[4] == 1,
	}, true, nil
}
