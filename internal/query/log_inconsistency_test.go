package query

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// fakeGetter is a hand-rolled PebbleGetter that lets each test inject the
// exact value/error the read should return. Avoids spinning up a real Pebble
// for what is purely an error-surface test.
type fakeGetter struct {
	entries map[string][]byte
	getErr  error
}

func (f *fakeGetter) Get(key []byte) ([]byte, io.Closer, error) {
	if f.getErr != nil {
		return nil, nil, f.getErr
	}

	v, ok := f.entries[string(key)]
	if !ok {
		return nil, nil, pebble.ErrNotFound
	}

	return v, io.NopCloser(nil), nil
}

func ledgerLogIndexValue(seq uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)

	return buf
}

func logID8(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)

	return buf
}

// TestReadLedgerLogsCompiled_HappyPath asserts the function still returns the
// expected global sequences when the per-ledger log index is healthy.
func TestReadLedgerLogsCompiled_HappyPath(t *testing.T) {
	t.Parallel()

	const ledgerID = uint32(42)
	kb := dal.NewKeyBuilder()
	entries := map[string][]byte{
		string(readstore.LedgerLogKey(kb, ledgerID, 10)): ledgerLogIndexValue(1010),
		string(readstore.LedgerLogKey(kb, ledgerID, 20)): ledgerLogIndexValue(2020),
		string(readstore.LedgerLogKey(kb, ledgerID, 30)): ledgerLogIndexValue(3030),
	}

	indexReader := &fakeGetter{entries: entries}

	cur, err := ReadLedgerLogsCompiled(
		&fakeGetter{}, // pebble reader unused in this path (cursor.Next is not called)
		indexReader,
		ledgerID,
		[][]byte{logID8(10), logID8(20), logID8(30)},
	)
	require.NoError(t, err)

	lc, ok := cur.(*ledgerLogCursor)
	require.True(t, ok)
	require.Equal(t, []uint64{1010, 2020, 3030}, lc.seqs)
}

// TestReadLedgerLogsCompiled_MalformedLogIDBytes asserts that a logID with
// the wrong byte length surfaces as ErrIndexInconsistent rather than being
// silently skipped (#192).
func TestReadLedgerLogsCompiled_MalformedLogIDBytes(t *testing.T) {
	t.Parallel()

	_, err := ReadLedgerLogsCompiled(
		&fakeGetter{}, &fakeGetter{},
		7,
		[][]byte{{0x01, 0x02}}, // logID is only 2 bytes — caught before any Get
	)

	require.Error(t, err)

	var inc *domain.ErrIndexInconsistent
	require.ErrorAs(t, err, &inc)
	require.Contains(t, inc.Detail, "unexpected length 2")
}

// TestReadLedgerLogsCompiled_IndexGetError asserts that any error from the
// per-ledger log index lookup (including pebble.ErrNotFound — the filter
// index produced the logID, so a miss is structurally inconsistent) surfaces
// as ErrIndexInconsistent rather than silently dropping the entry.
func TestReadLedgerLogsCompiled_IndexGetError(t *testing.T) {
	t.Parallel()

	const ledgerID = uint32(42)
	indexReader := &fakeGetter{getErr: pebble.ErrNotFound}

	_, err := ReadLedgerLogsCompiled(
		&fakeGetter{},
		indexReader,
		ledgerID,
		[][]byte{logID8(99)},
	)

	require.Error(t, err)

	var inc *domain.ErrIndexInconsistent
	require.ErrorAs(t, err, &inc)
	require.Contains(t, inc.Detail, "logID=99")
}

// TestReadLedgerLogsCompiled_MalformedIndexValue asserts that an index entry
// whose value isn't an 8-byte sequence surfaces as ErrIndexInconsistent.
// Pre-fix the loop silently skipped these — a hash check on the index store
// would still pass, but the query result would lose entries.
func TestReadLedgerLogsCompiled_MalformedIndexValue(t *testing.T) {
	t.Parallel()

	const ledgerID = uint32(42)
	kb := dal.NewKeyBuilder()
	entries := map[string][]byte{
		string(readstore.LedgerLogKey(kb, ledgerID, 5)): {0xAB}, // 1 byte instead of 8
	}

	_, err := ReadLedgerLogsCompiled(
		&fakeGetter{},
		&fakeGetter{entries: entries},
		ledgerID,
		[][]byte{logID8(5)},
	)

	require.Error(t, err)

	var inc *domain.ErrIndexInconsistent
	require.ErrorAs(t, err, &inc)
	require.Contains(t, inc.Detail, "logID=5")
	require.Contains(t, inc.Detail, "unexpected length 1")
}
