package readstore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Uint64Cursor is a fixed-key big-endian uint64 progress cursor persisted in
// the read store. It centralises the BE8 encode/decode, the ErrNotFound-as-0
// mapping, and the length guard shared by every tail-worker progress cursor
// (log index, applied-proposal, audit). A missing key reads as 0; a stored
// value whose length is not 8 bytes is a hard error (corruption), never a
// silent 0 — collapsing it would strand the worker at a bogus position.
type Uint64Cursor struct{ key []byte }

// Read returns the stored cursor, or 0 if the key is absent.
func (c Uint64Cursor) Read(r dal.PebbleGetter) (uint64, error) {
	v, closer, err := r.Get(c.key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading cursor %q: %w", c.key, err)
	}

	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, fmt.Errorf("cursor %q: unexpected length %d", c.key, len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// Write stages the cursor value into batch as a big-endian 8-byte value.
func (c Uint64Cursor) Write(batch *dal.WriteSession, sequence uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)

	return batch.SetBytes(c.key, buf[:])
}

// Package-level cursors, one per persisted progress key. Keys are unchanged
// from the pre-refactor methods, so no on-disk migration is required.
var (
	progressCursor        = Uint64Cursor{key: ProgressKey()}
	appliedProposalCursor = Uint64Cursor{key: AppliedProposalProgressKey()}
	auditCursor           = Uint64Cursor{key: AuditProgressKey()}
)
