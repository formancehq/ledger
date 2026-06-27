package dal

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// FSMDigestSize is the size of the XXH3-128 digest stored under
// SubGlobFSMDigest. The value record format is fsmDigestRecordSize bytes:
// [appliedIndex BE 8][digest 16].
const (
	FSMDigestSize       = 16
	fsmDigestRecordSize = 8 + FSMDigestSize
)

// fsmDigestKey is the single Pebble key holding the rolling FSM digest.
var fsmDigestKey = []byte{ZoneGlobal, SubGlobFSMDigest}

// FSMDigestKey returns the Pebble key of the rolling FSM digest.
func FSMDigestKey() []byte {
	out := make([]byte, len(fsmDigestKey))
	copy(out, fsmDigestKey)

	return out
}

// EncodeFSMDigest packs (appliedIndex, hash) into the on-disk record
// format. The hash slice MUST be exactly FSMDigestSize bytes.
func EncodeFSMDigest(appliedIndex uint64, hash []byte) ([]byte, error) {
	if len(hash) != FSMDigestSize {
		return nil, fmt.Errorf("fsm digest: hash size %d, want %d", len(hash), FSMDigestSize)
	}

	out := make([]byte, fsmDigestRecordSize)
	binary.BigEndian.PutUint64(out[:8], appliedIndex)
	copy(out[8:], hash)

	return out, nil
}

// DecodeFSMDigest unpacks an on-disk rolling FSM digest record into
// (appliedIndex, hash). Returns an error if the record length is wrong.
// The returned hash slice aliases v — callers that retain it past the
// underlying buffer's lifetime must copy.
func DecodeFSMDigest(v []byte) (appliedIndex uint64, hash []byte, err error) {
	if len(v) != fsmDigestRecordSize {
		return 0, nil, fmt.Errorf("fsm digest: record length %d, want %d", len(v), fsmDigestRecordSize)
	}

	appliedIndex = binary.BigEndian.Uint64(v[:8])
	hash = v[8:]

	return appliedIndex, hash, nil
}

// ZeroFSMDigest is the all-zeros digest used as the chain seed before any
// entry has been applied. Persisting `(0, ZeroFSMDigest)` on a fresh boot
// is unnecessary: any caller that reads the key when it does not yet exist
// gets ErrNotFound and falls back to this value via LoadFSMDigest.
var ZeroFSMDigest = make([]byte, FSMDigestSize)

// LoadFSMDigest reads the persisted rolling FSM digest from the supplied
// getter, returning ZeroFSMDigest when the key does not yet exist (fresh
// cluster, or pre-determinism boot). Used at NewStore time to seed the
// in-memory cache, after RestoreCheckpoint to re-seed from the
// freshly-published live database, and from GetFSMDigest gRPC handler to
// serve the snapshot view to peers.
func LoadFSMDigest(reader PebbleGetter) (appliedIndex uint64, hash []byte, err error) {
	value, closer, getErr := reader.Get(fsmDigestKey)
	if getErr != nil {
		if errors.Is(getErr, pebble.ErrNotFound) {
			return 0, append([]byte(nil), ZeroFSMDigest...), nil
		}

		return 0, nil, fmt.Errorf("reading rolling fsm digest: %w", getErr)
	}

	defer func() { _ = closer.Close() }()

	appliedIndex, raw, decodeErr := DecodeFSMDigest(value)
	if decodeErr != nil {
		return 0, nil, decodeErr
	}

	// Copy out of the closer-owned buffer.
	hash = append([]byte(nil), raw...)

	return appliedIndex, hash, nil
}
