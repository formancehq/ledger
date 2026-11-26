package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// SigningKeyEntry holds a signing key's public key and optional parent key ID.
type SigningKeyEntry struct {
	PublicKey   []byte
	ParentKeyID string
}

// MalformedSigningKeyRow describes a SubGlobSigningKey row whose value is too
// short to hold an Ed25519 public key. Only SaveSigningKey writes these rows and
// it always writes at least the full public key, so a short value means
// corruption, tampering, or a partial write.
//
// The rows are skipped rather than fatal: this decode runs on the boot path
// (state.Recovery), and panicking there crash-loops every replica because the
// row is Raft-replicated. They are returned so recovery can log them loudly and
// the checker can surface them as integrity events, instead of a key silently
// disappearing from the trusted set. Mirrors MalformedReversionRow.
type MalformedSigningKeyRow struct {
	KeyID       string
	Key         []byte
	ValueLength int
	Reason      string
}

// ed25519PublicKeySize is the size of an Ed25519 public key in bytes.
const ed25519PublicKeySize = 32

// ReadSigningKeys loads all signing keys from the given reader.
// Returns a map of keyID → SigningKeyEntry.
// Backward-compatible: values of exactly 32 bytes have no parent (root keys).
// Rows whose value is too short to hold a public key are skipped and reported
// in the malformed slice rather than decoded.
func ReadSigningKeys(reader dal.PebbleReader) (map[string]SigningKeyEntry, []MalformedSigningKeyRow, error) {
	lowerBound := []byte{dal.ZoneGlobal, dal.SubGlobSigningKey}
	upperBound := []byte{dal.ZoneGlobal, dal.SubGlobSigningKey + 1}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, nil, fmt.Errorf("creating iterator for signing keys: %w", err)
	}

	defer func() { _ = iter.Close() }()

	keys := make(map[string]SigningKeyEntry)

	var malformed []MalformedSigningKeyRow

	for iter.First(); iter.Valid(); iter.Next() {
		// Key format: [ZoneGlobal(1)][SubGlobSigningKey(1)][keyID(variable)]
		key := iter.Key()
		keyID := string(key[2:]) // skip the zone + sub prefix bytes

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, nil, fmt.Errorf("reading signing key value: %w", err)
		}

		// Guard before slicing: value[:32] is bounds-checked before copy runs, so
		// a short row would panic here rather than truncate.
		if len(value) < ed25519PublicKeySize {
			malformed = append(malformed, MalformedSigningKeyRow{
				KeyID:       keyID,
				Key:         append([]byte(nil), key...),
				ValueLength: len(value),
				Reason:      "value shorter than an Ed25519 public key",
			})

			continue
		}

		entry := SigningKeyEntry{
			PublicKey: make([]byte, ed25519PublicKeySize),
		}
		copy(entry.PublicKey, value[:ed25519PublicKeySize])

		// Backward-compatible: bytes after 32 = parentKeyID
		if len(value) > ed25519PublicKeySize {
			entry.ParentKeyID = string(value[ed25519PublicKeySize:])
		}

		keys[keyID] = entry
	}

	return keys, malformed, nil
}

// ReadSigningKeysCursor returns a cursor over all registered signing keys.
// The number of keys is always small, so we load them all and use a slice cursor.
func ReadSigningKeysCursor(ctx context.Context, reader dal.PebbleReader) (cursor.Cursor[*commonpb.SigningKey], error) {
	_, span := queryTracer.Start(ctx, "query.list_signing_keys")
	defer span.End()

	// Malformed rows are intentionally dropped here: this cursor feeds
	// ListSigningKeys, which has no channel for integrity events. Recovery logs
	// them and the checker reports them as SIGNING_KEY_MISMATCH.
	keys, _, err := ReadSigningKeys(reader)
	if err != nil {
		return nil, err
	}

	items := make([]*commonpb.SigningKey, 0, len(keys))
	for keyID, entry := range keys {
		items = append(items, &commonpb.SigningKey{
			KeyId:       keyID,
			PublicKey:   entry.PublicKey,
			ParentKeyId: entry.ParentKeyID,
		})
	}

	return cursor.NewSliceCursor(items), nil
}

// ReadSigningConfig loads the require-signatures flag from the given reader.
// Returns false if the config key does not exist.
func ReadSigningConfig(reader dal.PebbleGetter) (bool, error) {
	v, err := dal.ReadBool(reader, []byte{dal.ZoneGlobal, dal.SubGlobSigningConfig})
	if err != nil {
		return false, fmt.Errorf("loading signing config: %w", err)
	}

	return v, nil
}
