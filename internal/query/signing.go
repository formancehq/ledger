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

// ed25519PublicKeySize is the size of an Ed25519 public key in bytes.
const ed25519PublicKeySize = 32

// ReadSigningKeys loads all signing keys from the given reader.
// Returns a map of keyID → SigningKeyEntry.
// Backward-compatible: values of exactly 32 bytes have no parent (root keys).
func ReadSigningKeys(reader dal.PebbleReader) (map[string]SigningKeyEntry, error) {
	lowerBound := []byte{dal.ZoneGlobal, dal.SubGlobSigningKey}
	upperBound := []byte{dal.ZoneGlobal, dal.SubGlobSigningKey + 1}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for signing keys: %w", err)
	}

	defer func() { _ = iter.Close() }()

	keys := make(map[string]SigningKeyEntry)

	for iter.First(); iter.Valid(); iter.Next() {
		// Key format: [ZoneGlobal(1)][SubGlobSigningKey(1)][keyID(variable)]
		key := iter.Key()
		keyID := string(key[2:]) // skip the zone + sub prefix bytes

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading signing key value: %w", err)
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

	return keys, nil
}

// ReadSigningKeysCursor returns a cursor over all registered signing keys.
// The number of keys is always small, so we load them all and use a slice cursor.
func ReadSigningKeysCursor(ctx context.Context, reader dal.PebbleReader) (cursor.Cursor[*commonpb.SigningKey], error) {
	_, span := queryTracer.Start(ctx, "query.list_signing_keys")
	defer span.End()

	keys, err := ReadSigningKeys(reader)
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
