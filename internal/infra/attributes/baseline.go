package attributes

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// CreateBaselineSnapshot iterates all computed attribute values (volumes,
// metadata, transactions) from the source reader and writes them into a
// compact Pebble DB at destPath. Only the final value per canonical key is
// stored at raft index 0 — no history, no logs, no system keys.
//
// The result is orders of magnitude smaller than a full Pebble checkpoint
// because it contains only the seal-hash domain (attributes zone), not the
// entire store. This is critical for archived periods: the whole point of
// archiving is to reclaim disk space, so a full checkpoint would be
// counter-productive.
//
// The write uses atomic rename: data is written to a temporary directory
// first, then renamed to destPath. This eliminates TOCTOU races with
// concurrent readers (the checker).
func CreateBaselineSnapshot(reader dal.PebbleReader, attrs *Attributes, destPath string) error {
	// Write to a temporary sibling directory, then atomic rename.
	tmpPath := destPath + fmt.Sprintf(".tmp-%d-%d", os.Getpid(), time.Now().UnixNano())

	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return fmt.Errorf("creating temp baseline dir: %w", err)
	}

	// Clean up temp on failure
	success := false

	defer func() {
		if !success {
			_ = os.RemoveAll(tmpPath)
		}
	}()

	db, err := pebble.Open(tmpPath, &pebble.Options{
		DisableWAL: true,
	})
	if err != nil {
		return fmt.Errorf("opening temp baseline db: %w", err)
	}

	// Write all computed attribute values into the baseline DB.
	if err := writeBaselineAttributes(reader, attrs, db); err != nil {
		_ = db.Close()

		return err
	}

	if err := db.Flush(); err != nil {
		_ = db.Close()

		return fmt.Errorf("flushing baseline db: %w", err)
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("closing baseline db: %w", err)
	}

	// Atomic swap: remove old, rename temp → dest.
	_ = os.RemoveAll(destPath)

	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("renaming baseline snapshot: %w", err)
	}

	success = true

	return nil
}

// writeBaselineAttributes iterates each attribute type from the source reader
// and writes the computed (last-value-per-key) entries into the baseline DB
// at raft index 0 using the standard attribute key layout.
func writeBaselineAttributes(reader dal.PebbleReader, attrs *Attributes, db *pebble.DB) error {
	if err := writeBaselineAttr(reader, attrs.Volume, db); err != nil {
		return fmt.Errorf("writing baseline volumes: %w", err)
	}

	if err := writeBaselineAttr(reader, attrs.Metadata, db); err != nil {
		return fmt.Errorf("writing baseline metadata: %w", err)
	}

	if err := writeBaselineAttr(reader, attrs.Transaction, db); err != nil {
		return fmt.Errorf("writing baseline transactions: %w", err)
	}

	return nil
}

// writeBaselineAttr writes all computed entries for a single attribute type.
func writeBaselineAttr[V interface {
	~*E
	proto.Message
	MarshalVT() ([]byte, error)
}, E any](reader dal.PebbleReader, attr *Attribute[V], db *pebble.DB) error {
	si, err := attr.NewStreamingIter(reader, nil)
	if err != nil {
		return err
	}

	defer func() { _ = si.Close() }()

	// Pre-allocate a key buffer for writing entries.
	var keyBuf []byte

	for si.Next() {
		e := si.Entry()

		data, marshalErr := e.Value.MarshalVT()
		if marshalErr != nil {
			return fmt.Errorf("marshaling value: %w", marshalErr)
		}

		// Build Pebble key: [KeyPrefixAttributes][canonicalKey][attrType]
		pLen := 2 + len(e.CanonicalKey) // 1 prefix + N canonical + 1 attrType

		if len(keyBuf) < pLen {
			keyBuf = make([]byte, pLen)
		}

		keyBuf[0] = dal.KeyPrefixAttributes
		copy(keyBuf[1:], e.CanonicalKey)
		keyBuf[1+len(e.CanonicalKey)] = attr.prefix

		if err := db.Set(keyBuf[:pLen], data, pebble.NoSync); err != nil {
			return fmt.Errorf("writing entry: %w", err)
		}
	}

	if si.Err() != nil {
		return si.Err()
	}

	return nil
}
