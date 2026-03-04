package query

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/domain/analysis"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// CompactAccountIterator wraps a Pebble iterator and yields CompactAccount
// values by parsing attribute key bytes directly — no value deserialization.
// All entries for a (ledger, account) are contiguous in Pebble key order:
// volumes (separator \x00) sort before metadata (separator \x01).
type CompactAccountIterator struct {
	iter      *pebble.Iterator
	ledgerLen int // length of "ledger\x00" prefix in the canonical key

	// State for the current account being accumulated.
	currentAddr   []byte   // account address bytes (nil before first key)
	assets        []string // deduplicated asset names
	metadataKeys  []string // deduplicated metadata key names
	lastCanonical []byte   // last canonical key seen (for dedup)

	started bool
	done    bool
}

// NewCompactAccountIterator creates an iterator that yields CompactAccount
// values by scanning the Pebble attributes zone for the given ledger.
// Scan range: [0xF1][ledger\x00] → [0xF1][ledger\x01]
func NewCompactAccountIterator(reader dal.PebbleReader, ledger string) (*CompactAccountIterator, error) {
	// Lower bound: [0xF1][ledger]\x00
	lowerBound := make([]byte, 1+len(ledger)+1)
	lowerBound[0] = dal.KeyPrefixAttributes
	copy(lowerBound[1:], ledger)
	lowerBound[1+len(ledger)] = 0x00

	// Upper bound: [0xF1][ledger]\x01
	upperBound := make([]byte, 1+len(ledger)+1)
	upperBound[0] = dal.KeyPrefixAttributes
	copy(upperBound[1:], ledger)
	upperBound[1+len(ledger)] = 0x01

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating compact account iterator: %w", err)
	}

	return &CompactAccountIterator{
		iter:      iter,
		ledgerLen: len(ledger) + 1, // "ledger\x00"
	}, nil
}

// Next returns the next CompactAccount or io.EOF when exhausted.
func (it *CompactAccountIterator) Next() (analysis.CompactAccount, error) {
	if it.done {
		return analysis.CompactAccount{}, io.EOF
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.done = true
			if err := it.iter.Error(); err != nil {
				return analysis.CompactAccount{}, err
			}
			return analysis.CompactAccount{}, io.EOF
		}
	}

	for it.iter.Valid() {
		key := it.iter.Key()

		// Skip malformed keys.
		if len(key) <= 1+attributes.SuffixLen {
			it.iter.Next()
			continue
		}

		// Filter by attr type — only Volume and Metadata.
		attrType := key[len(key)-attributes.SuffixLen]
		var sepByte byte
		switch attrType {
		case dal.AttributePrefixVolume:
			sepByte = 0x00
		case dal.AttributePrefixMetadata:
			sepByte = 0x01
		default:
			it.iter.Next()
			continue
		}

		// Extract canonical key and parse account/name.
		canonical := key[1 : len(key)-attributes.SuffixLen]
		rest := canonical[it.ledgerLen:]
		sep := bytes.IndexByte(rest, sepByte)
		if sep < 0 {
			it.iter.Next()
			continue
		}

		accountBytes := rest[:sep]

		// Detect account boundary.
		if it.currentAddr != nil && !bytes.Equal(accountBytes, it.currentAddr) {
			acc := it.yieldCurrent()
			it.startAccount(accountBytes)
			it.addEntry(canonical, rest[sep+1:], attrType)
			it.iter.Next()
			return acc, nil
		}

		if it.currentAddr == nil {
			it.startAccount(accountBytes)
		}

		it.addEntry(canonical, rest[sep+1:], attrType)
		it.iter.Next()
	}

	// Iterator exhausted.
	it.done = true
	if err := it.iter.Error(); err != nil {
		return analysis.CompactAccount{}, err
	}
	if it.currentAddr != nil {
		acc := it.yieldCurrent()
		it.currentAddr = nil
		return acc, nil
	}

	return analysis.CompactAccount{}, io.EOF
}

// Close releases the underlying Pebble iterator.
func (it *CompactAccountIterator) Close() error {
	return it.iter.Close()
}

func (it *CompactAccountIterator) startAccount(addr []byte) {
	it.currentAddr = append(it.currentAddr[:0], addr...)
	it.assets = it.assets[:0]
	it.metadataKeys = it.metadataKeys[:0]
	it.lastCanonical = it.lastCanonical[:0]
}

func (it *CompactAccountIterator) addEntry(canonical []byte, nameBytes []byte, attrType byte) {
	// Deduplicate: skip if same canonical key as last (multiple raft index entries).
	if bytes.Equal(canonical, it.lastCanonical) {
		return
	}
	it.lastCanonical = append(it.lastCanonical[:0], canonical...)

	name := string(nameBytes)
	switch attrType {
	case dal.AttributePrefixVolume:
		it.assets = append(it.assets, name)
	case dal.AttributePrefixMetadata:
		it.metadataKeys = append(it.metadataKeys, name)
	}
}

func (it *CompactAccountIterator) yieldCurrent() analysis.CompactAccount {
	acc := analysis.CompactAccount{
		Address: string(it.currentAddr),
	}
	if len(it.assets) > 0 {
		acc.Assets = make([]string, len(it.assets))
		copy(acc.Assets, it.assets)
	}
	if len(it.metadataKeys) > 0 {
		acc.MetadataKeys = make([]string, len(it.metadataKeys))
		copy(acc.MetadataKeys, it.metadataKeys)
	}
	return acc
}
