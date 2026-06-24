package query

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/analysis"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// CompactAccountIterator yields CompactAccount values by merging Volume and
// Metadata attribute ranges. With the type-prefixed key layout, V and M entries
// are in separate Pebble ranges, so two sub-iterators are merged by account address.
type CompactAccountIterator struct {
	v compactSubIter // Volume sub-iterator
	m compactSubIter // Metadata sub-iterator

	done bool
}

// compactSubIter iterates one attribute type, collecting all entries per account.
type compactSubIter struct {
	iter      *pebble.Iterator
	ledgerLen int  // length of "ledger\x00" in canonical key
	attrType  byte // dal.SubAttrVolume or dal.SubAttrMetadata
	sepByte   byte // canonical key separator for this type

	// Current account state — populated by advance().
	addr          []byte
	assets        []string // only for V type — deduped asset names (color-collapsed)
	seenAssets    map[string]struct{}
	metadataKeys  []string // only for M type
	lastCanonical []byte   // for dedup

	valid   bool
	started bool

	// err is set when the iterator encounters a malformed key (a "should not
	// happen" branch per CLAUDE.md invariant #7) so the outer Next() can
	// surface the violation instead of silently dropping the entry.
	err error
}

// NewCompactAccountIterator creates an iterator that yields CompactAccount
// values by scanning Volume and Metadata attribute ranges for the given ledger.
func NewCompactAccountIterator(reader dal.PebbleReader, ledgerName string) (*CompactAccountIterator, error) {
	vIter, err := newCompactSubIter(reader, dal.SubAttrVolume, dal.CanonicalKeySepVolume, ledgerName, dal.LedgerNameFixedSize)
	if err != nil {
		return nil, err
	}

	mIter, err := newCompactSubIter(reader, dal.SubAttrMetadata, dal.CanonicalKeySepMetadata, ledgerName, dal.LedgerNameFixedSize)
	if err != nil {
		_ = vIter.iter.Close()

		return nil, err
	}

	return &CompactAccountIterator{v: *vIter, m: *mIter}, nil
}

func newCompactSubIter(reader dal.PebbleReader, attrType, sepByte byte, ledgerName string, ledgerLen int) (*compactSubIter, error) {
	// Bounds: [0xF1][attrType][ledgerName padded 64B] → successor (last byte +1).
	lowerBound := make([]byte, 2+dal.LedgerNameFixedSize)
	lowerBound[0] = dal.ZoneAttributes
	lowerBound[1] = attrType
	copy(lowerBound[2:], ledgerName)

	upperBound := make([]byte, 2+dal.LedgerNameFixedSize)
	copy(upperBound, lowerBound)
	upperBound[len(upperBound)-1]++

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating compact sub-iterator for type 0x%02x: %w", attrType, err)
	}

	return &compactSubIter{
		iter:      iter,
		ledgerLen: ledgerLen,
		attrType:  attrType,
		sepByte:   sepByte,
	}, nil
}

// advance moves the sub-iterator to the next account, collecting all entries for it.
// Returns false when exhausted.
func (si *compactSubIter) advance() bool {
	si.assets = si.assets[:0]
	si.metadataKeys = si.metadataKeys[:0]
	si.lastCanonical = si.lastCanonical[:0]
	si.addr = nil
	if si.seenAssets != nil {
		clear(si.seenAssets)
	}

	if !si.started {
		si.started = true
		if !si.iter.First() {
			si.valid = false

			return false
		}
	}

	for si.iter.Valid() {
		key := si.iter.Key()

		if len(key) <= 1+attributes.AttrTypeLen {
			si.iter.Next()

			continue
		}

		// Extract canonical key: key[2:] in new layout.
		canonical := key[2:]
		rest := canonical[si.ledgerLen:]

		sep := bytes.IndexByte(rest, si.sepByte)
		if sep < 0 {
			si.iter.Next()

			continue
		}

		accountBytes := rest[:sep]

		if si.addr == nil {
			// First account in this pass.
			si.addr = append(si.addr[:0], accountBytes...)
		} else if !bytes.Equal(accountBytes, si.addr) {
			// Crossed account boundary — current account is complete.
			si.valid = true

			return true
		}

		// Deduplicate by canonical key.
		if !bytes.Equal(canonical, si.lastCanonical) {
			si.lastCanonical = append(si.lastCanonical[:0], canonical...)
			nameBytes := rest[sep+1:]

			switch si.attrType {
			case dal.SubAttrVolume:
				// Volume key layout after the account separator is
				//   [color]\x00[asset_base][precision_byte]
				// We surface deduped asset names (color is collapsed at the
				// analysis layer — pattern discovery does not care about
				// color), so split on the second 0x00 to skip the color.
				colorSep := bytes.IndexByte(nameBytes, dal.CanonicalKeySepVolume)
				if colorSep < 0 {
					// "Should not happen": every volume key written by the
					// FSM carries the color separator (CLAUDE.md invariant
					// #7). Surface the violation loudly instead of silently
					// skipping the entry — a missing separator means either
					// pre-color legacy data or storage corruption.
					si.err = fmt.Errorf("compact_account: volume key missing color separator (canonical=%x)", canonical)
					si.valid = false

					return false
				}
				assetBytes := nameBytes[colorSep+1:]
				if len(assetBytes) < 2 {
					// Same class of invariant: asset_base must be at least
					// one byte plus the trailing precision byte. Anything
					// shorter is corrupt.
					si.err = fmt.Errorf("compact_account: volume key asset section too short (canonical=%x)", canonical)
					si.valid = false

					return false
				}
				asset := domain.FormatAsset(string(assetBytes[:len(assetBytes)-1]), assetBytes[len(assetBytes)-1])
				if si.seenAssets == nil {
					si.seenAssets = make(map[string]struct{})
				}
				if _, ok := si.seenAssets[asset]; !ok {
					si.seenAssets[asset] = struct{}{}
					si.assets = append(si.assets, asset)
				}
			case dal.SubAttrMetadata:
				si.metadataKeys = append(si.metadataKeys, string(nameBytes))
			}
		}

		si.iter.Next()
	}

	// Exhausted — flush last account if any.
	si.valid = si.addr != nil

	return si.valid
}

// Next returns the next CompactAccount or io.EOF when exhausted.
func (it *CompactAccountIterator) Next() (analysis.CompactAccount, error) {
	if it.done {
		return analysis.CompactAccount{}, io.EOF
	}

	// Advance sub-iterators that were consumed in the previous call.
	if !it.v.valid {
		it.v.advance()
	}
	if !it.m.valid {
		it.m.advance()
	}

	// An invariant violation surfaced by advance() must not be silently
	// dropped: the upstream key layout is wrong and the consumer needs to
	// know rather than silently miss volume entries.
	if it.v.err != nil {
		it.done = true

		return analysis.CompactAccount{}, it.v.err
	}
	if it.m.err != nil {
		it.done = true

		return analysis.CompactAccount{}, it.m.err
	}

	if !it.v.valid && !it.m.valid {
		it.done = true

		// Check for errors.
		if err := it.v.iter.Error(); err != nil {
			return analysis.CompactAccount{}, err
		}
		if err := it.m.iter.Error(); err != nil {
			return analysis.CompactAccount{}, err
		}

		return analysis.CompactAccount{}, io.EOF
	}

	var acc analysis.CompactAccount

	switch {
	case it.v.valid && it.m.valid:
		cmp := bytes.Compare(it.v.addr, it.m.addr)
		switch {
		case cmp < 0:
			acc = it.yieldV()
		case cmp > 0:
			acc = it.yieldM()
		default:
			acc = it.yieldBoth()
		}
	case it.v.valid:
		acc = it.yieldV()
	default:
		acc = it.yieldM()
	}

	return acc, nil
}

func (it *CompactAccountIterator) yieldV() analysis.CompactAccount {
	acc := analysis.CompactAccount{Address: string(it.v.addr)}
	if len(it.v.assets) > 0 {
		acc.Assets = make([]string, len(it.v.assets))
		copy(acc.Assets, it.v.assets)
	}
	it.v.valid = false // mark for advance on next call

	return acc
}

func (it *CompactAccountIterator) yieldM() analysis.CompactAccount {
	acc := analysis.CompactAccount{Address: string(it.m.addr)}
	if len(it.m.metadataKeys) > 0 {
		acc.MetadataKeys = make([]string, len(it.m.metadataKeys))
		copy(acc.MetadataKeys, it.m.metadataKeys)
	}
	it.m.valid = false

	return acc
}

func (it *CompactAccountIterator) yieldBoth() analysis.CompactAccount {
	acc := analysis.CompactAccount{Address: string(it.v.addr)}
	if len(it.v.assets) > 0 {
		acc.Assets = make([]string, len(it.v.assets))
		copy(acc.Assets, it.v.assets)
	}
	if len(it.m.metadataKeys) > 0 {
		acc.MetadataKeys = make([]string, len(it.m.metadataKeys))
		copy(acc.MetadataKeys, it.m.metadataKeys)
	}
	it.v.valid = false
	it.m.valid = false

	return acc
}

// Close releases the underlying Pebble iterators.
func (it *CompactAccountIterator) Close() error {
	vErr := it.v.iter.Close()
	mErr := it.m.iter.Close()

	if vErr != nil {
		return vErr
	}

	return mErr
}
