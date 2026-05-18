package domain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// CanonicalBytes is implemented by key types that can be serialized
// to a canonical byte representation for storage lookups.
type CanonicalBytes interface {
	Bytes() []byte
	Unmarshal(data []byte) error
}

type AccountKey struct {
	Ledger  string
	Account string
}

type VolumeKey struct {
	AccountKey

	// Asset is the full asset string (e.g. "USD/4"). Used for API responses,
	// error messages, and as map key in application code.
	Asset string

	// AssetBase and AssetPrecision are the decomposed form, populated by
	// Unmarshal. They avoid re-parsing the Asset string when serializing
	// or aggregating.
	AssetBase      string
	AssetPrecision uint8
}

// Bytes returns a canonical byte representation of the balance key.
// Format: [ledger]\x00[account]\x00[asset_base][precision_byte]
// The last byte is always the precision. Asset bases are uppercase ASCII
// (≥0x41), so there is no ambiguity with precision values (0x00–0xFF).
func (bk VolumeKey) Bytes() []byte {
	base := bk.AssetBase
	precision := bk.AssetPrecision

	// Fallback: if constructed via struct literal without decomposed fields.
	if base == "" && bk.Asset != "" {
		base, precision = ParseAssetPrecision(bk.Asset)
	}

	// [ledger]\x00[account][sep][base][precision]
	ret := make([]byte, len(bk.Ledger)+1+len(bk.Account)+1+len(base)+1)
	n := copy(ret, bk.Ledger)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], bk.Account)
	ret[n] = dal.CanonicalKeySepVolume
	n++
	n += copy(ret[n:], base)
	ret[n] = precision

	return ret
}

// Unmarshal parses canonical bytes into the VolumeKey.
// Expected format: [ledger]\x00[account]\x00[asset_base][precision_byte]
// The last byte is always the precision.
func (bk *VolumeKey) Unmarshal(d []byte) error {
	// Split ledger on first null byte.
	before, after, ok := bytes.Cut(d, []byte{0x00})
	if !ok {
		return errors.New("invalid balance key bytes: missing ledger separator")
	}

	bk.Ledger = string(before)
	rest := after

	// Find the volume separator to split account from asset.
	before0, after0, ok0 := bytes.Cut(rest, []byte{dal.CanonicalKeySepVolume})
	if !ok0 {
		return errors.New("invalid balance key bytes: missing volume separator")
	}

	bk.Account = string(before0)

	assetPart := after0
	if len(assetPart) < 2 {
		return errors.New("invalid balance key bytes: asset part too short")
	}

	bk.AssetBase = string(assetPart[:len(assetPart)-1])
	bk.AssetPrecision = assetPart[len(assetPart)-1]
	bk.Asset = FormatAsset(bk.AssetBase, bk.AssetPrecision)

	return nil
}

var _ CanonicalBytes = (*VolumeKey)(nil)

type MetadataKey struct {
	AccountKey

	Key string
}

// Bytes returns a canonical byte representation of the metadata key.
// Format: [ledger]\x00[account]\x01[key].
func (mk MetadataKey) Bytes() []byte {
	ret := make([]byte, len(mk.Ledger)+1+len(mk.Account)+1+len(mk.Key))
	n := copy(ret, mk.Ledger)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], mk.Account)
	ret[n] = dal.CanonicalKeySepMetadata
	n++
	copy(ret[n:], mk.Key)

	return ret
}

// Unmarshal parses canonical bytes into the MetadataKey.
func (mk *MetadataKey) Unmarshal(d []byte) error {
	// Split ledger on first null byte.
	before, after, ok := bytes.Cut(d, []byte{0x00})
	if !ok {
		return errors.New("invalid metadata key bytes: missing ledger separator")
	}

	mk.Ledger = string(before)

	// Rest is account + \x01 + key
	rest := after

	before0, after0, ok0 := bytes.Cut(rest, []byte{dal.CanonicalKeySepMetadata})
	if !ok0 {
		return errors.New("invalid metadata key bytes: missing metadata separator")
	}

	mk.Account = string(before0)
	mk.Key = string(after0)

	return nil
}

var _ CanonicalBytes = (*MetadataKey)(nil)

type TransactionKey struct {
	Ledger string
	ID     uint64
}

// Bytes returns a canonical byte representation of the transaction key.
// Format: [ledger]\x00\x02[txID (8 bytes)].
// \x02 = CanonicalKeySepTransaction.
func (tk TransactionKey) Bytes() []byte {
	ret := make([]byte, len(tk.Ledger)+1+1+8)
	n := copy(ret, tk.Ledger)
	ret[n] = 0x00
	n++
	ret[n] = dal.CanonicalKeySepTransaction
	n++
	binary.BigEndian.PutUint64(ret[n:], tk.ID)

	return ret
}

// Unmarshal parses canonical bytes into the TransactionKey.
func (tk *TransactionKey) Unmarshal(d []byte) error {
	// Find the \x00\x02 separator between ledger name and txID.
	sep := -1

	for i, b := range d {
		if b == 0x00 && i+1 < len(d) && d[i+1] == dal.CanonicalKeySepTransaction {
			sep = i

			break
		}
	}

	if sep == -1 || len(d) < sep+2+8 {
		return fmt.Errorf("invalid transaction key bytes: expected [ledger]\\x00[0x%02X][txID(8)]", dal.CanonicalKeySepTransaction)
	}

	tk.Ledger = string(d[:sep])
	tk.ID = binary.BigEndian.Uint64(d[sep+2 : sep+2+8])

	return nil
}

var _ CanonicalBytes = (*TransactionKey)(nil)

type IdempotencyKey struct {
	Key string
}

// Bytes returns a canonical byte representation of the idempotency key.
func (ik IdempotencyKey) Bytes() []byte {
	return []byte(ik.Key)
}

// Unmarshal parses canonical bytes into the IdempotencyKey.
func (ik *IdempotencyKey) Unmarshal(data []byte) error {
	ik.Key = string(data)

	return nil
}

var _ CanonicalBytes = (*IdempotencyKey)(nil)

// TransactionReferenceKey represents a unique reference scoped to a ledger.
type TransactionReferenceKey struct {
	Ledger    string
	Reference string
}

// Bytes returns a canonical byte representation of the transaction reference key.
// Format: [ledger]\x00[reference].
func (trk TransactionReferenceKey) Bytes() []byte {
	ret := make([]byte, len(trk.Ledger)+1+len(trk.Reference))
	n := copy(ret, trk.Ledger)
	ret[n] = 0x00
	n++
	copy(ret[n:], trk.Reference)

	return ret
}

// Unmarshal parses canonical bytes into the TransactionReferenceKey.
func (trk *TransactionReferenceKey) Unmarshal(d []byte) error {
	parts := splitNullBytes(d, 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid transaction reference key bytes: expected 2 parts, got %d", len(parts))
	}

	trk.Ledger = string(parts[0])
	trk.Reference = string(parts[1])

	return nil
}

var _ CanonicalBytes = (*TransactionReferenceKey)(nil)

type LedgerKey struct {
	Name string
}

// Bytes returns a canonical byte representation of the ledger key.
func (lk LedgerKey) Bytes() []byte {
	return []byte(lk.Name)
}

// Unmarshal parses canonical bytes into the LedgerKey.
func (lk *LedgerKey) Unmarshal(data []byte) error {
	lk.Name = string(data)

	return nil
}

var _ CanonicalBytes = (*LedgerKey)(nil)

// LedgerMetadataKey represents a metadata key scoped to a ledger.
type LedgerMetadataKey struct {
	Ledger string
	Key    string
}

// Bytes returns a canonical byte representation of the ledger metadata key.
// Format: [ledger]\x01[key].
func (lmk LedgerMetadataKey) Bytes() []byte {
	ret := make([]byte, len(lmk.Ledger)+1+len(lmk.Key))
	n := copy(ret, lmk.Ledger)
	ret[n] = 0x01
	n++
	copy(ret[n:], lmk.Key)

	return ret
}

// Unmarshal parses canonical bytes into the LedgerMetadataKey.
func (lmk *LedgerMetadataKey) Unmarshal(d []byte) error {
	separator := -1

	for i, b := range d {
		if b == 0x01 {
			separator = i

			break
		}
	}

	if separator == -1 {
		return errors.New("invalid ledger metadata key bytes: missing separator")
	}

	lmk.Ledger = string(d[:separator])
	lmk.Key = string(d[separator+1:])

	return nil
}

var _ CanonicalBytes = (*LedgerMetadataKey)(nil)

// SinkConfigKey uniquely identifies an event sink by name.
type SinkConfigKey struct {
	Name string
}

func (k SinkConfigKey) Bytes() []byte {
	return []byte(k.Name)
}

// PreparedQueryKey uniquely identifies a prepared query by ledger and name.
type PreparedQueryKey struct {
	Ledger string
	Name   string
}

func (k PreparedQueryKey) Bytes() []byte {
	ret := make([]byte, len(k.Ledger)+1+len(k.Name))
	n := copy(ret, k.Ledger)
	ret[n] = 0x00
	n++
	copy(ret[n:], k.Name)

	return ret
}

// NumscriptVersionKey uniquely identifies a numscript by ledger and name for version tracking.
type NumscriptVersionKey struct {
	Ledger string
	Name   string
}

func (k NumscriptVersionKey) Bytes() []byte {
	ret := make([]byte, len(k.Ledger)+1+len(k.Name))
	n := copy(ret, k.Ledger)
	ret[n] = 0x00
	n++
	copy(ret[n:], k.Name)

	return ret
}

// NumscriptEntryKey uniquely identifies a specific numscript version entry scoped to a ledger.
type NumscriptEntryKey struct {
	Ledger  string
	Name    string
	Version string
}

func (k NumscriptEntryKey) Bytes() []byte {
	ret := make([]byte, len(k.Ledger)+1+len(k.Name)+1+len(k.Version))
	n := copy(ret, k.Ledger)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], k.Name)
	ret[n] = 0x00
	n++
	copy(ret[n:], k.Version)

	return ret
}

const (
	// NumscriptVersionTagSemver is the tag byte for semver-encoded numscript entries.
	NumscriptVersionTagSemver byte = 0x00
	// NumscriptVersionTagLatest is the tag byte for the "latest" numscript slot.
	NumscriptVersionTagLatest byte = 0x01
)

// splitNullBytes splits data by null bytes into at most n parts.
func splitNullBytes(data []byte, n int) [][]byte {
	var parts [][]byte

	start := 0

	for i, b := range data {
		if b == 0x00 && len(parts) < n-1 {
			parts = append(parts, data[start:i])
			start = i + 1
		}
	}

	parts = append(parts, data[start:])

	return parts
}
