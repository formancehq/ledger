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

// AppendBytes appends the canonical byte representation to dst and returns the
// extended slice. Format: [ledger]\x00[account]\x00[asset_base][precision_byte]
func (bk VolumeKey) AppendBytes(dst []byte) []byte {
	base := bk.AssetBase
	precision := bk.AssetPrecision

	// Fallback: if constructed via struct literal without decomposed fields.
	if base == "" && bk.Asset != "" {
		base, precision = ParseAssetPrecision(bk.Asset)
	}

	dst = append(dst, bk.Ledger...)
	dst = append(dst, 0x00)
	dst = append(dst, bk.Account...)
	dst = append(dst, dal.CanonicalKeySepVolume)
	dst = append(dst, base...)
	dst = append(dst, precision)

	return dst
}

// Bytes returns a canonical byte representation of the balance key.
func (bk VolumeKey) Bytes() []byte {
	return bk.AppendBytes(nil)
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

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledger]\x00[account]\x01[key].
func (mk MetadataKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, mk.Ledger...)
	dst = append(dst, 0x00)
	dst = append(dst, mk.Account...)
	dst = append(dst, dal.CanonicalKeySepMetadata)
	dst = append(dst, mk.Key...)

	return dst
}

// Bytes returns a canonical byte representation of the metadata key.
func (mk MetadataKey) Bytes() []byte {
	return mk.AppendBytes(nil)
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

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledger]\x00\x02[txID (8 bytes)].
func (tk TransactionKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, tk.Ledger...)
	dst = append(dst, 0x00, dal.CanonicalKeySepTransaction)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], tk.ID)
	dst = append(dst, buf[:]...)

	return dst
}

// Bytes returns a canonical byte representation of the transaction key.
func (tk TransactionKey) Bytes() []byte {
	return tk.AppendBytes(nil)
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

// AppendBytes appends the canonical byte representation to dst.
func (ik IdempotencyKey) AppendBytes(dst []byte) []byte {
	return append(dst, ik.Key...)
}

// Bytes returns a canonical byte representation of the idempotency key.
func (ik IdempotencyKey) Bytes() []byte {
	return ik.AppendBytes(nil)
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

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledger]\x00[reference].
func (trk TransactionReferenceKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, trk.Ledger...)
	dst = append(dst, 0x00)
	dst = append(dst, trk.Reference...)

	return dst
}

// Bytes returns a canonical byte representation of the transaction reference key.
func (trk TransactionReferenceKey) Bytes() []byte {
	return trk.AppendBytes(nil)
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

// AppendBytes appends the canonical byte representation to dst.
func (lk LedgerKey) AppendBytes(dst []byte) []byte {
	return append(dst, lk.Name...)
}

// Bytes returns a canonical byte representation of the ledger key.
func (lk LedgerKey) Bytes() []byte {
	return lk.AppendBytes(nil)
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

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledger]\x01[key].
func (lmk LedgerMetadataKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, lmk.Ledger...)
	dst = append(dst, 0x01)
	dst = append(dst, lmk.Key...)

	return dst
}

// Bytes returns a canonical byte representation of the ledger metadata key.
func (lmk LedgerMetadataKey) Bytes() []byte {
	return lmk.AppendBytes(nil)
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

func (k SinkConfigKey) AppendBytes(dst []byte) []byte {
	return append(dst, k.Name...)
}

func (k SinkConfigKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// PreparedQueryKey uniquely identifies a prepared query by ledger and name.
type PreparedQueryKey struct {
	Ledger string
	Name   string
}

func (k PreparedQueryKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, k.Ledger...)
	dst = append(dst, 0x00)
	dst = append(dst, k.Name...)

	return dst
}

func (k PreparedQueryKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// NumscriptVersionKey uniquely identifies a numscript by ledger and name for version tracking.
type NumscriptVersionKey struct {
	Ledger string
	Name   string
}

func (k NumscriptVersionKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, k.Ledger...)
	dst = append(dst, 0x00)
	dst = append(dst, k.Name...)

	return dst
}

func (k NumscriptVersionKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// NumscriptEntryKey uniquely identifies a specific numscript version entry scoped to a ledger.
type NumscriptEntryKey struct {
	Ledger  string
	Name    string
	Version string
}

func (k NumscriptEntryKey) AppendBytes(dst []byte) []byte {
	dst = append(dst, k.Ledger...)
	dst = append(dst, 0x00)
	dst = append(dst, k.Name...)
	dst = append(dst, 0x00)
	dst = append(dst, k.Version...)

	return dst
}

func (k NumscriptEntryKey) Bytes() []byte {
	return k.AppendBytes(nil)
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
