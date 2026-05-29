package domain

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// CanonicalBytes is implemented by key types that can be serialized
// to a canonical byte representation for storage lookups.
type CanonicalBytes interface {
	Bytes() []byte
	Unmarshal(data []byte) error
}

type AccountKey struct {
	LedgerID uint32
	Account  string
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

// appendLedgerID appends a uint32 ledger ID in big-endian order to dst.
func appendLedgerID(dst []byte, id uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], id)

	return append(dst, buf[:]...)
}

// NewVolumeKey creates a VolumeKey with pre-parsed AssetBase and AssetPrecision,
// avoiding re-parsing on every AppendBytes call in the hot path.
func NewVolumeKey(ledgerID uint32, account, asset string) VolumeKey {
	base, precision := ParseAssetPrecision(asset)

	return VolumeKey{
		AccountKey:     AccountKey{LedgerID: ledgerID, Account: account},
		Asset:          asset,
		AssetBase:      base,
		AssetPrecision: precision,
	}
}

// AppendBytes appends the canonical byte representation to dst and returns the
// extended slice. Format: [ledgerID BE 4B][account][sep][asset_base][precision_byte].
func (bk VolumeKey) AppendBytes(dst []byte) []byte {
	base := bk.AssetBase
	precision := bk.AssetPrecision

	// Fallback: if constructed via struct literal without decomposed fields.
	if base == "" && bk.Asset != "" {
		base, precision = ParseAssetPrecision(bk.Asset)
	}

	dst = appendLedgerID(dst, bk.LedgerID)
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
func (bk *VolumeKey) Unmarshal(d []byte) error {
	if len(d) < 4 {
		return errors.New("invalid balance key bytes: too short for ledger ID")
	}

	bk.LedgerID = binary.BigEndian.Uint32(d[0:4])

	rest := d[4:]
	parts := splitNullBytes(rest, 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid balance key bytes: expected 2 parts after ledger ID, got %d", len(parts))
	}

	bk.Account = string(parts[0])

	assetPart := parts[1]
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
// Format: [ledgerID BE 4B][account]\x01[key].
func (mk MetadataKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, mk.LedgerID)
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
	if len(d) < 4 {
		return errors.New("invalid metadata key bytes: too short for ledger ID")
	}

	mk.LedgerID = binary.BigEndian.Uint32(d[0:4])

	rest := d[4:]
	separator := -1

	for i, b := range rest {
		if b == dal.CanonicalKeySepMetadata {
			separator = i

			break
		}
	}

	if separator == -1 {
		return errors.New("invalid metadata key bytes: missing account/key separator")
	}

	mk.Account = string(rest[:separator])
	mk.Key = string(rest[separator+1:])

	return nil
}

var _ CanonicalBytes = (*MetadataKey)(nil)

type TransactionKey struct {
	LedgerID uint32
	ID       uint64
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerID BE 4B]\x02[txID (8 bytes)].
func (tk TransactionKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, tk.LedgerID)
	dst = append(dst, dal.CanonicalKeySepTransaction)

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
	if len(d) < 4+1+8 {
		return fmt.Errorf("invalid transaction key bytes: expected [ledgerID(4)]\\x%02X[txID(8)]", dal.CanonicalKeySepTransaction)
	}

	tk.LedgerID = binary.BigEndian.Uint32(d[0:4])

	if d[4] != dal.CanonicalKeySepTransaction {
		return fmt.Errorf("invalid transaction key bytes: expected separator 0x%02X, got 0x%02x", dal.CanonicalKeySepTransaction, d[4])
	}

	tk.ID = binary.BigEndian.Uint64(d[5:13])

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
	LedgerID  uint32
	Reference string
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerID BE 4B][reference].
func (trk TransactionReferenceKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, trk.LedgerID)
	dst = append(dst, trk.Reference...)

	return dst
}

// Bytes returns a canonical byte representation of the transaction reference key.
func (trk TransactionReferenceKey) Bytes() []byte {
	return trk.AppendBytes(nil)
}

// Unmarshal parses canonical bytes into the TransactionReferenceKey.
func (trk *TransactionReferenceKey) Unmarshal(d []byte) error {
	if len(d) < 4 {
		return errors.New("invalid transaction reference key bytes: too short for ledger ID")
	}

	trk.LedgerID = binary.BigEndian.Uint32(d[0:4])
	trk.Reference = string(d[4:])

	return nil
}

var _ CanonicalBytes = (*TransactionReferenceKey)(nil)

// LedgerKey identifies a ledger by name. Used as the attribute key for
// LedgerInfo and Boundaries (keyed by name for name-based lookups).
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
	LedgerID uint32
	Key      string
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerID BE 4B]\x01[key].
func (lmk LedgerMetadataKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, lmk.LedgerID)
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
	if len(d) < 5 {
		return errors.New("invalid ledger metadata key bytes: too short")
	}

	lmk.LedgerID = binary.BigEndian.Uint32(d[0:4])

	if d[4] != 0x01 {
		return fmt.Errorf("invalid ledger metadata key bytes: expected separator 0x01, got 0x%02x", d[4])
	}

	lmk.Key = string(d[5:])

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
	LedgerID uint32
	Name     string
}

func (k PreparedQueryKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, k.LedgerID)
	dst = append(dst, k.Name...)

	return dst
}

func (k PreparedQueryKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// NumscriptVersionKey uniquely identifies a numscript by ledger and name for version tracking.
type NumscriptVersionKey struct {
	LedgerID uint32
	Name     string
}

func (k NumscriptVersionKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, k.LedgerID)
	dst = append(dst, k.Name...)

	return dst
}

func (k NumscriptVersionKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// NumscriptEntryKey uniquely identifies a specific numscript version entry scoped to a ledger.
type NumscriptEntryKey struct {
	LedgerID uint32
	Name     string
	Version  string
}

func (k NumscriptEntryKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerID(dst, k.LedgerID)
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
