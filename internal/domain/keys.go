package domain

import (
	"bytes"
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

// appendLedgerName appends the ledger name as a fixed-size, zero-padded block.
// copy() truncates silently if name > dal.LedgerNameFixedSize, so callers MUST
// validate the length upstream (admission layer) to avoid collisions.
func appendLedgerName(dst []byte, name string) []byte {
	var pad [dal.LedgerNameFixedSize]byte

	copy(pad[:], name)

	return append(dst, pad[:]...)
}

// readLedgerName extracts the ledger name from the first dal.LedgerNameFixedSize
// bytes of src and returns the remainder for further parsing. The trailing
// zero padding is trimmed.
func readLedgerName(src []byte) (string, []byte, error) {
	if len(src) < dal.LedgerNameFixedSize {
		return "", nil, fmt.Errorf("invalid canonical key: expected at least %d bytes for ledger name, got %d", dal.LedgerNameFixedSize, len(src))
	}

	raw := src[:dal.LedgerNameFixedSize]

	end := bytes.IndexByte(raw, 0)
	if end < 0 {
		end = dal.LedgerNameFixedSize
	}

	return string(raw[:end]), src[dal.LedgerNameFixedSize:], nil
}

type AccountKey struct {
	LedgerName string
	Account    string
}

// AccountAssetKey identifies a single Pebble volume cell within a ledger by
// its (account, asset) coordinates. It is the ledger-local subset of
// VolumeKey (no LedgerName), used as map key in code that already scopes
// data per ledger — exclusion sets in the index builder and the integrity
// checker, transient/purged dedup helpers, etc. Keep this type plain (no
// derived fields) so it is a value-equal map key.
type AccountAssetKey struct {
	Account string
	Asset   string
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

// NewVolumeKey creates a VolumeKey with pre-parsed AssetBase and AssetPrecision,
// avoiding re-parsing on every AppendBytes call in the hot path.
func NewVolumeKey(ledgerName, account, asset string) VolumeKey {
	base, precision := ParseAssetPrecision(asset)

	return VolumeKey{
		AccountKey:     AccountKey{LedgerName: ledgerName, Account: account},
		Asset:          asset,
		AssetBase:      base,
		AssetPrecision: precision,
	}
}

// AppendBytes appends the canonical byte representation to dst and returns the
// extended slice. Format: [ledgerName padded 64B][account][sep][asset_base][precision_byte].
func (bk VolumeKey) AppendBytes(dst []byte) []byte {
	base := bk.AssetBase
	precision := bk.AssetPrecision

	// Fallback: if constructed via struct literal without decomposed fields.
	if base == "" && bk.Asset != "" {
		base, precision = ParseAssetPrecision(bk.Asset)
	}

	dst = appendLedgerName(dst, bk.LedgerName)
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
	name, rest, err := readLedgerName(d)
	if err != nil {
		return fmt.Errorf("invalid balance key bytes: %w", err)
	}

	bk.LedgerName = name

	// The remaining bytes are [account][sep_volume=0x00][asset_base][precision_byte].
	// CanonicalKeySepVolume is 0x00; account and asset_base are byte strings
	// without embedded zero bytes (validated upstream).
	before, after, ok := bytes.Cut(rest, []byte{dal.CanonicalKeySepVolume})
	if !ok {
		return errors.New("invalid balance key bytes: missing account/asset separator")
	}

	bk.Account = string(before)

	assetPart := after
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
// Format: [ledgerName padded 64B][account]\x01[key].
func (mk MetadataKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, mk.LedgerName)
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
	name, rest, err := readLedgerName(d)
	if err != nil {
		return fmt.Errorf("invalid metadata key bytes: %w", err)
	}

	mk.LedgerName = name

	before, after, ok := bytes.Cut(rest, []byte{dal.CanonicalKeySepMetadata})
	if !ok {
		return errors.New("invalid metadata key bytes: missing account/key separator")
	}

	mk.Account = string(before)
	mk.Key = string(after)

	return nil
}

var _ CanonicalBytes = (*MetadataKey)(nil)

type TransactionKey struct {
	LedgerName string
	ID         uint64
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerName padded 64B]\x02[txID (8 bytes)].
func (tk TransactionKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, tk.LedgerName)
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
	name, rest, err := readLedgerName(d)
	if err != nil {
		return fmt.Errorf("invalid transaction key bytes: %w", err)
	}

	tk.LedgerName = name

	if len(rest) < 1+8 {
		return fmt.Errorf("invalid transaction key bytes: expected separator + 8B txID after ledger name, got %d bytes", len(rest))
	}

	if rest[0] != dal.CanonicalKeySepTransaction {
		return fmt.Errorf("invalid transaction key bytes: expected separator 0x%02X, got 0x%02x", dal.CanonicalKeySepTransaction, rest[0])
	}

	tk.ID = binary.BigEndian.Uint64(rest[1:9])

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
	LedgerName string
	Reference  string
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerName padded 64B][reference].
func (trk TransactionReferenceKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, trk.LedgerName)
	dst = append(dst, trk.Reference...)

	return dst
}

// Bytes returns a canonical byte representation of the transaction reference key.
func (trk TransactionReferenceKey) Bytes() []byte {
	return trk.AppendBytes(nil)
}

// Unmarshal parses canonical bytes into the TransactionReferenceKey.
func (trk *TransactionReferenceKey) Unmarshal(d []byte) error {
	name, rest, err := readLedgerName(d)
	if err != nil {
		return fmt.Errorf("invalid transaction reference key bytes: %w", err)
	}

	trk.LedgerName = name
	trk.Reference = string(rest)

	return nil
}

var _ CanonicalBytes = (*TransactionReferenceKey)(nil)

// LedgerScopedPrefix returns the fixed-width canonical prefix shared by every
// ledger-scoped attribute key (the ledger name padded to
// dal.LedgerNameFixedSize). Scans over ledger-scoped keys must use this, not
// the raw name: an unpadded prefix also matches every ledger whose name
// extends it ("pay" would match "payments").
func LedgerScopedPrefix(name string) []byte {
	return appendLedgerName(nil, name)
}

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
	LedgerName string
	Key        string
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerName padded 64B]\x01[key].
func (lmk LedgerMetadataKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, lmk.LedgerName)
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
	name, rest, err := readLedgerName(d)
	if err != nil {
		return fmt.Errorf("invalid ledger metadata key bytes: %w", err)
	}

	lmk.LedgerName = name

	if len(rest) < 1 {
		return errors.New("invalid ledger metadata key bytes: missing separator")
	}

	if rest[0] != 0x01 {
		return fmt.Errorf("invalid ledger metadata key bytes: expected separator 0x01, got 0x%02x", rest[0])
	}

	lmk.Key = string(rest[1:])

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
	LedgerName string
	Name       string
}

func (k PreparedQueryKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, k.LedgerName)
	dst = append(dst, k.Name...)

	return dst
}

func (k PreparedQueryKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// NumscriptVersionKey uniquely identifies a numscript by ledger and name for version tracking.
type NumscriptVersionKey struct {
	LedgerName string
	Name       string
}

func (k NumscriptVersionKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, k.LedgerName)
	dst = append(dst, k.Name...)

	return dst
}

func (k NumscriptVersionKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// NumscriptEntryKey uniquely identifies a specific numscript version entry scoped to a ledger.
type NumscriptEntryKey struct {
	LedgerName string
	Name       string
	Version    string
}

func (k NumscriptEntryKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, k.LedgerName)
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

// IndexKey identifies an entry in the bucket-scoped index registry.
//
// Scope:
//   - LedgerName == "" → bucket-scoped (e.g. audit indexes), single entry
//     across the bucket. Empty names are rejected by ledger validation
//     so the sentinel cannot collide with a real ledger.
//   - LedgerName != "" → ledger-scoped (tx / account / log / metadata indexes).
//
// Canonical is the result of indexes.Canonical(IndexID), which produces a
// deterministic string encoding of the IndexID oneof.
//
// IndexKey uses LedgerName (not LedgerID) so admission can predict the cache
// key without resolving the ledger ID — required when CreateLedger and
// CreateIndex ride in the same proposal: the ledger ID is only assigned at
// FSM apply time, so any LedgerID-based prediction at admission would mis-
// declare the preload and bubble up as an *ErrCoverageMiss on apply.
type IndexKey struct {
	LedgerName string
	Canonical  string
}

// AppendBytes appends the canonical byte representation to dst.
// Format: [ledgerName padded 64B][canonical bytes].
func (k IndexKey) AppendBytes(dst []byte) []byte {
	dst = appendLedgerName(dst, k.LedgerName)
	dst = append(dst, k.Canonical...)

	return dst
}

// Bytes returns a canonical byte representation of the index key.
func (k IndexKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

// Unmarshal parses canonical bytes into the IndexKey.
func (k *IndexKey) Unmarshal(d []byte) error {
	name, rest, err := readLedgerName(d)
	if err != nil {
		return fmt.Errorf("invalid index key bytes: %w", err)
	}

	k.LedgerName = name
	k.Canonical = string(rest)

	return nil
}

var _ CanonicalBytes = (*IndexKey)(nil)
