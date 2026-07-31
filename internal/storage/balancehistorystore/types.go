// Package balancehistorystore stores the rebuildable monetary history used by
// point-in-time balance queries. It is a peer secondary store: the FSM never
// reads or writes it.
package balancehistorystore

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

const (
	formatVersion  = 1
	reducerVersion = 1
)

// Axis selects the timestamp used to fold monetary effects.
type Axis uint8

const (
	AxisEffective Axis = 1
	AxisInsertion Axis = 2
)

func (a Axis) valid() bool {
	return a == AxisEffective || a == AxisInsertion
}

func validateEffect(e balancehistory.Effect) error {
	switch {
	case e.LedgerID == 0:
		return errors.New("ledger id is required")
	case e.AuditSequence == 0:
		return errors.New("audit sequence is required")
	case e.LogSequence == 0:
		return errors.New("log sequence is required")
	case e.Account == "":
		return errors.New("account is required")
	case e.AssetBase == "":
		return errors.New("asset base is required")
	case e.Input.IsZero() && e.Output.IsZero():
		return errors.New("effect must change input or output")
	default:
		return nil
	}
}

// Coverage is the consecutive source prefix made visible by a publication.
// Empty-effect publications still advance coverage for failed and
// non-monetary proposals.
type Coverage struct {
	AuditSequence  uint64
	LogSequence    uint64
	AuditHash      []byte
	SourceComplete bool
	// EffectiveFloor and InsertionFloor are reserved compatibility fields.
	// Publish rejects non-zero values until a chain-bound or signed base import
	// can prove the cumulative state introduced with a floor.
	EffectiveFloor uint64
	InsertionFloor uint64
}

// Publication atomically adds one level-zero run and advances source coverage.
type Publication struct {
	Effects      []balancehistory.Effect
	Coverage     Coverage
	ReducerState balancehistory.State
}

// RunRef identifies one immutable logical run inside the history database.
type RunRef struct {
	ID                 uint64        `json:"id"`
	Level              uint32        `json:"level"`
	FirstAuditSequence uint64        `json:"firstAuditSequence"`
	LastAuditSequence  uint64        `json:"lastAuditSequence"`
	MaxLogSequence     uint64        `json:"maxLogSequence"`
	EntryCount         uint64        `json:"entryCount"`
	IdentityCount      uint64        `json:"identityCount"`
	Checksum           [32]byte      `json:"checksum"`
	Archived           bool          `json:"archived,omitempty"`
	ArchiveParts       []ArchivePart `json:"archiveParts,omitempty"`
	LocalRemoved       bool          `json:"localRemoved,omitempty"`
}

// ArchivePart is one bounded, content-addressed key range of an immutable run.
// LowerBound is inclusive and UpperBound is exclusive; an empty UpperBound
// denotes the end of the run.
type ArchivePart struct {
	Ref        balancehistoryarchive.Ref `json:"ref"`
	LowerBound []byte                    `json:"lowerBound"`
	UpperBound []byte                    `json:"upperBound,omitempty"`
}

func cloneRunRef(run RunRef) RunRef {
	run.ArchiveParts = cloneArchiveParts(run.ArchiveParts)

	return run
}

func cloneArchiveParts(parts []ArchivePart) []ArchivePart {
	cloned := append([]ArchivePart(nil), parts...)
	for index := range cloned {
		cloned[index].LowerBound = bytes.Clone(cloned[index].LowerBound)
		cloned[index].UpperBound = bytes.Clone(cloned[index].UpperBound)
	}

	return cloned
}

func archivePartsEqual(left, right []ArchivePart) bool {
	return reflect.DeepEqual(left, right)
}

func runRefsEqual(left, right RunRef) bool {
	return reflect.DeepEqual(left, right)
}

// Manifest is an immutable view descriptor. A View pins both this value and a
// Pebble snapshot, so later publication, compaction, or GC cannot change it.
type Manifest struct {
	Version        uint64 `json:"version"`
	FormatVersion  uint32 `json:"formatVersion"`
	ReducerVersion uint32 `json:"reducerVersion"`
	AuditWatermark uint64 `json:"auditWatermark"`
	LogWatermark   uint64 `json:"logWatermark"`
	SourceComplete bool   `json:"sourceComplete"`
	// EffectiveFloor and InsertionFloor decode the reserved manifest shape but
	// must remain zero until a verifiable base-import authority is configured.
	EffectiveFloor uint64               `json:"effectiveFloor"`
	InsertionFloor uint64               `json:"insertionFloor"`
	AuditHash      []byte               `json:"auditHash,omitempty"`
	LogicalDigest  [32]byte             `json:"logicalDigest"`
	ReducerState   balancehistory.State `json:"reducerState"`
	NextRunID      uint64               `json:"nextRunId"`
	Runs           []RunRef             `json:"runs"`
	Digest         [32]byte             `json:"digest"`
}

func initialManifest() Manifest {
	return Manifest{
		FormatVersion:  formatVersion,
		ReducerVersion: reducerVersion,
		NextRunID:      1,
	}
}

func encodeManifest(manifest Manifest) ([]byte, error) {
	manifest.Digest = [32]byte{}
	unsigned, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshaling unsigned balance history manifest: %w", err)
	}

	manifest.Digest = sha256.Sum256(unsigned)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshaling balance history manifest: %w", err)
	}

	return encoded, nil
}

func decodeManifest(encoded []byte) (Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return Manifest{}, &ErrCorrupt{Detail: fmt.Sprintf("manifest cannot be decoded: %v", err)}
	}

	want := manifest.Digest
	manifest.Digest = [32]byte{}
	unsigned, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, fmt.Errorf("marshaling balance history manifest for verification: %w", err)
	}
	if got := sha256.Sum256(unsigned); got != want {
		return Manifest{}, &ErrCorrupt{Detail: "manifest digest mismatch"}
	}

	manifest.Digest = want
	if manifest.FormatVersion != formatVersion {
		return Manifest{}, &ErrUnsupportedFormat{Found: manifest.FormatVersion, Supported: formatVersion}
	}
	if manifest.ReducerVersion != reducerVersion {
		return Manifest{}, &ErrUnsupportedReducer{Found: manifest.ReducerVersion, Supported: reducerVersion}
	}

	return manifest, nil
}

// Volume is one historical account/asset/color result.
type Volume struct {
	Account        string
	AssetBase      string
	AssetPrecision uint8
	Color          string
	Input          *big.Int
	Output         *big.Int
}

// AssetVolume is one historical ledger-wide asset/color result.
type AssetVolume struct {
	AssetBase      string
	AssetPrecision uint8
	Color          string
	Input          *big.Int
	Output         *big.Int
}
