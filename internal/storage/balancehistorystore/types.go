// Package balancehistorystore stores the rebuildable monetary history used by
// historical balance queries. It is a peer secondary store: the FSM never
// reads or writes it.
package balancehistorystore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

const (
	formatVersion  = 2
	reducerVersion = 1
)

// Temporality selects the timestamp used to fold monetary effects.
type Temporality uint8

const (
	TemporalityEffective Temporality = 1
	TemporalityInsertion Temporality = 2
)

func (a Temporality) valid() bool {
	return a == TemporalityEffective || a == TemporalityInsertion
}

func validateEffect(e balancehistory.Effect) error {
	switch {
	case e.LedgerName == "":
		return errors.New("ledger name is required")
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
}

// Publication atomically adds one level-zero segment and advances source coverage.
type Publication struct {
	Effects      []balancehistory.Effect
	Coverage     Coverage
	ReducerState balancehistory.State
}

// SegmentRef identifies one immutable logical history segment. A segment is
// application-level data stored inside Pebble; it is not a Pebble SSTable.
type SegmentRef struct {
	ID                 uint64 `json:"id"`
	Level              uint32 `json:"level"`
	FirstAuditSequence uint64 `json:"firstAuditSequence"`
	LastAuditSequence  uint64 `json:"lastAuditSequence"`
	MaxLogSequence     uint64 `json:"maxLogSequence"`
	EntryCount         uint64 `json:"entryCount"`
	IdentityCount      uint64 `json:"identityCount"`
}

// Manifest is an immutable view descriptor. A View pins both this value and a
// Pebble snapshot, so later publication, compaction, or GC cannot change it.
type Manifest struct {
	Version        uint64               `json:"version"`
	FormatVersion  uint32               `json:"formatVersion"`
	ReducerVersion uint32               `json:"reducerVersion"`
	AuditWatermark uint64               `json:"auditWatermark"`
	LogWatermark   uint64               `json:"logWatermark"`
	SourceComplete bool                 `json:"sourceComplete"`
	AuditHash      []byte               `json:"auditHash,omitempty"`
	ReducerState   balancehistory.State `json:"reducerState"`
	Ledgers        []string             `json:"ledgers,omitempty"`
	NextSegmentID  uint64               `json:"nextSegmentId"`
	Segments       []SegmentRef         `json:"segments"`
}

func initialManifest() Manifest {
	return Manifest{
		FormatVersion:  formatVersion,
		ReducerVersion: reducerVersion,
		NextSegmentID:  1,
	}
}

func encodeManifest(manifest Manifest) ([]byte, error) {
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
