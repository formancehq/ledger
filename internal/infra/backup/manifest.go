package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
)

// Manifest describes the current state of a backup, combining a full checkpoint
// with incremental log/audit exports.
type Manifest struct {
	Checkpoint *CheckpointManifest `json:"checkpoint"`
	Exports    []ExportSegment     `json:"exports"`
}

// CheckpointManifest describes a full Pebble checkpoint upload.
type CheckpointManifest struct {
	Timestamp         string                    `json:"timestamp"`
	LastAppliedIndex  uint64                    `json:"lastAppliedIndex"`
	LastLogSequence   uint64                    `json:"lastLogSequence"`
	LastAuditSequence uint64                    `json:"lastAuditSequence"`
	Files             map[string]CheckpointFile `json:"files"` // local filename → stored object
}

// CheckpointFile records where one checkpoint file lives on storage and its
// size. The storage Key is content-addressed (it embeds a hash of the file
// bytes), so a file whose content changes between checkpoints — most notably
// Pebble's MANIFEST-NNNNNN, which keeps the same local name but grows — is
// uploaded under a NEW key rather than overwriting the object the currently
// published manifest still references. This makes every object a published
// manifest points at immutable: a crash between upload and the manifest swap
// can never corrupt the previous backup (EN-1055). Identical content across
// checkpoints yields the same Key, so unchanged SSTs are naturally deduped and
// skipped on re-upload.
type CheckpointFile struct {
	Size int64  `json:"size"`
	Key  string `json:"key"`
}

// ExportSegment describes a single incremental export segment stored on S3.
type ExportSegment struct {
	Type     string `json:"type"` // "log", "audit", "auditItem", or "appliedProposal"
	StartSeq uint64 `json:"startSeq"`
	EndSeq   uint64 `json:"endSeq"`
	Key      string `json:"key"` // S3 object key
	Size     int64  `json:"size"`
}

// LastExportLogSequence returns the highest exported log sequence,
// checking exports first then falling back to the checkpoint.
func (m *Manifest) LastExportLogSequence() uint64 {
	for _, v := range slices.Backward(m.Exports) {
		if v.Type == "log" {
			return v.EndSeq
		}
	}

	if m.Checkpoint != nil {
		return m.Checkpoint.LastLogSequence
	}

	return 0
}

// LastExportAuditSequence returns the highest exported audit sequence,
// checking exports first then falling back to the checkpoint.
func (m *Manifest) LastExportAuditSequence() uint64 {
	for _, v := range slices.Backward(m.Exports) {
		if v.Type == "audit" {
			return v.EndSeq
		}
	}

	if m.Checkpoint != nil {
		return m.Checkpoint.LastAuditSequence
	}

	return 0
}

// ErrLegacyManifestFormat is returned by ReadManifest when the stored manifest
// uses the pre-content-addressing schema (checkpoint.files encoded as
// filename→size numbers instead of filename→{size,key} objects). Such backups
// were written by an older binary and store their checkpoint objects under
// bare data/<filename> keys with no content hash; the current restore path
// resolves objects by the content-addressed key recorded in the manifest and
// cannot read them. This is a deliberate pre-GA break — the fix is to retake
// the backup with the current binary, not to migrate the old manifest in place.
var ErrLegacyManifestFormat = errors.New(
	"backup: manifest uses the legacy pre-content-addressing format " +
		"(checkpoint.files as sizes, not {size,key}); this backup was written by an " +
		"older binary and must be retaken with the current version before it can be restored")

// ReadManifest reads and decodes a manifest from storage.
func ReadManifest(ctx context.Context, storage Storage, key string) (*Manifest, error) {
	reader, err := storage.GetFile(ctx, key)
	if err != nil {
		return nil, err
	}

	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	return DecodeManifest(data)
}

// DecodeManifest decodes already-read manifest bytes, translating a decode
// failure caused by the legacy pre-content-addressing schema into the
// actionable ErrLegacyManifestFormat. Callers that read the manifest bytes
// themselves (e.g. the gRPC restore path and the offline bootstrap command,
// which each apply their own size guards and progress reporting around the
// read) MUST decode through this helper rather than a bare json.Unmarshal so
// legacy detection stays consistent everywhere.
func DecodeManifest(data []byte) (*Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		// A legacy manifest (checkpoint.files as filename→number) fails to
		// decode into the current filename→{size,key} shape. Detect that exact
		// shape and surface a clear, actionable error instead of the raw
		// "cannot unmarshal number into Go struct field" — the operator needs
		// to know the backup must be retaken, not that the JSON is malformed.
		if isLegacyManifest(data) {
			return nil, ErrLegacyManifestFormat
		}

		return nil, fmt.Errorf("decoding manifest: %w", err)
	}

	return &manifest, nil
}

// isLegacyManifest reports whether the manifest bytes use the pre-content-
// addressing schema, i.e. checkpoint.files maps filenames to plain numbers
// (sizes) rather than to {size,key} objects. It parses loosely into
// interface{} so it never itself fails on the shape difference.
func isLegacyManifest(data []byte) bool {
	var loose struct {
		Checkpoint *struct {
			Files map[string]json.RawMessage `json:"files"`
		} `json:"checkpoint"`
	}

	if err := json.Unmarshal(data, &loose); err != nil || loose.Checkpoint == nil {
		return false
	}

	for _, raw := range loose.Checkpoint.Files {
		trimmed := bytes.TrimSpace(raw)
		// A legacy entry is a bare JSON number; the current entry is an object.
		if len(trimmed) > 0 && trimmed[0] != '{' {
			return true
		}
	}

	return false
}

// WriteManifest encodes and writes a manifest to storage.
func WriteManifest(ctx context.Context, storage Storage, key string, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	return storage.PutFile(ctx, key, bytes.NewReader(data), int64(len(data)))
}

// ManifestKey returns the S3 key for the manifest file.
func ManifestKey(bucketID string) string {
	return bucketID + "/backups/manifest.json"
}

// ReadManifestOrEmpty reads a manifest from storage, returning an empty
// manifest if the file does not exist. Any other error (decode, permission,
// network) is returned rather than masked.
func ReadManifestOrEmpty(ctx context.Context, logger interface{ Infof(string, ...any) }, storage Storage, key string) (*Manifest, error) {
	manifest, err := ReadManifest(ctx, storage, key)
	if errors.Is(err, ErrFileNotFound) {
		logger.Infof("No existing manifest found, starting fresh")

		return &Manifest{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("reading existing manifest %s: %w", key, err)
	}

	return manifest, nil
}
