package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Timestamp         string           `json:"timestamp"`
	LastAppliedIndex  uint64           `json:"lastAppliedIndex"`
	LastLogSequence   uint64           `json:"lastLogSequence"`
	LastAuditSequence uint64           `json:"lastAuditSequence"`
	Files             map[string]int64 `json:"files"` // filename → size in bytes
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

// ReadManifest reads and decodes a manifest from storage.
func ReadManifest(ctx context.Context, storage Storage, key string) (*Manifest, error) {
	reader, err := storage.GetFile(ctx, key)
	if err != nil {
		return nil, err
	}

	defer func() { _ = reader.Close() }()

	var manifest Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}

	return &manifest, nil
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
