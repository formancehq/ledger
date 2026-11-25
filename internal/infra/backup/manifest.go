package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Manifest describes the published checkpoint. Exports and ClusterPolicy are
// retained only to decode and reject legacy backup artifacts during restore.
type Manifest struct {
	Checkpoint *CheckpointManifest `json:"checkpoint"`
	Exports    []ExportSegment     `json:"exports"`
	// ClusterPolicy is a legacy out-of-band policy override. Current checkpoints
	// include the policy directly in Pebble and do not write this field.
	ClusterPolicy *ClusterPolicySnapshot `json:"clusterPolicy,omitempty"`
}

// ClusterPolicySnapshot is the legacy policy-only backup representation.
type ClusterPolicySnapshot struct {
	AppliedIndex        uint64 `json:"appliedIndex"`
	Data                []byte `json:"data"` // deterministic ClusterPolicy protobuf bytes
	ConfigurationAbsent bool   `json:"-"`
}

type controlStateSnapshotData struct {
	Version             uint32 `json:"version"`
	Policy              []byte `json:"policy"`
	ConfigurationAbsent bool   `json:"configurationAbsent,omitempty"`
}

func (snapshot ClusterPolicySnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		AppliedIndex uint64                   `json:"appliedIndex"`
		Data         controlStateSnapshotData `json:"data"`
	}{AppliedIndex: snapshot.AppliedIndex, Data: controlStateSnapshotData{Version: 1, Policy: snapshot.Data, ConfigurationAbsent: snapshot.ConfigurationAbsent}})
}

func (snapshot *ClusterPolicySnapshot) UnmarshalJSON(data []byte) error {
	var wire struct {
		AppliedIndex uint64          `json:"appliedIndex"`
		Data         json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	snapshot.AppliedIndex = wire.AppliedIndex
	if len(wire.Data) != 0 && wire.Data[0] == '{' {
		var payload controlStateSnapshotData
		if err := json.Unmarshal(wire.Data, &payload); err != nil {
			return err
		}
		if payload.Version != 1 {
			return fmt.Errorf("unsupported control-state snapshot version %d", payload.Version)
		}
		snapshot.Data = payload.Policy
		snapshot.ConfigurationAbsent = payload.ConfigurationAbsent

		return nil
	}
	snapshot.ConfigurationAbsent = false

	return json.Unmarshal(wire.Data, &snapshot.Data)
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

// ErrCheckpointFileCorrupted is returned by CheckpointFile.CopyVerified when
// the downloaded bytes do not match the manifest entry.
var ErrCheckpointFileCorrupted = errors.New("checkpoint file does not match its manifest entry")

// CopyVerified streams src into dst and checks the bytes against the entry:
// exactly Size bytes whose sha256 equals the hash CheckpointFileKey embedded in
// Key. It reads at most Size+1 bytes, so an oversized object is rejected
// without being copied in full. On a mismatch dst holds partial or wrong bytes;
// the caller must discard it. It returns the number of bytes written to dst.
//
// This detects an object that was damaged or replaced on storage. It does not
// authenticate the manifest itself: a manifest rewritten together with its
// objects passes.
func (cf CheckpointFile) CopyVerified(dst io.Writer, src io.Reader) (int64, error) {
	want, ok := checkpointKeyHash(cf.Key)
	if !ok {
		return 0, fmt.Errorf("%w: key %q carries no sha256", ErrCheckpointFileCorrupted, cf.Key)
	}

	if cf.Size < 0 {
		return 0, fmt.Errorf("%w: negative size %d", ErrCheckpointFileCorrupted, cf.Size)
	}

	h := sha256.New()

	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(src, cf.Size+1))
	if err != nil {
		return n, err
	}

	if n > cf.Size {
		return n, fmt.Errorf("%w: size mismatch: object is larger than %d bytes", ErrCheckpointFileCorrupted, cf.Size)
	}

	if n < cf.Size {
		return n, fmt.Errorf("%w: size mismatch: got %d bytes, want %d", ErrCheckpointFileCorrupted, n, cf.Size)
	}

	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		return n, fmt.Errorf("%w: sha256 mismatch: got %s, want %s", ErrCheckpointFileCorrupted, got, want)
	}

	return n, nil
}

// ExportSegment describes a legacy incremental export segment stored on S3.
type ExportSegment struct {
	Type     string `json:"type"` // "log", "audit", "auditItem", or "appliedProposal"
	StartSeq uint64 `json:"startSeq"`
	EndSeq   uint64 `json:"endSeq"`
	Key      string `json:"key"` // S3 object key
	Size     int64  `json:"size"`
}

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

// DecodeManifest decodes already-read manifest bytes. The gRPC restore path
// and offline bootstrap command use this helper after their own size guards.
func DecodeManifest(data []byte) (*Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}

	return &manifest, nil
}

// ValidateCheckpointOnlyManifest rejects backup formats that require the
// removed export replay path. Both restore entry points call this before
// creating or activating staging state.
func ValidateCheckpointOnlyManifest(manifest *Manifest) error {
	if len(manifest.Exports) != 0 || manifest.ClusterPolicy != nil {
		return errors.New("incremental backup manifests are no longer supported; take a new checkpoint backup")
	}
	if manifest.Checkpoint == nil || len(manifest.Checkpoint.Files) == 0 {
		return errors.New("backup manifest contains no checkpoint files")
	}

	return nil
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
	return bucketID + "/manifest.json"
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
