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
	"slices"
)

// Manifest describes the current state of a backup, combining a full checkpoint
// with incremental log/audit exports.
type Manifest struct {
	Checkpoint *CheckpointManifest `json:"checkpoint"`
	Exports    []ExportSegment     `json:"exports"`
	// ClusterPolicy is operational state captured from the same snapshot as
	// the latest exports. Absent on released manifests: replay their old logs.
	ClusterPolicy *ClusterPolicySnapshot `json:"clusterPolicy,omitempty"`
}

// ClusterPolicySnapshot is independent of business/audit sequence numbers: a
// policy-only incremental backup can update it without exporting history.
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
	ReadIndex         *ReadIndexManifest        `json:"readIndex,omitempty"`
}

// ReadIndexVersion identifies the portable seed format understood by this release.
const ReadIndexVersion = 1

// ReadIndexManifest is an optional physical index captured before the primary.
// Each store has its own local names; both reference immutable objects in data/.
// Released primary-only manifests omit this field.
type ReadIndexManifest struct {
	Version         uint32                    `json:"version"`
	InventorySHA256 string                    `json:"inventorySHA256"` // binds this v1 inventory to its paired primary
	Files           map[string]CheckpointFile `json:"files"`
	opaque          json.RawMessage           // an unknown version survives incremental manifest rewrites
}

// UnmarshalJSON interprets only the version discriminator until the reader
// knows the seed format. A future inventory need not use v1's file map.
func (m *ReadIndexManifest) UnmarshalJSON(data []byte) error {
	var header struct {
		Version uint32 `json:"version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	if header.Version != ReadIndexVersion {
		*m = ReadIndexManifest{Version: header.Version, opaque: bytes.Clone(data)}

		return nil
	}
	type v1 ReadIndexManifest
	var decoded v1
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*m = ReadIndexManifest(decoded)

	return nil
}

// MarshalJSON republishes a future seed without projecting it down to v1.
func (m ReadIndexManifest) MarshalJSON() ([]byte, error) {
	if m.opaque != nil {
		return m.opaque, nil
	}
	type v1 ReadIndexManifest

	return json.Marshal(v1(m))
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

// ErrNoFullCheckpoint is returned by RunIncrementalBackup when the destination
// manifest has no full checkpoint (Checkpoint == nil): either the destination is
// empty/fresh, or it holds an export-only manifest. An incremental backup is
// only meaningful on top of a full checkpoint — the checkpoint is what carries
// the Global-zone persisted config, last-applied index, and timestamp that
// restore needs to build a valid store. Publishing export-only segments against
// a checkpoint-less manifest would produce an artifact restore cannot turn into
// a store, so the incremental path fails fast and asks for a full backup first.
var ErrNoFullCheckpoint = errors.New(
	"backup: incremental backup requires an existing full checkpoint; " +
		"none found at the destination — run a full backup before any incremental backup")

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

// Restore bounds. Both inventories share the byte budget; each Pebble store
// independently owns up to MaxRestoreManifestFiles names.
const (
	MaxRestoreManifestBytes = 32 << 20
	MaxRestoreManifestFiles = 100_000
)

// WriteManifest encodes and writes a manifest to storage. The size check
// occurs before PutFile for both full and incremental backups, so if the
// added read-index inventory would exceed the restore contract, the old chain
// remains published. Legacy primary-only writers keep their existing limits.
func WriteManifest(ctx context.Context, storage Storage, key string, manifest *Manifest) error {
	data, err := marshalManifest(manifest, MaxRestoreManifestFiles, MaxRestoreManifestBytes)
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	return storage.PutFile(ctx, key, bytes.NewReader(data), int64(len(data)))
}

func marshalManifest(manifest *Manifest, maxFiles, maxBytes int) ([]byte, error) {
	if manifest == nil || manifest.Checkpoint == nil || manifest.Checkpoint.ReadIndex == nil {
		return json.MarshalIndent(manifest, "", "  ")
	}
	cp := manifest.Checkpoint
	if len(cp.Files) > maxFiles || len(cp.ReadIndex.Files) > maxFiles {
		return nil, fmt.Errorf("checkpoint inventories have %d primary and %d read index files; restore limit is %d per store", len(cp.Files), len(cp.ReadIndex.Files), maxFiles)
	}
	if cp.ReadIndex.Version == ReadIndexVersion {
		if err := cp.verifyReadIndexBinding(); err != nil {
			return nil, err
		}
	}
	// Each incremental republishes both inventories. Compact JSON reduces
	// transfer size and leaves more of the bounded manifest for export entries.
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if len(data) > maxBytes {
		return nil, fmt.Errorf("encoded manifest is %d bytes; restore limit is %d", len(data), maxBytes)
	}

	return data, nil
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
