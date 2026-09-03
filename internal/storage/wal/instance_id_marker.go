package wal

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// WriteInstanceID creates the INSTANCE_ID marker in dataDir containing the
// given 16 raw bytes. Fails if the file already exists — instance IDs are
// immutable for the lifetime of the WAL directory; overwriting would defeat
// the discrimination guarantee.
func WriteInstanceID(dataDir string, id []byte) error {
	if dataDir == "" {
		return errors.New("WriteInstanceID: empty dataDir")
	}

	if len(id) != InstanceIDLen {
		return fmt.Errorf("WriteInstanceID: instance id must be %d bytes, got %d", InstanceIDLen, len(id))
	}

	markerPath := filepath.Join(dataDir, InstanceIDMarkerFile)

	f, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("creating instance-id marker: %w", err)
	}

	if _, err := f.Write(id); err != nil {
		_ = f.Close()

		return fmt.Errorf("writing instance-id marker: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return fmt.Errorf("syncing instance-id marker: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing instance-id marker: %w", err)
	}

	return nil
}

// ReadInstanceID reads the INSTANCE_ID marker from dataDir. Returns
// (nil, nil) when the marker is absent so EnsureInstanceID can create it on
// first boot. A present-but-corrupt marker (wrong size) is a fatal error.
func ReadInstanceID(dataDir string) ([]byte, error) {
	if dataDir == "" {
		return nil, errors.New("ReadInstanceID: empty dataDir")
	}

	data, err := os.ReadFile(filepath.Join(dataDir, InstanceIDMarkerFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading instance-id marker: %w", err)
	}

	if len(data) != InstanceIDLen {
		return nil, fmt.Errorf("instance-id marker has unexpected length %d (want %d)", len(data), InstanceIDLen)
	}

	return data, nil
}

// GenerateInstanceID returns a fresh 16-byte random identifier suitable for
// persisting via WriteInstanceID. Uses crypto/rand so the ID is unique with
// overwhelming probability across the cluster's lifetime.
func GenerateInstanceID() ([]byte, error) {
	id := make([]byte, InstanceIDLen)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("generating instance id: %w", err)
	}

	return id, nil
}

// existingWALIdentityArtifact returns the first durable artifact proving that
// this directory has already entered the WAL lifecycle. INSTANCE_ID is
// established before any of these artifacts can be created, so finding one
// without the identity marker means the marker was lost or deleted. Generating
// a replacement in that state would split this peer's identity from the one
// already replicated by the cluster.
func existingWALIdentityArtifact(dataDir string) (string, error) {
	for _, artifact := range []string{
		ClusterJoinedMarkerFile,
		walCreationCompletedFile,
		etcdWalDir,
		snapDir,
	} {
		_, err := os.Stat(filepath.Join(dataDir, artifact))
		if err == nil {
			return artifact, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("checking WAL identity artifact %q: %w", artifact, err)
		}
	}

	return "", nil
}

// EnsureInstanceID reads the INSTANCE_ID marker from dataDir and returns its
// value; if the marker is absent (first boot on a fresh WAL) it generates a
// new UUID, persists it, then returns it. Creates dataDir if it doesn't
// already exist so it can be called before any other WAL machinery.
//
// This is the one call site that establishes a peer's identity for the
// lifetime of its (pod, PVC) incarnation. All later boots of the same PVC
// read the same value from disk; a fresh PVC gets a fresh value. If any WAL
// lifecycle artifact already exists while INSTANCE_ID is absent, startup
// fails closed instead of rotating the identity of existing consensus state.
func EnsureInstanceID(dataDir string) ([]byte, error) {
	if dataDir == "" {
		return nil, errors.New("EnsureInstanceID: empty dataDir")
	}

	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating data dir for instance-id marker: %w", err)
	}

	existing, err := ReadInstanceID(dataDir)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		return existing, nil
	}

	artifact, err := existingWALIdentityArtifact(dataDir)
	if err != nil {
		return nil, err
	}
	if artifact != "" {
		return nil, fmt.Errorf(
			"INSTANCE_ID marker is missing while WAL artifact %q exists; refusing to generate a new identity for existing consensus state (manual intervention required)",
			artifact,
		)
	}

	fresh, err := GenerateInstanceID()
	if err != nil {
		return nil, err
	}

	if err := WriteInstanceID(dataDir, fresh); err != nil {
		return nil, err
	}

	return fresh, nil
}
