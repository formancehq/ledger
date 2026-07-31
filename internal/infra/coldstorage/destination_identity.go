package coldstorage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
)

const destinationIdentityVersion = "cold-storage-destination-v1"

// DestinationIdentified is the non-secret physical binding companion required
// by consumers that persist references outside ColdStorage itself.
type DestinationIdentified interface {
	DestinationIdentity() (string, error)
}

// IdentifiedStorage combines ordinary archive I/O with a stable physical
// destination identity. It intentionally does not grant ObjectCatalog delete
// authority.
type IdentifiedStorage interface {
	ColdStorage
	DestinationIdentified
}

func (f *FilesystemStorage) DestinationIdentity() (string, error) {
	if f == nil || strings.TrimSpace(f.basePath) == "" {
		return "", errors.New("filesystem cold storage base path is required for destination identity")
	}
	absolute, err := filepath.Abs(filepath.Clean(f.basePath))
	if err != nil {
		return "", err
	}

	return hashDestinationIdentity("filesystem", absolute), nil
}

func hashDestinationIdentity(parts ...string) string {
	payload := destinationIdentityVersion + "\x00" + strings.Join(parts, "\x00")
	digest := sha256.Sum256([]byte(payload))

	return destinationIdentityVersion + ":" + hex.EncodeToString(digest[:])
}
