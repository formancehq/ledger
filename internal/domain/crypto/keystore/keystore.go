package keystore

import (
	"crypto/ed25519"
	"slices"
	"sync"
)

// KeyStore holds Ed25519 public keys for signature verification.
// It is thread-safe and updated dynamically by the Raft FSM when signing key
// management orders are applied.
// It also tracks parent-child relationships between keys for cascade revocation.
type KeyStore struct {
	mu         sync.RWMutex
	publicKeys map[string]ed25519.PublicKey
	parents    map[string]string // keyID -> parentKeyID
	// undecodableRows counts persisted signing-key rows recovery had to skip
	// because they did not decode. They hold no usable key, but their existence
	// proves the cluster is NOT a fresh one — which is a different question from
	// HasKeys, and the one the unsigned-bootstrap gate must ask.
	undecodableRows int
}

// NewKeyStore creates a new empty KeyStore.
func NewKeyStore() *KeyStore {
	return &KeyStore{
		publicKeys: make(map[string]ed25519.PublicKey),
		parents:    make(map[string]string),
	}
}

// GetPublicKey returns the public key for the given key ID, or nil if not found.
func (ks *KeyStore) GetPublicKey(keyID string) ed25519.PublicKey {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	return ks.publicKeys[keyID]
}

// AddPublicKey registers a public key with the given key ID and optional parent.
func (ks *KeyStore) AddPublicKey(keyID string, pubKey ed25519.PublicKey, parentKeyID string) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	ks.publicKeys[keyID] = pubKey
	if parentKeyID != "" {
		ks.parents[keyID] = parentKeyID
	}
}

// RemovePublicKey removes the public key with the given key ID.
func (ks *KeyStore) RemovePublicKey(keyID string) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	delete(ks.publicKeys, keyID)
	delete(ks.parents, keyID)
}

// GetChildren returns all key IDs that have the given keyID as their parent.
func (ks *KeyStore) GetChildren(keyID string) []string {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	var children []string

	for childID, parentID := range ks.parents {
		if parentID == keyID {
			children = append(children, childID)
		}
	}

	// Sort for determinism: this feeds signing-key revocation cascades whose
	// RevokedSigningKeyLog.cascadedKeyIds (and BFS traversal order) are
	// chain-bound FSM output. A raw map range would let two replicas emit
	// different orderings for identical state (invariant #2). See EN-1521.
	slices.Sort(children)

	return children
}

// HasKeys returns true if at least one public key is registered.
func (ks *KeyStore) HasKeys() bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	return len(ks.publicKeys) > 0
}

// MarkUndecodableRows records how many persisted signing-key rows failed to
// decode during the recovery that is repopulating this store. Called after
// Reset, which clears the previous count.
func (ks *KeyStore) MarkUndecodableRows(count int) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	ks.undecodableRows = count
}

// HasUndecodableRows reports whether recovery skipped any signing-key row it
// could not decode.
//
// This is what distinguishes "no keys because this cluster never registered one"
// from "no keys because the rows on disk are corrupt", a distinction HasKeys
// cannot make and the unsigned-bootstrap exception depends on.
func (ks *KeyStore) HasUndecodableRows() bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	return ks.undecodableRows > 0
}

// Reset clears all keys. Used during snapshot restore.
func (ks *KeyStore) Reset() {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	ks.publicKeys = make(map[string]ed25519.PublicKey)
	ks.parents = make(map[string]string)
	// Cleared with the keys: the count describes the rows the NEXT load observes,
	// so carrying a stale one over would keep the bootstrap gate shut on a store
	// whose corrupt rows are gone.
	ks.undecodableRows = 0
}
