package keystore

import (
	"crypto/ed25519"
	"sync"
)

// KeyStore holds Ed25519 public keys for signature verification.
// It is thread-safe and updated dynamically by the Raft FSM when signing key
// management orders are applied.
type KeyStore struct {
	mu                sync.RWMutex
	publicKeys        map[string]ed25519.PublicKey
	requireSignatures bool
	maintenanceMode   bool
}

// NewKeyStore creates a new empty KeyStore.
func NewKeyStore() *KeyStore {
	return &KeyStore{
		publicKeys: make(map[string]ed25519.PublicKey),
	}
}

// GetPublicKey returns the public key for the given key ID, or nil if not found.
func (ks *KeyStore) GetPublicKey(keyID string) ed25519.PublicKey {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.publicKeys[keyID]
}

// AddPublicKey registers a public key with the given key ID.
func (ks *KeyStore) AddPublicKey(keyID string, pubKey ed25519.PublicKey) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.publicKeys[keyID] = pubKey
}

// RemovePublicKey removes the public key with the given key ID.
func (ks *KeyStore) RemovePublicKey(keyID string) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	delete(ks.publicKeys, keyID)
}

// RequireSignatures returns whether all requests must be signed.
func (ks *KeyStore) RequireSignatures() bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.requireSignatures
}

// SetRequireSignatures sets whether all requests must be signed.
func (ks *KeyStore) SetRequireSignatures(require bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.requireSignatures = require
}

// MaintenanceMode returns whether maintenance mode is active.
func (ks *KeyStore) MaintenanceMode() bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.maintenanceMode
}

// SetMaintenanceMode sets whether maintenance mode is active.
func (ks *KeyStore) SetMaintenanceMode(enabled bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.maintenanceMode = enabled
}

// HasKeys returns true if at least one public key is registered.
func (ks *KeyStore) HasKeys() bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return len(ks.publicKeys) > 0
}

// GetAllKeys returns a copy of all registered public keys.
// Used for snapshot serialization.
func (ks *KeyStore) GetAllKeys() map[string]ed25519.PublicKey {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	result := make(map[string]ed25519.PublicKey, len(ks.publicKeys))
	for k, v := range ks.publicKeys {
		result[k] = v
	}
	return result
}

// Reset clears all keys and configuration. Used during snapshot restore.
func (ks *KeyStore) Reset() {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.publicKeys = make(map[string]ed25519.PublicKey)
	ks.requireSignatures = false
	ks.maintenanceMode = false
}
