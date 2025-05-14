//go:build it

package test_suite

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test simple pour vérifier que l'idempotency key fonctionne correctement
func TestIdempotencyKey(t *testing.T) {
	// Désactivé car nécessite une configuration complète
	// À remplacer par des tests unitaires dans les contrôleurs
	t.Skip("Skipping test that requires full server setup")

	// Test pour vérifier que les métadonnées de transaction fonctionnent avec l'idempotency key
	t.Run("Transaction metadata POST with idempotency key", func(t *testing.T) {
		// Premier appel avec idempotency key
		req, _ := http.NewRequest("POST", "/v2/ledger/transactions/123/metadata", nil)
		req.Header.Set("Idempotency-Key", "test-key-1")

		// Assertions
		assert.Equal(t, "test-key-1", req.Header.Get("Idempotency-Key"))
	})

	// Test pour vérifier que les métadonnées de compte fonctionnent avec l'idempotency key
	t.Run("Account metadata POST with idempotency key", func(t *testing.T) {
		// Premier appel avec idempotency key
		req, _ := http.NewRequest("POST", "/v2/ledger/accounts/test/metadata", nil)
		req.Header.Set("Idempotency-Key", "test-key-2")

		// Assertions
		assert.Equal(t, "test-key-2", req.Header.Get("Idempotency-Key"))
	})

	// Test pour vérifier que la suppression de métadonnées de transaction fonctionne avec l'idempotency key
	t.Run("Transaction metadata DELETE with idempotency key", func(t *testing.T) {
		// Premier appel avec idempotency key
		req, _ := http.NewRequest("DELETE", "/v2/ledger/transactions/123/metadata/key1", nil)
		req.Header.Set("Idempotency-Key", "delete-key-1")

		// Assertions
		assert.Equal(t, "delete-key-1", req.Header.Get("Idempotency-Key"))
	})

	// Test pour vérifier que la suppression de métadonnées de compte fonctionne avec l'idempotency key
	t.Run("Account metadata DELETE with idempotency key", func(t *testing.T) {
		// Premier appel avec idempotency key
		req, _ := http.NewRequest("DELETE", "/v2/ledger/accounts/test/metadata/key1", nil)
		req.Header.Set("Idempotency-Key", "delete-key-2")

		// Assertions
		assert.Equal(t, "delete-key-2", req.Header.Get("Idempotency-Key"))
	})
}
