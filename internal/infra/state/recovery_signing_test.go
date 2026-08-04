package state

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestRecoverState_SurvivesMalformedSigningKeyRow is the boot-path regression
// test for the length guard in query.ReadSigningKeys.
//
// Before the guard, recovery sliced value[:ed25519.PublicKeySize] on every
// SubGlobSigningKey row it read. Go bounds-checks that slice expression before
// the copy, so a row shorter than 32 bytes panicked — and since signing key rows
// are written through Raft, every replica held the same row and crash-looped
// together. The length check was also written after the slice, so it could never
// have fired.
//
// The assertion that matters is that boot completes and the decodable keys still
// load: dropping the malformed row silently would stop authorizing requests
// signed with it, which is why recovery logs each one and the checker reports it
// as SIGNING_KEY_MISMATCH.
func TestRecoverState_SurvivesMalformedSigningKeyRow(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	good := make([]byte, ed25519.PublicKeySize)
	for i := range good {
		good[i] = 0x42
	}

	batch := dataStore.OpenWriteSession()
	require.NoError(t, SaveSigningKey(batch, "good-key", good, ""))

	// Written raw rather than through SaveSigningKey, which always emits a full
	// public key: a short value can only come from disk corruption or tampering,
	// which is exactly the condition being modelled.
	require.NoError(t, batch.SetBytes(
		append([]byte{dal.ZoneGlobal, dal.SubGlobSigningKey}, "truncated-key"...),
		[]byte{0x01, 0x02, 0x03}))
	require.NoError(t, batch.Commit())

	var recoverErr error

	require.NotPanics(t, func() {
		recoverErr = NewRecovery(machine, dataStore).RecoverState()
	}, "a truncated signing key row must not panic the boot path")

	require.NoError(t, recoverErr, "a malformed signing key row must not fail boot")

	require.Equal(t, ed25519.PublicKey(good), machine.keyStore.GetPublicKey("good-key"),
		"the decodable keys must still load after a malformed row is skipped")
	require.Nil(t, machine.keyStore.GetPublicKey("truncated-key"),
		"a malformed row must be dropped whole, never half-loaded")

	// The skip must be recorded on the key store, not only logged. With EVERY row
	// corrupt the store would otherwise look identical to a fresh cluster, and
	// admission opens the unsigned RegisterSigningKey bootstrap exception on
	// exactly that condition — see
	// TestAuthorizeUnsignedBatch_BootstrapExceptionClosesOnUndecodableRows.
	require.True(t, machine.keyStore.HasUndecodableRows(),
		"recovery must mark the key store when it skips an undecodable row")
}
