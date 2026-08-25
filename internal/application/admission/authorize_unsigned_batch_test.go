package admission

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestAuthorizeUnsignedBatch_BootstrapExceptionClosesOnUndecodableRows pins the
// gate on the unsigned RegisterSigningKey bootstrap exception.
//
// The exception exists so the very first signing key can be registered on a
// cluster that has none. Recovery skips undecodable signing-key rows, which
// creates a second way to reach an empty key store: every persisted row is
// corrupt. HasKeys cannot tell those two apart.
//
// Leaving the exception open there would be an authorization bypass with an
// unusually nasty property: whoever corrupted the rows registers their own key
// through the normal unsigned path, so the registration is AUDITED. The checker's
// signing pass would then see a legitimately chain-bound key and report only the
// corrupt row it replaced — the injected key is laundered into the audit chain.
func TestAuthorizeUnsignedBatch_BootstrapExceptionClosesOnUndecodableRows(t *testing.T) {
	t.Parallel()

	registerRequest := []*servicepb.Request{{
		Type: &servicepb.Request_RegisterSigningKey{
			RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
				KeyId:     "bootstrap",
				PublicKey: make([]byte, ed25519.PublicKeySize),
			},
		},
	}}

	newAdmission := func(seed func(*keystore.KeyStore)) *Admission {
		ks := keystore.NewKeyStore()
		if seed != nil {
			seed(ks)
		}

		return &Admission{keyStore: ks, sharedState: state.NewSharedState()}
	}

	t.Run("fresh cluster may bootstrap unsigned", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, newAdmission(nil).authorizeUnsignedBatch(context.Background(), registerRequest),
			"an empty key store with no corrupt rows is a genuinely fresh cluster")
	})

	t.Run("undecodable rows close the exception", func(t *testing.T) {
		t.Parallel()

		a := newAdmission(func(ks *keystore.KeyStore) {
			ks.MarkUndecodableRows(1)
		})

		require.Error(t, a.authorizeUnsignedBatch(context.Background(), registerRequest),
			"a store whose signing rows exist but do not decode is not a fresh cluster")
	})

	t.Run("existing keys close the exception", func(t *testing.T) {
		t.Parallel()

		a := newAdmission(func(ks *keystore.KeyStore) {
			ks.AddPublicKey("root", make([]byte, ed25519.PublicKeySize), "")
		})

		require.Error(t, a.authorizeUnsignedBatch(context.Background(), registerRequest),
			"the pre-existing guard must keep rejecting once a usable key is loaded")
	})

	t.Run("undecodable rows alongside a usable key still close it", func(t *testing.T) {
		t.Parallel()

		a := newAdmission(func(ks *keystore.KeyStore) {
			ks.AddPublicKey("root", make([]byte, ed25519.PublicKeySize), "")
			ks.MarkUndecodableRows(1)
		})

		require.Error(t, a.authorizeUnsignedBatch(context.Background(), registerRequest))
	})
}

// TestKeyStoreUndecodableRowsResetWithKeys pins that the marker is cleared by
// Reset. It describes the rows the load being performed observed, so carrying a
// stale count across a restore would keep the bootstrap gate shut on a store
// whose corrupt rows are gone — turning a transient corruption into a permanent
// lockout of signing management.
func TestKeyStoreUndecodableRowsResetWithKeys(t *testing.T) {
	t.Parallel()

	ks := keystore.NewKeyStore()
	require.False(t, ks.HasUndecodableRows(), "a fresh key store has observed no rows at all")

	ks.MarkUndecodableRows(2)
	require.True(t, ks.HasUndecodableRows())

	ks.Reset()
	require.False(t, ks.HasUndecodableRows(), "Reset must clear the marker along with the keys")

	ks.MarkUndecodableRows(0)
	require.False(t, ks.HasUndecodableRows(), "a clean load must not set the marker")
}

// TestAuthorizeUnsignedBatch_ExemptsSystemActor pins that a leader-internal
// proposal (system actor) is admitted unsigned even under RequireSignatures,
// while the same unsigned batch from a client is rejected. Without the exemption
// the chapter workers and schedulers can never propose on a signed cluster.
func TestAuthorizeUnsignedBatch_ExemptsSystemActor(t *testing.T) {
	t.Parallel()

	businessReq := []*servicepb.Request{{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: "l"},
		},
	}}

	a := &Admission{keyStore: keystore.NewKeyStore(), sharedState: state.NewSharedState()}
	a.sharedState.SetRequireSignatures(true)

	require.ErrorIs(t, a.authorizeUnsignedBatch(context.Background(), businessReq), signing.ErrMissingSignature,
		"a client's unsigned business write is rejected under RequireSignatures")

	sysCtx := internalauth.WithSystemActor(context.Background(), commands.ComponentChapterArchiver)
	require.NoError(t, a.authorizeUnsignedBatch(sysCtx, businessReq),
		"a leader-internal system proposal is admitted unsigned")
}
