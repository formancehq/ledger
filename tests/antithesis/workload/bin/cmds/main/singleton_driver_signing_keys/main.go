package main

import (
	"crypto/ed25519"
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	log.Println("composer: singleton_driver_signing_keys")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	bucketClient, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)

		return
	}
	defer func() { _ = conn.Close() }()

	// Key IDs are unique technical identities on the shared cluster.
	keyID := fmt.Sprintf("test-key-%d", internal.Rand().Uint64())
	var privateKey ed25519.PrivateKey
	if err := internal.RunOperationalGeneration("signing-keys", func(src *generation.Source) {
		privateKey = generatedSigningKey(src)
	}); err != nil {
		log.Printf("signing-keys: generation failed: %s", err)

		return
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)

	details := internal.Details{"keyId": keyID}

	// 1. Register the signing key.
	_, err = bucketClient.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_RegisterSigningKey{
			RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
				KeyId:     keyID,
				PublicKey: publicKey,
			},
		},
	}))

	assert.Sometimes(internal.IsTolerated(err),
		"should be able to register signing key", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// 2. Verify the key appears in ListSigningKeys. Use the trailer-aware
	// helper so long-running clusters with more than the server's default
	// page of keys still surface the one we just registered.
	keys, listErr := actions.ListAllSigningKeys(ctx, bucketClient)

	found := false
	for _, k := range keys {
		if k.GetKeyId() == keyID {
			found = true

			break
		}
	}

	if listErr == nil {
		assert.AlwaysOrUnreachable(found, "registered key should appear in ListSigningKeys", details)
	}

	// 3. Revoke the signing key (must be signed — keys exist on the cluster).
	revokeReq := &servicepb.Request{
		Type: &servicepb.Request_RevokeSigningKey{
			RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
				KeyId:   keyID,
				Cascade: true,
			},
		},
	}

	signedRevoke, err := signing.Sign(&servicepb.ApplyBatch{
		Requests: []*servicepb.Request{revokeReq},
	}, keyID, privateKey)
	if err != nil {
		log.Printf("failed to sign revoke request: %s", err)

		return
	}

	_, err = bucketClient.Apply(ctx, servicepb.SignedApplyRequest(signedRevoke))

	assert.Sometimes(internal.IsTolerated(err),
		"should be able to revoke signing key", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// 4. Verify the key is no longer listed (trailer-aware helper).
	keysAfter, listErr := actions.ListAllSigningKeys(ctx, bucketClient)

	foundAfterRevoke := false
	for _, k := range keysAfter {
		if k.GetKeyId() == keyID {
			foundAfterRevoke = true

			break
		}
	}

	if listErr == nil {
		assert.AlwaysOrUnreachable(!foundAfterRevoke, "revoked key should not appear in ListSigningKeys", details)
	}

	log.Printf("Signing key lifecycle complete: %s", keyID)
}

// generatedSigningKey draws all 32 seed bytes directly, then derives a coherent
// Ed25519 key pair. The Source never escapes into signing or RPC execution.
func generatedSigningKey(src *generation.Source) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(src.Intn(256))
	}

	return ed25519.NewKeyFromSeed(seed)
}
