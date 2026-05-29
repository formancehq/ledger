package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: singleton_driver_signing_keys")

	ctx := context.Background()
	bucketClient, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	r := internal.Rand()
	keyID := fmt.Sprintf("test-key-%d", r.Uint64())

	// Generate a random Ed25519 key pair.
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(r.Uint64())
	}

	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)

	details := internal.Details{"keyId": keyID}

	// 1. Register the signing key.
	_, err = bucketClient.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_RegisterSigningKey{
				RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
					KeyId:     keyID,
					PublicKey: publicKey,
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsTransient(err),
		"should be able to register signing key", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// 2. Verify the key appears in ListSigningKeys.
	stream, err := bucketClient.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	if err != nil {
		return
	}

	found := false
	streamErr := false

	for {
		key, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			streamErr = true

			break
		}

		if key.GetKeyId() == keyID {
			found = true
		}
	}

	if !streamErr {
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

	if err := signing.Sign(revokeReq, keyID, privateKey); err != nil {
		log.Printf("failed to sign revoke request: %s", err)
		return
	}

	_, err = bucketClient.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{revokeReq},
	})

	assert.Sometimes(err == nil || internal.IsTransient(err),
		"should be able to revoke signing key", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// 4. Verify the key is no longer listed.
	stream, err = bucketClient.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	if err != nil {
		return
	}

	foundAfterRevoke := false
	streamErr = false

	for {
		key, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			streamErr = true

			break
		}

		if key.GetKeyId() == keyID {
			foundAfterRevoke = true
		}
	}

	if !streamErr {
		assert.AlwaysOrUnreachable(!foundAfterRevoke, "revoked key should not appear in ListSigningKeys", details)
	}

	log.Printf("Signing key lifecycle complete: %s", keyID)
}
