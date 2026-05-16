package actions

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/domain/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// GenerateTestKeypair generates an Ed25519 keypair for testing.
func GenerateTestKeypair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// SignRequest signs a request with the given key. Returns the same request (modified in place).
func SignRequest(req *servicepb.Request, keyID string, privKey ed25519.PrivateKey) (*servicepb.Request, error) {
	if err := signing.Sign(req, keyID, privKey); err != nil {
		return nil, fmt.Errorf("signing request: %w", err)
	}

	return req, nil
}

// ListAllSigningKeys collects all signing keys from the ListSigningKeys stream.
func ListAllSigningKeys(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.SigningKey, error) {
	stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	if err != nil {
		return nil, err
	}

	var keys []*commonpb.SigningKey
	for {
		key, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, nil
}
