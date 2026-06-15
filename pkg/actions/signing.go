package actions

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

// ListAllSigningKeys collects every signing key from the ListSigningKeys
// stream, following the x-next-cursor trailer chain so clusters with more
// keys than the server's default page still surface them all.
func ListAllSigningKeys(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.SigningKey, error) {
	var (
		keys   []*commonpb.SigningKey
		cursor string
	)

	for {
		stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{
			Options: &commonpb.ListOptions{PageSize: listAllPageSize, Cursor: cursor},
		})
		if err != nil {
			return nil, err
		}

		for {
			key, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				return nil, recvErr
			}
			keys = append(keys, key)
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return keys, nil
		}

		cursor = next
	}
}
