package actions

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
)

// GenerateTestKeypair generates an Ed25519 keypair for testing.
func GenerateTestKeypair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// SignBatch signs an ApplyBatch (the atomic unit: ordered requests + idempotency
// key) and returns a signed ApplyRequest ready to pass to Apply. Signing the
// whole batch authenticates its composition and ordering.
func SignBatch(batch *ledgerpb.ApplyBatch, keyID string, privKey ed25519.PrivateKey) (*ledgerpb.ApplyRequest, error) {
	sb, err := signing.Sign(batch, keyID, privKey)
	if err != nil {
		return nil, fmt.Errorf("signing batch: %w", err)
	}

	return ledgerpb.SignedApplyRequest(sb), nil
}

// ListAllSigningKeys collects every signing key from the ListSigningKeys
// stream, following the x-next-cursor trailer chain so clusters with more
// keys than the server's default page still surface them all.
func ListAllSigningKeys(ctx context.Context, client ledgerpb.BucketServiceClient) ([]*ledgerpb.SigningKey, error) {
	var (
		keys   []*ledgerpb.SigningKey
		cursor string
	)

	for {
		stream, err := client.ListSigningKeys(ctx, &ledgerpb.ListSigningKeysRequest{
			Options: &ledgerpb.ListOptions{PageSize: listAllPageSize, Cursor: cursor},
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
