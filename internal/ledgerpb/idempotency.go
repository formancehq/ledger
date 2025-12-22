package ledgerpb

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
)

// ComputeIdempotencyHash computes a hash from inputs for idempotency checking
func ComputeIdempotencyHash(inputs any) string {
	digest := sha256.New()
	enc := json.NewEncoder(digest)

	if err := enc.Encode(inputs); err != nil {
		panic(err)
	}

	return base64.URLEncoding.EncodeToString(digest.Sum(nil))
}

