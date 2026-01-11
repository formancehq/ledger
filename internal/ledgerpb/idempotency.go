package ledgerpb

import (
	"crypto/sha256"
	"encoding/json/v2"
)

// ComputeIdempotencyHash computes a hash from inputs for idempotency checking
func ComputeIdempotencyHash(inputs any) []byte {
	digest := sha256.New()
	data, err := json.Marshal(inputs)
	if err != nil {
		panic(err)
	}
	digest.Write(data)
	digest.Write([]byte("\n")) // Add newline to match json.NewEncoder behavior

	return digest.Sum(nil)
}

