package ledgerpb

import (
	"crypto/sha256"

	"google.golang.org/protobuf/proto"
)

// ComputeIdempotencyHash computes a hash from inputs for idempotency checking
func ComputeIdempotencyHash(inputs proto.Message) []byte {
	data, err := proto.Marshal(inputs)
	if err != nil {
		panic(err)
	}

	digest := sha256.New()
	digest.Write(data)

	return digest.Sum(nil)
}

