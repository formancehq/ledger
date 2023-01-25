package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
)

func Sign(id string, timestamp int64, secret string, payload []byte) (string, error) {
	toSign := fmt.Sprintf("%s.%d.%s", id, timestamp, payload)

	hash := hmac.New(sha256.New, []byte(secret))
	if _, err := hash.Write([]byte(toSign)); err != nil {
		return "", fmt.Errorf("hash.Hash.Write: %w", err)
	}

	signature := make([]byte, base64.StdEncoding.EncodedLen(hash.Size()))

	base64.StdEncoding.Encode(signature, hash.Sum(nil))

	return fmt.Sprintf("v1,%s", signature), nil
}

func Verify(signatures, id string, timestamp int64, secret string, payload []byte) (bool, error) {
	computedSignature, err := Sign(id, timestamp, secret, payload)
	if err != nil {
		return false, err
	}

	expectedSignature := []byte(strings.Split(computedSignature, ",")[1])

	signatureSlice := strings.Split(signatures, " ")
	for _, versionedSignature := range signatureSlice {
		sigParts := strings.Split(versionedSignature, ",")
		if len(sigParts) < 2 {
			continue
		}

		version := sigParts[0]
		signature := []byte(sigParts[1])

		if version != "v1" {
			continue
		}

		if hmac.Equal(signature, expectedSignature) {
			return true, nil
		}
	}

	return false, nil
}
