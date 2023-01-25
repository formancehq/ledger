package webhooks

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

type Secret struct {
	Secret string `json:"secret" bson:"secret"`
}

func (s *Secret) Validate() error {
	if s.Secret == "" {
		s.Secret = NewSecret()
	} else {
		var decoded []byte
		var err error
		if decoded, err = base64.StdEncoding.DecodeString(s.Secret); err != nil {
			return fmt.Errorf("secret should be base64 encoded: %w", err)
		}
		if len(decoded) != 24 {
			return ErrInvalidSecret
		}
	}

	return nil
}

func NewSecret() string {
	token := make([]byte, 24)
	_, err := rand.Read(token)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(token)
}
