// Package httpreceipt authenticates public HTTP response bytes using a trusted
// Ed25519 key. Receipts do not prove freshness, request-body identity, actor or
// cluster identity, or audit-chain inclusion. Verification failure does not undo
// a mutation. Clients must establish key trust independently of the receipt.
package httpreceipt

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

const (
	// Header carries a base64url-encoded JSON receipt envelope without padding.
	Header = "Formance-Public-Receipt"
	// Version identifies the public receipt claims contract.
	Version       = "v1"
	domain        = "formance.http-public-receipt.v1\x00"
	maxHeaderSize = 4096
)

// Claims binds the exact method, request target (escaped path and raw query),
// HTTP status and exact public Apply response body. The body contains the
// ordered log sequence and credential-safe projection. Hashes are lowercase
// hex SHA-256.
type Claims struct {
	Version    string `json:"version"`
	Method     string `json:"method"`
	TargetHash string `json:"targetHash"`
	BodyHash   string `json:"bodyHash"`
	Status     int    `json:"status"`
}

type envelope struct {
	KeyID     string `json:"keyId"`
	Payload   []byte `json:"payload"`
	Signature []byte `json:"signature"`
}

// Sign signs the exact JSON claims bytes with domain separation. The envelope's
// keyId is a lookup hint only; it is not an independently trusted identity.
func Sign(privateKey ed25519.PrivateKey, claims Claims) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", errors.New("invalid receipt private key length")
	}
	if err := claims.validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("encode receipt claims: %w", err)
	}
	keyHash := sha256.Sum256(privateKey[ed25519.SeedSize:])
	encoded, err := json.Marshal(envelope{KeyID: hex.EncodeToString(keyHash[:8]), Payload: payload, Signature: ed25519.Sign(privateKey, append([]byte(domain), payload...))})
	if err != nil {
		return "", fmt.Errorf("encode receipt envelope: %w", err)
	}
	header := base64.RawURLEncoding.EncodeToString(encoded)
	if len(header) > maxHeaderSize {
		return "", errors.New("receipt exceeds header size limit")
	}

	return header, nil
}

// Verify authenticates the original payload bytes, then binds their claims to
// the response the client actually observed. Pass the unmodified response body
// before JSON decoding, and the exact request target observed by Ledger. Proxies
// that rewrite targets require a trusted mapping configured by the client. A missing
// receipt is an error. The trusted key must come from an authenticated channel.
func Verify(header string, trustedPublicKey ed25519.PublicKey, method, target string, status int, body []byte) (*Claims, error) {
	if len(header) == 0 || len(header) > maxHeaderSize {
		return nil, errors.New("missing or oversized public receipt")
	}
	if len(trustedPublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("invalid receipt public key length")
	}
	raw, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return nil, fmt.Errorf("decode receipt header: %w", err)
	}
	var signed envelope
	if err := json.Unmarshal(raw, &signed); err != nil {
		return nil, fmt.Errorf("decode receipt envelope: %w", err)
	}
	if !ed25519.Verify(trustedPublicKey, append([]byte(domain), signed.Payload...), signed.Signature) {
		return nil, errors.New("public receipt signature verification failed")
	}
	var claims Claims
	if err := json.Unmarshal(signed.Payload, &claims); err != nil {
		return nil, fmt.Errorf("decode receipt claims: %w", err)
	}
	if err := claims.validate(); err != nil {
		return nil, err
	}
	targetHash, bodyHash := sha256.Sum256([]byte(target)), sha256.Sum256(body)
	if claims.Method != method || claims.TargetHash != hex.EncodeToString(targetHash[:]) || claims.Status != status || claims.BodyHash != hex.EncodeToString(bodyHash[:]) {
		return nil, errors.New("observed HTTP response does not match public receipt")
	}

	return &claims, nil
}

func (c Claims) validate() error {
	if c.Version != Version {
		return errors.New("unsupported public receipt version")
	}
	if c.Method != http.MethodPost || c.Status < 200 || c.Status > 599 {
		return errors.New("invalid public receipt claims")
	}
	for _, value := range []string{c.TargetHash, c.BodyHash} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
			return errors.New("invalid public receipt hash")
		}
	}

	return nil
}
