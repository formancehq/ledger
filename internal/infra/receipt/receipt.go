package receipt

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Signer creates and verifies JWT receipts for transactions.
type Signer struct {
	signingKey []byte
}

// NewSigner creates a new receipt signer with the given HMAC key.
func NewSigner(key []byte) *Signer {
	return &Signer{signingKey: key}
}

// PostingClaim is the JSON representation of a posting inside a JWT.
type PostingClaim struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"`
	Asset       string `json:"asset"`
}

// Claims are the custom JWT claims for a transaction receipt.
type Claims struct {
	jwt.RegisteredClaims

	Ledger    string         `json:"ledger"`
	TxID      uint64         `json:"txId"`
	Postings  []PostingClaim `json:"postings"`
	ChapterID uint64         `json:"chapterId"`
}

// Sign creates a JWT receipt for a transaction.
func (s *Signer) Sign(ledger string, txID uint64, postings []*commonpb.Posting, timestamp uint64, chapterID uint64) (string, error) {
	postingClaims := make([]PostingClaim, len(postings))
	for i, p := range postings {
		postingClaims[i] = PostingClaim{
			Source:      p.GetSource(),
			Destination: p.GetDestination(),
			Amount:      p.GetAmount().Dec(),
			Asset:       p.GetAsset(),
		}
	}

	issuedAt := time.Now()
	if timestamp != 0 {
		issuedAt = time.UnixMicro(int64(timestamp))
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "ledger-v3",
			IssuedAt: jwt.NewNumericDate(issuedAt),
		},
		Ledger:    ledger,
		TxID:      txID,
		Postings:  postingClaims,
		ChapterID: chapterID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	return token.SignedString(s.signingKey)
}

// Verify verifies a JWT receipt and returns its claims.
//
// Algorithm pinning is required defense-in-depth on a path that
// verifies financial receipts: golang-jwt v5 already blocks
// alg:none and the []byte keyfunc makes asymmetric-alg confusion
// fail at the type assertion, but pinning protects against future
// library or keyfunc changes (#342 / Review-2 L-21). Issuer pinning
// in the same call rejects tokens minted by anything other than
// this Signer.
func (s *Signer) Verify(tokenString string) (*Claims, error) {
	claims := &Claims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(_ *jwt.Token) (any, error) {
		return s.signingKey, nil
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer("ledger-v3"),
	)
	if err != nil {
		return nil, fmt.Errorf("parsing receipt token: %w", err)
	}

	if !token.Valid {
		return nil, errors.New("invalid receipt token")
	}

	return claims, nil
}

// ClaimsToPostings converts PostingClaims back to protobuf Postings.
func ClaimsToPostings(claims []PostingClaim) []*commonpb.Posting {
	postings := make([]*commonpb.Posting, len(claims))
	for i, c := range claims {
		var v uint256.Int

		err := v.SetFromDecimal(c.Amount)
		if err != nil {
			v.Clear()
		}

		postings[i] = &commonpb.Posting{
			Source:      c.Source,
			Destination: c.Destination,
			Amount:      commonpb.NewUint256(&v),
			Asset:       c.Asset,
		}
	}

	return postings
}
