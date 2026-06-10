package receipt

import (
	"math/big"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestSignAndVerify(t *testing.T) {
	t.Parallel()

	signer := NewSigner([]byte("test-secret-key-32-bytes-long!!!"))

	postings := []*commonpb.Posting{
		{
			Source:      "world",
			Destination: "user:alice",
			Amount:      commonpb.NewUint256FromUint64(uint64(1000)),
			Asset:       "USD",
		},
	}
	timestamp := &commonpb.Timestamp{Data: 1700000000}

	token, err := signer.Sign("my-ledger", 42, postings, timestamp, 1)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := signer.Verify(token)
	require.NoError(t, err)
	require.Equal(t, "my-ledger", claims.Ledger)
	require.Equal(t, uint64(42), claims.TxID)
	require.Equal(t, uint64(1), claims.PeriodID)
	require.Equal(t, "ledger-v3", claims.Issuer)
	require.Len(t, claims.Postings, 1)
	require.Equal(t, "world", claims.Postings[0].Source)
	require.Equal(t, "user:alice", claims.Postings[0].Destination)
	require.Equal(t, "1000", claims.Postings[0].Amount)
	require.Equal(t, "USD", claims.Postings[0].Asset)
}

func TestVerifyWithWrongKey(t *testing.T) {
	t.Parallel()

	signer := NewSigner([]byte("correct-key-32-bytes-long!!!!!!"))
	wrongSigner := NewSigner([]byte("wrong-key-32-bytes-long!!!!!!!!"))

	postings := []*commonpb.Posting{
		{
			Source:      "world",
			Destination: "user:alice",
			Amount:      commonpb.NewUint256FromUint64(uint64(1000)),
			Asset:       "USD",
		},
	}

	token, err := signer.Sign("ledger", 1, postings, nil, 0)
	require.NoError(t, err)

	_, err = wrongSigner.Verify(token)
	require.Error(t, err)
}

func TestVerifyInvalidToken(t *testing.T) {
	t.Parallel()

	signer := NewSigner([]byte("test-key"))

	_, err := signer.Verify("not-a-valid-jwt-token")
	require.Error(t, err)
}

func TestSignMultiplePostings(t *testing.T) {
	t.Parallel()

	signer := NewSigner([]byte("test-secret-key-for-multiple!!!!"))

	postings := []*commonpb.Posting{
		{
			Source:      "world",
			Destination: "bank",
			Amount:      commonpb.NewUint256FromUint64(uint64(10000)),
			Asset:       "USD",
		},
		{
			Source:      "bank",
			Destination: "user:alice",
			Amount:      commonpb.NewUint256FromUint64(uint64(5000)),
			Asset:       "USD",
		},
		{
			Source:      "bank",
			Destination: "fees",
			Amount:      commonpb.NewUint256FromUint64(uint64(100)),
			Asset:       "USD",
		},
	}

	token, err := signer.Sign("payments", 7, postings, nil, 2)
	require.NoError(t, err)

	claims, err := signer.Verify(token)
	require.NoError(t, err)
	require.Len(t, claims.Postings, 3)
	require.Equal(t, uint64(7), claims.TxID)
	require.Equal(t, uint64(2), claims.PeriodID)
}

func TestClaimsToPostings(t *testing.T) {
	t.Parallel()

	postingClaims := []PostingClaim{
		{
			Source:      "world",
			Destination: "user:bob",
			Amount:      "5000",
			Asset:       "EUR",
		},
		{
			Source:      "user:bob",
			Destination: "user:carol",
			Amount:      "1000",
			Asset:       "EUR",
		},
	}

	postings := ClaimsToPostings(postingClaims)
	require.Len(t, postings, 2)

	require.Equal(t, "world", postings[0].GetSource())
	require.Equal(t, "user:bob", postings[0].GetDestination())
	require.Equal(t, big.NewInt(5000), postings[0].GetAmount().ToBigInt())
	require.Equal(t, "EUR", postings[0].GetAsset())

	require.Equal(t, "user:bob", postings[1].GetSource())
	require.Equal(t, "user:carol", postings[1].GetDestination())
	require.Equal(t, big.NewInt(1000), postings[1].GetAmount().ToBigInt())
	require.Equal(t, "EUR", postings[1].GetAsset())
}

// TestVerifyRejectsForeignAlg pins the algorithm-confusion defense
// from #342 / Review-2 L-21. A token forged with a different
// SigningMethod (HS512 here) but signed using the same []byte key
// must be rejected, even if the keyfunc returns []byte and the alg
// is not "none". golang-jwt's WithValidMethods is the canonical
// belt-and-braces for this attack class.
func TestVerifyRejectsForeignAlg(t *testing.T) {
	t.Parallel()

	key := []byte("test-secret-key-32-bytes-long!!!")
	signer := NewSigner(key)

	// Build a token with HS512 instead of the signer's HS256.
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{Issuer: "ledger-v3"},
		Ledger:           "L",
		TxID:             1,
	}
	forged, err := jwt.NewWithClaims(jwt.SigningMethodHS512, claims).SignedString(key)
	require.NoError(t, err)

	_, err = signer.Verify(forged)
	require.Error(t, err,
		"Verify must reject tokens whose alg is not the pinned HS256 (#342)")
}

// TestVerifyRejectsForeignIssuer pins the issuer guard. A token
// minted by a sibling service (different issuer) but signed with
// the same key must not be accepted as a ledger-v3 receipt.
func TestVerifyRejectsForeignIssuer(t *testing.T) {
	t.Parallel()

	key := []byte("test-secret-key-32-bytes-long!!!")
	signer := NewSigner(key)

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{Issuer: "some-other-service"},
		Ledger:           "L",
		TxID:             1,
	}
	forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	require.NoError(t, err)

	_, err = signer.Verify(forged)
	require.Error(t, err,
		"Verify must reject tokens whose issuer is not ledger-v3 (#342)")
}

func TestSignWithNilTimestamp(t *testing.T) {
	t.Parallel()

	signer := NewSigner([]byte("test-key-for-nil-timestamp!!!!!!"))

	postings := []*commonpb.Posting{
		{
			Source:      "world",
			Destination: "user:alice",
			Amount:      commonpb.NewUint256FromUint64(uint64(100)),
			Asset:       "USD",
		},
	}

	token, err := signer.Sign("ledger", 1, postings, nil, 0)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := signer.Verify(token)
	require.NoError(t, err)
	require.Equal(t, uint64(0), claims.PeriodID)
}
