package ledger

import (
	"encoding/base64"
	"github.com/formancehq/go-libs/v2/metadata"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/stretchr/testify/require"
)

func TestTransactionsReverse(t *testing.T) {
	tx := NewTransaction().
		WithPostings(
			NewPosting("world", "users:001", "COIN", big.NewInt(100)),
			NewPosting("users:001", "payments:001", "COIN", big.NewInt(100)),
		)

	expected := NewTransaction().
		WithPostings(
			NewPosting("payments:001", "users:001", "COIN", big.NewInt(100)),
			NewPosting("users:001", "world", "COIN", big.NewInt(100)),
		).
		WithTimestamp(tx.Timestamp)

	reversed := tx.Reverse()
	reversed.Timestamp = time.Time{}
	expected.Timestamp = time.Time{}
	require.Equal(t, expected, reversed)
}

func TestTransactionsVolumesUpdate(t *testing.T) {
	tx := NewTransaction().
		WithPostings(
			NewPosting("world", "users:001", "COIN", big.NewInt(100)),
			NewPosting("users:001", "payments:001", "COIN", big.NewInt(100)),
			NewPosting("payments:001", "world", "COIN", big.NewInt(100)),
		)

	require.Equal(t, []AccountsVolumes{
		{
			Account: "payments:001",
			Asset:   "COIN",
			Input:   big.NewInt(100),
			Output:  big.NewInt(100),
		},
		{
			Account: "users:001",
			Asset:   "COIN",
			Input:   big.NewInt(100),
			Output:  big.NewInt(100),
		},
		{
			Account: "world",
			Asset:   "COIN",
			Input:   big.NewInt(100),
			Output:  big.NewInt(100),
		},
	}, tx.VolumeUpdates())
}

func TestHash(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		log               Log
		expectedHash      string
		expectedHashRetry string
	}

	refDate := time.Time{}

	for _, tc := range []testCase{
		{
			name: "new transaction",
			log: NewLog(CreatedTransaction{
				Transaction:     NewTransaction(),
				AccountMetadata: make(AccountMetadata),
			}),
			expectedHash:      "RjKsuJOuPYeFljGJlXZ5nk4_21apQY_k8daJamyZTVI=",
			expectedHashRetry: "klWyIDudjjWU-BNHjRcFzTYHpU2CWi8lEVdVYjizHKo=",
		},
		{
			name: "new transaction with reference",
			log: NewLog(CreatedTransaction{
				Transaction:     NewTransaction().WithReference("foo"),
				AccountMetadata: make(AccountMetadata),
			}),
			expectedHash:      "SZ7XX-W_odawRCRvAmZkF0U_YnHDKY0Ku9zG_oaRgA4=",
			expectedHashRetry: "KWxResFbWNf2xoH5u1gKggQkxbXSG7wdrzrKVVBk6BE=",
		},
		{
			name: "new transaction with nil account metadata",
			log: NewLog(CreatedTransaction{
				Transaction:     NewTransaction(),
				AccountMetadata: nil,
			}),
			expectedHash:      "I4IOKCBxlOWAeTSwj52ZElJAWc88F1UkA63QtJceshw=",
			expectedHashRetry: "2cGS1rsuOcbHNqyeiOAx8mMBSvpNSFl_u_dSANI2BIM=",
		},
		{
			name: "saved metadata on account",
			log: NewLog(SavedMetadata{
				TargetType: MetaTargetTypeAccount,
				TargetID:   "world",
				Metadata: metadata.Metadata{
					"foo": "bar",
				},
			}),
			expectedHash:      "6TifTCapZm6xc2EaazWo-PTdruDa7DYtAn1SU6zS4uI=",
			expectedHashRetry: "a_pPkeX87fuTPof7SCIovxCbDF3EXvhASqrcXtzqoTs=",
		},
		{
			name: "saved metadata on transaction",
			log: NewLog(SavedMetadata{
				TargetType: MetaTargetTypeTransaction,
				TargetID:   big.NewInt(1),
				Metadata: metadata.Metadata{
					"foo": "bar",
				},
			}),
			expectedHash:      "zH6jHi4kW8HvZnqhnpBxga-R-WPkuFaTCiFn8vgR0is=",
			expectedHashRetry: "y-zQAnOwKdfqMetWoi6btTXuix5JgWkMEGP2a0z3YbY=",
		},
		{
			name: "deleted metadata on account",
			log: NewLog(DeletedMetadata{
				TargetType: MetaTargetTypeAccount,
				TargetID:   "world",
				Key:        "foo",
			}),
			expectedHash:      "e5Hb2rvqnhr96jCfoek69Fw7iYgoKoCYtl-qstYBvIg=",
			expectedHashRetry: "t0SizlUMhLc5RkF9849zQZ34JPSom29WRVnBXlDM-O8=",
		},
		{
			name: "deleted metadata on transaction",
			log: NewLog(DeletedMetadata{
				TargetType: MetaTargetTypeTransaction,
				TargetID:   big.NewInt(1),
				Key:        "foo",
			}),
			expectedHash:      "3TAvOvastJtB_KxvccNFpuXp57MEv8kSR3NiUf7zosg=",
			expectedHashRetry: "izQj6mfY65ePSC9utaiAftBnsPVwP8PaHPdoi7ruSN4=",
		},
		{
			name: "reverted transaction",
			log: NewLog(RevertedTransaction{
				RevertedTransaction: Transaction{ID: 1},
				RevertTransaction:   NewTransaction().WithTimestamp(refDate),
			}),
			expectedHash:      "14SSRP9Nf7zxJWPSH7KOz15favZmBhyWZ59V-WQZx18=",
			expectedHashRetry: "Re0FjRP34EBKzJTp4emmnVC1OKwd9f4mxVzbnTrvPd4=",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			chainedLog := tc.log.ChainLog(nil)
			require.Equal(t, tc.expectedHash, base64.URLEncoding.EncodeToString(chainedLog.Hash))

			hashedAgain := tc.log.ChainLog(&chainedLog)
			require.Equal(t, tc.expectedHashRetry, base64.URLEncoding.EncodeToString(hashedAgain.Hash))
		})
	}
}
