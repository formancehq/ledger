package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogProcessor(t *testing.T) {

	inputs := []any{
		Transaction{
			TransactionData: TransactionData{
				Postings: []Posting{
					{
						Source:      "world",
						Destination: "orders:1234",
						Amount:      NewMonetaryInt(100),
						Asset:       "USD",
					},
				},
				Metadata: Metadata{},
			},
			ID: 0,
		},
		Transaction{
			TransactionData: TransactionData{
				Postings: []Posting{
					{
						Source:      "orders:1234",
						Destination: "merchant:1234",
						Amount:      NewMonetaryInt(90),
						Asset:       "USD",
					},
					{
						Source:      "orders:1234",
						Destination: "fees",
						Amount:      NewMonetaryInt(10),
						Asset:       "USD",
					},
				},
				Metadata: Metadata{},
			},
			ID: 1,
		},
		SetMetadata{
			TargetType: MetaTargetTypeTransaction,
			TargetID:   0,
			Metadata: Metadata{
				"psp-ref": json.RawMessage(`"#ABCDEF"`),
			},
		},
		SetMetadata{
			TargetType: MetaTargetTypeAccount,
			TargetID:   "orders:1234",
			Metadata: Metadata{
				"booking-online": json.RawMessage(`true`),
			},
		},
	}

	p := NewLogProcessor()
	for _, input := range inputs {
		var log Log
		switch ob := input.(type) {
		case Transaction:
			log = NewTransactionLog(ob)
		case SetMetadata:
			log = NewSetMetadataLog(time.Now().Truncate(time.Second).UTC(), ob)
		}
		p.ProcessNextLog(log)
	}

	require.Equal(t, []*ExpandedTransaction{
		{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: []Posting{
						{
							Source:      "world",
							Destination: "orders:1234",
							Amount:      NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
					Metadata: Metadata{
						"psp-ref": json.RawMessage(`"#ABCDEF"`),
					},
				},
				ID: 0,
			},
			PreCommitVolumes: AccountsAssetsVolumes{
				"world": {
					"USD": {
						Input:  NewMonetaryInt(0),
						Output: NewMonetaryInt(0),
					},
				},
				"orders:1234": {
					"USD": {
						Input:  NewMonetaryInt(0),
						Output: NewMonetaryInt(0),
					},
				},
			},
			PostCommitVolumes: AccountsAssetsVolumes{
				"world": {
					"USD": {
						Input:  NewMonetaryInt(0),
						Output: NewMonetaryInt(100),
					},
				},
				"orders:1234": {
					"USD": {
						Input:  NewMonetaryInt(100),
						Output: NewMonetaryInt(0),
					},
				},
			},
		},
		{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: []Posting{
						{
							Source:      "orders:1234",
							Destination: "merchant:1234",
							Amount:      NewMonetaryInt(90),
							Asset:       "USD",
						},
						{
							Source:      "orders:1234",
							Destination: "fees",
							Amount:      NewMonetaryInt(10),
							Asset:       "USD",
						},
					},
					Metadata: Metadata{},
				},
				ID: 1,
			},
			PreCommitVolumes: AccountsAssetsVolumes{
				"orders:1234": {
					"USD": {
						Input:  NewMonetaryInt(100),
						Output: NewMonetaryInt(0),
					},
				},
				"merchant:1234": {
					"USD": {
						Input:  NewMonetaryInt(0),
						Output: NewMonetaryInt(0),
					},
				},
				"fees": {
					"USD": {
						Input:  NewMonetaryInt(0),
						Output: NewMonetaryInt(0),
					},
				},
			},
			PostCommitVolumes: AccountsAssetsVolumes{
				"orders:1234": {
					"USD": {
						Input:  NewMonetaryInt(100),
						Output: NewMonetaryInt(100),
					},
				},
				"merchant:1234": {
					"USD": {
						Input:  NewMonetaryInt(90),
						Output: NewMonetaryInt(0),
					},
				},
				"fees": {
					"USD": {
						Input:  NewMonetaryInt(10),
						Output: NewMonetaryInt(0),
					},
				},
			},
		},
	}, p.Transactions)
	require.Equal(t, AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  NewMonetaryInt(0),
				Output: NewMonetaryInt(100),
			},
		},
		"orders:1234": {
			"USD": {
				Input:  NewMonetaryInt(100),
				Output: NewMonetaryInt(100),
			},
		},
		"merchant:1234": {
			"USD": {
				Input:  NewMonetaryInt(90),
				Output: NewMonetaryInt(0),
			},
		},
		"fees": {
			"USD": {
				Input:  NewMonetaryInt(10),
				Output: NewMonetaryInt(0),
			},
		},
	}, p.Volumes)
	require.EqualValues(t, Accounts{
		"world": {
			Address:  "world",
			Metadata: Metadata{},
		},
		"orders:1234": {
			Address: "orders:1234",
			Metadata: Metadata{
				"booking-online": json.RawMessage(`true`),
			},
		},
		"merchant:1234": {
			Address:  "merchant:1234",
			Metadata: Metadata{},
		},
		"fees": {
			Address:  "fees",
			Metadata: Metadata{},
		},
	}, p.Accounts)

}
