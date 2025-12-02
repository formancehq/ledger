package v2

import (
	"encoding/json"
	"math/big"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
)

func TestTransactionRender(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tx := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
		).
		WithInsertedAt(now).
		WithUpdatedAt(now).
		WithTimestamp(now).
		WithMetadata(map[string]string{
			"foo": "bar",
		}).
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"world": ledger.VolumesByAssets{
				"USD/2": {
					Input:  new(big.Int),
					Output: big.NewInt(100),
				},
			},
			"bank": ledger.VolumesByAssets{
				"USD/2": {
					Input:  big.NewInt(100),
					Output: new(big.Int),
				},
			},
		}).
		WithPostCommitEffectiveVolumes(ledger.PostCommitVolumes{
			"world": ledger.VolumesByAssets{
				"USD/2": {
					Input:  new(big.Int),
					Output: big.NewInt(100),
				},
			},
			"bank": ledger.VolumesByAssets{
				"USD/2": {
					Input:  big.NewInt(100),
					Output: new(big.Int),
				},
			},
		}).
		WithReference("ref1").
		WithRevertedAt(now).
		WithID(10)

	type testCase struct {
		name     string
		headers  http.Header
		expected map[string]any
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			expected: map[string]any{
				"metadata": map[string]any{
					"foo": "bar",
				},
				"timestamp":  now.Format(time.RFC3339Nano),
				"reference":  "ref1",
				"insertedAt": now.Format(time.RFC3339Nano),
				"updatedAt":  now.Format(time.RFC3339Nano),
				"id":         float64(10),
				"revertedAt": now.Format(time.RFC3339Nano),
				"postings": []any{
					map[string]any{
						"source":      "world",
						"destination": "bank",
						"asset":       "USD/2",
						"amount":      float64(100),
					},
				},
				"postCommitEffectiveVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(100),
							"output":  float64(0),
							"balance": float64(100),
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(0),
							"output":  float64(100),
							"balance": float64(-100),
						},
					},
				},
				"postCommitVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(100),
							"output":  float64(0),
							"balance": float64(100),
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(0),
							"output":  float64(100),
							"balance": float64(-100),
						},
					},
				},
				"reverted": true,
				"preCommitEffectiveVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(0),
							"output":  float64(0),
							"balance": float64(0),
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(0),
							"output":  float64(0),
							"balance": float64(0),
						},
					},
				},
				"preCommitVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(0),
							"output":  float64(0),
							"balance": float64(0),
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   float64(0),
							"output":  float64(0),
							"balance": float64(0),
						},
					},
				},
				"template": "",
			},
		},
		{
			name: "big int as string",
			headers: http.Header{
				HeaderBigIntAsString: []string{"true"},
			},
			expected: map[string]any{
				"metadata": map[string]any{
					"foo": "bar",
				},
				"timestamp":  now.Format(time.RFC3339Nano),
				"reference":  "ref1",
				"insertedAt": now.Format(time.RFC3339Nano),
				"updatedAt":  now.Format(time.RFC3339Nano),
				"id":         float64(10),
				"revertedAt": now.Format(time.RFC3339Nano),
				"postings": []any{
					map[string]any{
						"source":      "world",
						"destination": "bank",
						"asset":       "USD/2",
						"amount":      "100",
					},
				},
				"postCommitEffectiveVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   "100",
							"output":  "0",
							"balance": "100",
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   "0",
							"output":  "100",
							"balance": "-100",
						},
					},
				},
				"postCommitVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   "100",
							"output":  "0",
							"balance": "100",
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   "0",
							"output":  "100",
							"balance": "-100",
						},
					},
				},
				"reverted": true,
				"preCommitEffectiveVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   "0",
							"output":  "0",
							"balance": "0",
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   "0",
							"output":  "0",
							"balance": "0",
						},
					},
				},
				"preCommitVolumes": map[string]any{
					"bank": map[string]any{
						"USD/2": map[string]any{
							"input":   "0",
							"output":  "0",
							"balance": "0",
						},
					},
					"world": map[string]any{
						"USD/2": map[string]any{
							"input":   "0",
							"output":  "0",
							"balance": "0",
						},
					},
				},
				"template": "",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			r.Header = tc.headers

			ret := renderTransaction(r, tx)

			data, err := json.Marshal(ret)
			require.NoError(t, err)

			m := make(map[string]interface{})
			err = json.Unmarshal(data, &m)
			require.NoError(t, err)

			require.Equal(t, tc.expected, m)
		})
	}
}

func TestVolumesWithBalanceByAssetByAccountRender(t *testing.T) {
	t.Parallel()

	volumes := ledger.VolumesWithBalanceByAssetByAccount{
		Account: "world",
		Asset:   "USD",
		VolumesWithBalance: ledger.VolumesWithBalance{
			Input:   big.NewInt(100),
			Output:  big.NewInt(200),
			Balance: big.NewInt(-100),
		},
	}

	type testCase struct {
		name     string
		headers  http.Header
		expected map[string]any
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			expected: map[string]any{
				"account": "world",
				"asset":   "USD",
				"input":   float64(100),
				"output":  float64(200),
				"balance": float64(-100),
			},
		},
		{
			name: "big int as string",
			headers: http.Header{
				HeaderBigIntAsString: []string{"true"},
			},
			expected: map[string]any{
				"account": "world",
				"asset":   "USD",
				"input":   "100",
				"output":  "200",
				"balance": "-100",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			r.Header = tc.headers

			ret := renderVolumesWithBalances(r, volumes)

			data, err := json.Marshal(ret)
			require.NoError(t, err)

			m := make(map[string]interface{})
			err = json.Unmarshal(data, &m)
			require.NoError(t, err)

			require.Equal(t, tc.expected, m)
		})
	}
}

func TestAccountRender(t *testing.T) {
	t.Parallel()

	now := time.Now()
	account := ledger.Account{
		BaseModel: bun.BaseModel{},
		Address:   "world",
		Metadata: metadata.Metadata{
			"foo": "bar",
		},
		FirstUsage:    now,
		InsertionDate: now,
		UpdatedAt:     now,
		Volumes: ledger.VolumesByAssets{
			"USD/2": ledger.Volumes{
				Input:  big.NewInt(100),
				Output: big.NewInt(200),
			},
		},
		EffectiveVolumes: ledger.VolumesByAssets{
			"USD/2": ledger.Volumes{
				Input:  big.NewInt(100),
				Output: big.NewInt(200),
			},
		},
	}

	type testCase struct {
		name     string
		headers  http.Header
		expected map[string]any
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			expected: map[string]any{
				"address": "world",
				"metadata": map[string]any{
					"foo": "bar",
				},
				"volumes": map[string]any{
					"USD/2": map[string]any{
						"input":   float64(100),
						"output":  float64(200),
						"balance": float64(-100),
					},
				},
				"effectiveVolumes": map[string]any{
					"USD/2": map[string]any{
						"input":   float64(100),
						"output":  float64(200),
						"balance": float64(-100),
					},
				},
				"firstUsage":    now.Format(time.RFC3339Nano),
				"insertionDate": now.Format(time.RFC3339Nano),
				"updatedAt":     now.Format(time.RFC3339Nano),
			},
		},
		{
			name: "big int as string",
			headers: http.Header{
				HeaderBigIntAsString: []string{"true"},
			},
			expected: map[string]any{
				"address": "world",
				"metadata": map[string]any{
					"foo": "bar",
				},
				"volumes": map[string]any{
					"USD/2": map[string]any{
						"input":   "100",
						"output":  "200",
						"balance": "-100",
					},
				},
				"effectiveVolumes": map[string]any{
					"USD/2": map[string]any{
						"input":   "100",
						"output":  "200",
						"balance": "-100",
					},
				},
				"firstUsage":    now.Format(time.RFC3339Nano),
				"insertionDate": now.Format(time.RFC3339Nano),
				"updatedAt":     now.Format(time.RFC3339Nano),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			r.Header = tc.headers

			ret := renderAccount(r, account)

			data, err := json.Marshal(ret)
			require.NoError(t, err)

			m := make(map[string]interface{})
			err = json.Unmarshal(data, &m)
			require.NoError(t, err)

			require.Equal(t, tc.expected, m)
		})
	}
}

func TestLogRender(t *testing.T) {
	t.Parallel()

	now := time.Now()
	type testCase struct {
		name     string
		log      ledger.Log
		headers  http.Header
		expected map[string]any
	}

	for _, tc := range []testCase{
		{
			name: "create transaction - nominal",
			log: ledger.
				NewLog(ledger.CreatedTransaction{
					Transaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100))).
						WithTimestamp(now).
						WithUpdatedAt(now).
						WithInsertedAt(now),
				}).
				WithID(10).
				WithDate(now),
			expected: map[string]any{
				"type": ledger.NewTransactionLogType.String(),
				"data": map[string]any{
					"transaction": map[string]any{
						"postings": []any{
							map[string]any{
								"source":      "world",
								"destination": "bank",
								"amount":      float64(100),
								"asset":       "USD/2",
							},
						},
						"metadata":   map[string]any{},
						"timestamp":  now.Format(time.RFC3339Nano),
						"insertedAt": now.Format(time.RFC3339Nano),
						"updatedAt":  now.Format(time.RFC3339Nano),
						"id":         nil,
						"reverted":   false,
						"template":   "",
					},
					"accountMetadata": nil,
				},
				"date":            now.Format(time.RFC3339Nano),
				"idempotencyKey":  "",
				"idempotencyHash": "",
				"id":              float64(10),
				"hash":            nil,
			},
		},
		{
			name: "create transaction - big int as string",
			headers: http.Header{
				HeaderBigIntAsString: []string{"true"},
			},
			log: ledger.
				NewLog(ledger.CreatedTransaction{
					Transaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100))).
						WithTimestamp(now).
						WithUpdatedAt(now).
						WithInsertedAt(now),
				}).
				WithID(10).
				WithDate(now),
			expected: map[string]any{
				"type": ledger.NewTransactionLogType.String(),
				"data": map[string]any{
					"transaction": map[string]any{
						"postings": []any{
							map[string]any{
								"source":      "world",
								"destination": "bank",
								"amount":      "100",
								"asset":       "USD/2",
							},
						},
						"metadata":   map[string]any{},
						"timestamp":  now.Format(time.RFC3339Nano),
						"insertedAt": now.Format(time.RFC3339Nano),
						"updatedAt":  now.Format(time.RFC3339Nano),
						"id":         nil,
						"reverted":   false,
						"template":   "",
					},
					"accountMetadata": nil,
				},
				"date":            now.Format(time.RFC3339Nano),
				"idempotencyKey":  "",
				"idempotencyHash": "",
				"id":              float64(10),
				"hash":            nil,
			},
		},
		{
			name: "reverted transaction - nominal",
			log: ledger.
				NewLog(ledger.RevertedTransaction{
					RevertedTransaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100))).
						WithTimestamp(now).
						WithUpdatedAt(now).
						WithInsertedAt(now).
						WithID(1),
					RevertTransaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("bank", "world", "USD/2", big.NewInt(100))).
						WithTimestamp(now).
						WithUpdatedAt(now).
						WithInsertedAt(now).
						WithID(2),
				}).
				WithID(10).
				WithDate(now),
			expected: map[string]any{
				"type": ledger.RevertedTransactionLogType.String(),
				"data": map[string]any{
					"revertedTransaction": map[string]any{
						"postings": []any{
							map[string]any{
								"source":      "world",
								"destination": "bank",
								"amount":      float64(100),
								"asset":       "USD/2",
							},
						},
						"metadata":   map[string]any{},
						"timestamp":  now.Format(time.RFC3339Nano),
						"insertedAt": now.Format(time.RFC3339Nano),
						"updatedAt":  now.Format(time.RFC3339Nano),
						"id":         float64(1),
						"reverted":   false,
						"template":   "",
					},
					"transaction": map[string]any{
						"postings": []any{
							map[string]any{
								"source":      "bank",
								"destination": "world",
								"amount":      float64(100),
								"asset":       "USD/2",
							},
						},
						"metadata":   map[string]any{},
						"timestamp":  now.Format(time.RFC3339Nano),
						"insertedAt": now.Format(time.RFC3339Nano),
						"updatedAt":  now.Format(time.RFC3339Nano),
						"id":         float64(2),
						"reverted":   false,
						"template":   "",
					},
				},
				"date":            now.Format(time.RFC3339Nano),
				"idempotencyKey":  "",
				"idempotencyHash": "",
				"id":              float64(10),
				"hash":            nil,
			},
		},
		{
			name: "reverted transaction - big int as string",
			headers: http.Header{
				HeaderBigIntAsString: []string{"true"},
			},
			log: ledger.
				NewLog(ledger.RevertedTransaction{
					RevertedTransaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100))).
						WithTimestamp(now).
						WithUpdatedAt(now).
						WithInsertedAt(now).
						WithID(1),
					RevertTransaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("bank", "world", "USD/2", big.NewInt(100))).
						WithTimestamp(now).
						WithUpdatedAt(now).
						WithInsertedAt(now).
						WithID(2),
				}).
				WithID(10).
				WithDate(now),
			expected: map[string]any{
				"type": ledger.RevertedTransactionLogType.String(),
				"data": map[string]any{
					"revertedTransaction": map[string]any{
						"postings": []any{
							map[string]any{
								"source":      "world",
								"destination": "bank",
								"amount":      "100",
								"asset":       "USD/2",
							},
						},
						"metadata":   map[string]any{},
						"timestamp":  now.Format(time.RFC3339Nano),
						"insertedAt": now.Format(time.RFC3339Nano),
						"updatedAt":  now.Format(time.RFC3339Nano),
						"id":         float64(1),
						"reverted":   false,
						"template":   "",
					},
					"transaction": map[string]any{
						"postings": []any{
							map[string]any{
								"source":      "bank",
								"destination": "world",
								"amount":      "100",
								"asset":       "USD/2",
							},
						},
						"metadata":   map[string]any{},
						"timestamp":  now.Format(time.RFC3339Nano),
						"insertedAt": now.Format(time.RFC3339Nano),
						"updatedAt":  now.Format(time.RFC3339Nano),
						"id":         float64(2),
						"reverted":   false,
						"template":   "",
					},
				},
				"date":            now.Format(time.RFC3339Nano),
				"idempotencyKey":  "",
				"idempotencyHash": "",
				"id":              float64(10),
				"hash":            nil,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			r.Header = tc.headers

			ret := renderLog(r, tc.log)

			data, err := json.Marshal(ret)
			require.NoError(t, err)

			m := make(map[string]interface{})
			err = json.Unmarshal(data, &m)
			require.NoError(t, err)

			require.Equal(t, tc.expected, m)
		})
	}
}
