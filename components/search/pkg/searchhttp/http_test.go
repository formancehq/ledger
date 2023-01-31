package searchhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aquasecurity/esquery"
	"github.com/formancehq/go-libs/api"
	search "github.com/formancehq/search/pkg"
	"github.com/formancehq/search/pkg/es"
	"github.com/formancehq/search/pkg/searchengine"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
)

type queryChecker func(*testing.T, map[string]interface{})

func hasPageSize(pageSize int) queryChecker {
	return func(t *testing.T, m map[string]interface{}) {
		assert.EqualValues(t, pageSize, m["size"])
	}
}

func hasSort(sorts ...searchengine.Sort) queryChecker {
	expected := esquery.Sort{}
	for _, sort := range sorts {
		expected = append(expected, map[string]interface{}{
			"indexed." + sort.Key: map[string]interface{}{
				"order": sort.Order,
			},
		})
	}
	return func(t *testing.T, m map[string]interface{}) {
		assert.EqualValues(t, expected, m["sort"])
	}
}

func hasSearchAfter(searchAfter ...interface{}) queryChecker {
	return func(t *testing.T, m map[string]interface{}) {
		assert.EqualValues(t, searchAfter, m["search_after"])
	}
}

func TestMultiSearch(t *testing.T) {
	type testCase struct {
		name     string
		results  map[string][]interface{}
		expected interface{}
	}

	now := time.Now().Round(time.Second).UTC()
	var testCases = []testCase{
		{
			name: "nominal",
			results: map[string][]interface{}{
				"ACCOUNT": {
					core.Account{
						Address: "user:001",
					},
					core.Account{
						Address: "user:002",
						Metadata: core.Metadata{
							"foo": json.RawMessage(`"bar"`),
						},
					},
				},
				"TRANSACTION": {
					core.Transaction{
						ID: 1,
						TransactionData: core.TransactionData{
							Postings: []core.Posting{
								{
									Source:      "world",
									Destination: "central_bank",
									Amount:      core.NewMonetaryInt(100),
									Asset:       "USD",
								},
							},
							Reference: "tx1",
							Timestamp: now,
							Metadata: core.Metadata{
								"foo": json.RawMessage(`"bar"`),
							},
						},
					},
				},
			},
			expected: api.BaseResponse[map[string]interface{}]{
				Data: &map[string]interface{}{
					"ACCOUNT": []interface{}{
						map[string]interface{}{
							"address":  "user:001",
							"metadata": nil,
						},
						map[string]interface{}{
							"address": "user:002",
							"metadata": map[string]interface{}{
								"foo": "bar",
							},
						},
					},
					"TRANSACTION": []interface{}{
						map[string]interface{}{
							"txid":      float64(1),
							"reference": "tx1",
							"timestamp": now.Format(time.RFC3339),
							"metadata": map[string]interface{}{
								"foo": "bar",
							},
							"postings": []interface{}{
								map[string]interface{}{
									"source":      "world",
									"destination": "central_bank",
									"amount":      float64(100),
									"asset":       "USD",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			esResponse := &es.Response{
				Hits: es.ResponseHits{
					Hits: []es.ResponseHit{},
				},
			}

			for key, sources := range tc.results {
				esResponse.Hits.Hits = append(esResponse.Hits.Hits, es.ResponseHit{
					Fields: map[string][]string{
						"kind": {key},
					},
					InnerHits: map[string]struct {
						Hits es.ResponseHits `json:"hits"`
					}{
						"docs": {
							Hits: es.ResponseHits{
								Hits: func() []es.ResponseHit {
									ret := make([]es.ResponseHit, 0)
									for _, source := range sources {
										sourceData, err := json.Marshal(source)
										assert.NoError(t, err)

										data, err := json.Marshal(search.Source{
											Kind:   key,
											Ledger: "testing",
											When:   time.Time{},
											Data:   sourceData,
										})
										assert.NoError(t, err)
										ret = append(ret, es.ResponseHit{
											Source: data,
										})
									}
									return ret
								}(),
							},
						},
					},
				})
			}

			engine := searchengine.EngineFn(
				func(ctx context.Context, m map[string]interface{}) (*es.Response, error) {
					return esResponse, nil
				})

			r := Handler(engine)

			query := map[string]interface{}{}
			data, err := json.Marshal(query)
			assert.NoError(t, err)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/", bytes.NewBuffer(data))
			r.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Result().StatusCode)

			response := api.BaseResponse[map[string]interface{}]{}
			err = json.NewDecoder(rec.Body).Decode(&response)
			assert.NoError(t, err)

			assert.EqualValues(t, tc.expected, response)
		})
	}

}

func TestSingleDocTypeSearch(t *testing.T) {
	type testCase struct {
		name         string
		query        map[string]interface{}
		kind         string
		results      []interface{}
		expected     interface{}
		queryChecker []queryChecker
	}

	now := time.Now().Round(time.Second).UTC()
	var testCases = []testCase{
		{
			name:  "nominal",
			kind:  "ACCOUNT",
			query: map[string]interface{}{},
			results: []interface{}{
				core.Account{
					Address: "user:001",
				},
				core.Account{
					Address: "user:002",
					Metadata: core.Metadata{
						"foo": json.RawMessage(`"bar"`),
					},
				},
			},
			expected: api.BaseResponse[map[string]interface{}]{
				Cursor: &api.Cursor[map[string]interface{}]{
					PageSize: 2,
					Total: &api.Total{
						Value: 2,
						Rel:   "eq",
					},
					Data: []map[string]interface{}{
						{
							"address":  "user:001",
							"metadata": nil,
						},
						{
							"address": "user:002",
							"metadata": map[string]interface{}{
								"foo": "bar",
							},
						},
					},
				},
			},
		},
		{
			name: "pageSize",
			kind: "ACCOUNT",
			query: map[string]interface{}{
				"pageSize": 1,
			},
			queryChecker: []queryChecker{
				hasPageSize(2),
				hasSort(searchengine.Sort{
					Key:   "address",
					Order: esquery.OrderDesc,
				}),
			},
			results: []interface{}{
				core.Account{
					Address: "user:002",
					Metadata: core.Metadata{
						"foo": json.RawMessage(`"bar"`),
					},
				},
			},
			expected: api.BaseResponse[map[string]interface{}]{
				Cursor: &api.Cursor[map[string]interface{}]{
					PageSize: 1,
					HasMore:  false,
					Total: &api.Total{
						Value: 1,
						Rel:   "eq",
					},
					Data: []map[string]interface{}{
						{
							"address": "user:002",
							"metadata": map[string]interface{}{
								"foo": "bar",
							},
						},
					},
				},
			},
		},
		{
			name: "search-after",
			kind: "ACCOUNT",
			query: map[string]interface{}{
				"after": []interface{}{
					"user:002",
				},
			},
			queryChecker: []queryChecker{
				hasSort(searchengine.Sort{
					Key:   "address",
					Order: esquery.OrderDesc,
				}),
				hasSearchAfter("user:002"),
			},
			results: []interface{}{
				core.Account{
					Address: "user:001",
				},
			},
			expected: api.BaseResponse[map[string]interface{}]{
				Cursor: &api.Cursor[map[string]interface{}]{
					PageSize: 1,
					HasMore:  false,
					Total: &api.Total{
						Value: 1,
						Rel:   "eq",
					},
					Data: []map[string]interface{}{
						{
							"address":  "user:001",
							"metadata": nil,
						},
					},
				},
			},
		},
		{
			name: "next-page",
			kind: "ACCOUNT",
			query: map[string]interface{}{
				"cursor": EncodeCursorToken(cursorTokenInfo{
					Target: "ACCOUNT",
					Sort: []searchengine.Sort{
						{
							Key:   "address",
							Order: esquery.OrderDesc,
						},
					},
					SearchAfter: []interface{}{
						"user:002",
					},
					PageSize: 5,
				}),
			},
			queryChecker: []queryChecker{
				hasPageSize(6),
				hasSort(searchengine.Sort{
					Key:   "address",
					Order: esquery.OrderDesc,
				}),
				hasSearchAfter("user:002"),
			},
			results: []interface{}{
				core.Account{
					Address: "user:001",
				},
			},
			expected: api.BaseResponse[map[string]interface{}]{
				Cursor: &api.Cursor[map[string]interface{}]{
					PageSize: 1,
					HasMore:  false,
					Previous: EncodeCursorToken(cursorTokenInfo{
						Target: "ACCOUNT",
						Sort: []searchengine.Sort{
							{
								Key:   "address",
								Order: esquery.OrderAsc,
							},
						},
						SearchAfter: []interface{}{
							"user:001",
						},
						PageSize: 5,
						Reverse:  true,
					}),
					Total: &api.Total{
						Value: 1,
						Rel:   "eq",
					},
					Data: []map[string]interface{}{
						{
							"address":  "user:001",
							"metadata": nil,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			esResponse := &es.Response{
				Hits: es.ResponseHits{
					Hits: []es.ResponseHit{},
					Total: es.ResponseHitsTotal{
						Value:    len(tc.results),
						Relation: "eq",
					},
				},
			}

			for _, source := range tc.results {
				sourceData, err := json.Marshal(source)
				assert.NoError(t, err)

				data, err := json.Marshal(search.Source{
					Kind:   tc.kind,
					Ledger: "testing",
					When:   now,
					Data:   sourceData,
				})
				assert.NoError(t, err)
				esResponse.Hits.Hits = append(esResponse.Hits.Hits, es.ResponseHit{
					Source: data,
				})
			}

			engine := searchengine.EngineFn(
				func(ctx context.Context, m map[string]interface{}) (*es.Response, error) {
					for _, check := range tc.queryChecker {
						check(t, m)
					}
					return esResponse, nil
				})

			r := Handler(engine)

			tc.query["target"] = tc.kind
			data, err := json.Marshal(tc.query)
			assert.NoError(t, err)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/", bytes.NewBuffer(data))
			r.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Result().StatusCode)

			response := api.BaseResponse[map[string]interface{}]{}
			err = json.NewDecoder(rec.Body).Decode(&response)
			assert.NoError(t, err)
			assert.EqualValues(t, tc.expected, response)
		})
	}
}
