package searchengine

import (
	"context"
	"encoding/json"

	"github.com/aquasecurity/esquery"
	search "github.com/formancehq/search/pkg"
	"github.com/formancehq/search/pkg/es"
	"github.com/pkg/errors"
)

type baseQuery struct {
	Ledgers    []string `json:"ledgers"`
	Terms      []string `json:"terms"`
	PageSize   uint64   `json:"pageSize"`
	TermPolicy string   `json:"policy"`
}

func (q *baseQuery) WithLedgers(ledgers ...string) {
	q.Ledgers = ledgers
}

func (q *baseQuery) WithTerms(terms ...string) {
	q.Terms = terms
}

func (q *baseQuery) WithPageSize(pageSize uint64) {
	q.PageSize = pageSize
}

func (q *baseQuery) WithPolicy(policy string) {
	q.TermPolicy = policy
}

func (q *baseQuery) buildList(hits []es.ResponseHit) ([]json.RawMessage, error) {
	res := make([]json.RawMessage, 0)
	for _, hit := range hits {
		src := search.Source{}
		err := json.Unmarshal(hit.Source, &src)
		if err != nil {
			return nil, err
		}
		res = append(res, src.Data)
	}
	return res, nil
}

type Sort struct {
	Key   string        `json:"key"`
	Order esquery.Order `json:"order"`
}

type SingleDocTypeSearchResponse struct {
	Items []json.RawMessage
	Total es.ResponseHitsTotal
}

type SingleDocTypeSearch struct {
	baseQuery
	Sort        []Sort        `json:"sort"`
	Target      string        `json:"target"`
	SearchAfter []interface{} `json:"after"`
}

func (q *SingleDocTypeSearch) Do(ctx context.Context, e Engine) (*SingleDocTypeSearchResponse, error) {

	filter := esquery.Bool().
		Must(esquery.Match("kind", q.Target))

	should := make([]esquery.Mappable, 0)
	if len(q.Ledgers) > 0 {
		should = append(should, esquery.Bool().Should(
			func() []esquery.Mappable {
				res := make([]esquery.Mappable, 0)
				for _, l := range q.Ledgers {
					res = append(res, esquery.Match("ledger", l))
				}
				res = append(res, esquery.Bool().MustNot(esquery.Exists("ledger")))
				return res
			}()...,
		))
		filter = filter.MinimumShouldMatch(1).Should(should...)
	}

	query := esquery.Bool().Filter(filter)
	if len(q.Terms) > 0 {
		terms, err := ParseTerms(q.Terms...)
		if err != nil {
			return nil, errors.Wrap(err, "parsing terms")
		}
		if q.TermPolicy == TermPolicyOR {
			query = query.Should(terms...).MinimumShouldMatch(1)
		} else {
			query = query.Must(terms...)
		}
	}

	req := esquery.Search().Query(query)
	if len(q.SearchAfter) != 0 {
		req = req.SearchAfter(q.SearchAfter...)
	}
	for _, sort := range q.Sort {
		req.Sort("indexed."+sort.Key, sort.Order)
	}
	req.Size(q.PageSize)

	res, err := e.doRequest(ctx, req.Map())
	if err != nil {
		return nil, err
	}

	list, err := q.buildList(res.Hits.Hits)
	if err != nil {
		return nil, err
	}

	return &SingleDocTypeSearchResponse{
		Items: list,
		Total: res.Hits.Total,
	}, nil
}

func (q *SingleDocTypeSearch) WithSort(field string, order esquery.Order) {
	q.Sort = append(q.Sort, struct {
		Key   string        `json:"key"`
		Order esquery.Order `json:"order"`
	}{Key: field, Order: order})
}

func (q *SingleDocTypeSearch) WithSearchAfter(after []interface{}) {
	q.SearchAfter = after
}

func (q *SingleDocTypeSearch) WithTarget(target string) {
	q.Target = target
}

func NewSingleDocTypeSearch(target string) *SingleDocTypeSearch {
	return &SingleDocTypeSearch{
		baseQuery: baseQuery{
			PageSize: 5,
		},
		Target: target,
	}
}

type MultiDocTypeSearchResponse map[string][]json.RawMessage

type MultiDocTypeSearch struct {
	baseQuery
}

func (q *MultiDocTypeSearch) Do(ctx context.Context, e Engine) (MultiDocTypeSearchResponse, error) {

	result := MultiDocTypeSearchResponse{}
	filter := esquery.Bool()

	should := make([]esquery.Mappable, 0)
	if len(q.Ledgers) > 0 {
		should = append(should, esquery.Bool().Should(
			func() []esquery.Mappable {
				res := make([]esquery.Mappable, 0)
				for _, l := range q.Ledgers {
					res = append(res, esquery.Match("ledger", l))
				}
				res = append(res, esquery.Bool().MustNot(esquery.Exists("ledger")))
				return res
			}()...,
		))
		filter = filter.MinimumShouldMatch(1).Should(should...)
	}

	query := esquery.Bool().Filter(filter)
	if len(q.Terms) > 0 {
		terms, err := ParseTerms(q.Terms...)
		if err != nil {
			return nil, errors.Wrap(err, "parsing terms")
		}
		query = query.Must(terms...)
	}

	req := esquery.Search().Query(query)
	m := req.Map()
	m["collapse"] = map[string]interface{}{
		"field": "kind",
		"inner_hits": map[string]interface{}{
			"name": "docs",
			"size": q.PageSize,
			"sort": []map[string]interface{}{
				{
					"when": "desc",
				},
			},
		},
	}

	res, err := e.doRequest(ctx, m)
	if err != nil {
		return nil, err
	}

	for _, hit := range res.Hits.Hits {
		objects, err := q.buildList(hit.InnerHits["docs"].Hits.Hits)
		if err != nil {
			return nil, err
		}
		result[hit.Fields["kind"][0]] = objects
	}

	return result, nil
}

func NewMultiDocTypeSearch() *MultiDocTypeSearch {
	return &MultiDocTypeSearch{
		baseQuery{PageSize: 5},
	}
}

type RawQuery struct {
	Body json.RawMessage `json:"body"`
}

type RawQueryResponse *es.Response

func (q RawQuery) WithPageSize(pageSize uint64) {
}

func (q RawQuery) Do(ctx context.Context, e Engine) (RawQueryResponse, error) {
	var query map[string]interface{}
	err := json.Unmarshal(q.Body, &query)
	if err != nil {
		return nil, errors.Wrap(err, "query.invalid_body")
	}
	res, err := e.doRequest(ctx, query)
	if err != nil {
		return nil, err
	}
	return RawQueryResponse(res), nil
}
