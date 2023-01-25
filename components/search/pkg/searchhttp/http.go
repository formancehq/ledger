package searchhttp

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/aquasecurity/esquery"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/search/pkg/searchengine"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

type BaseQuery interface {
	WithPageSize(pageSize uint64)
}

func resolveQuery(r *http.Request) (*cursorTokenInfo, BaseQuery, error) {
	var (
		target      string
		cursorToken string
		info        *cursorTokenInfo
	)

	if r.ContentLength > 0 {
		type resolveQuery struct {
			Target      string `json:"target"`
			CursorToken string `json:"cursor"`
		}
		rq := &resolveQuery{}
		buf := bytes.NewBufferString("")
		err := json.NewDecoder(io.TeeReader(r.Body, buf)).Decode(rq)
		if err != nil {
			return nil, nil, errors.Wrap(err, "first phase decoding")
		}
		r.Body = io.NopCloser(buf)
		target = rq.Target
		cursorToken = rq.CursorToken
	} else {
		target = r.Form.Get("target")
		cursorToken = r.Form.Get("cursor")
	}

	var searchQuery BaseQuery
	if cursorToken == "" {
		if target == "" {
			sq := searchengine.NewMultiDocTypeSearch()
			sq.WithTerms(r.Form["terms"]...)
			sq.WithLedgers(r.Form["ledgers"]...)
			if termPolicy := r.Form.Get("term-policy"); termPolicy != "" {
				sq.WithPolicy(termPolicy)
			}
			searchQuery = sq
		} else {
			sq := searchengine.NewSingleDocTypeSearch(target)
			if after := r.Form.Get("after"); after != "" {
				sq.WithSearchAfter([]interface{}{after})
			}
			if sorts := r.Form["sort"]; len(sorts) > 0 {
				for _, sort := range sorts {
					parts := strings.Split(sort, ":")
					sq.WithSort(parts[0], esquery.Order(parts[1]))
				}
			}
			sq.WithTerms(r.Form["terms"]...)
			sq.WithLedgers(r.Form["ledgers"]...)
			if termPolicy := r.Form.Get("policy"); termPolicy != "" {
				sq.WithPolicy(termPolicy)
			}
			searchQuery = sq
		}
		if r.ContentLength > 0 {
			err := json.NewDecoder(r.Body).Decode(&searchQuery)
			if err != nil {
				return nil, nil, errors.Wrap(err, "decoding query to target struct")
			}
		}
	} else {
		info = &cursorTokenInfo{}
		err := DecodeCursorToken(cursorToken, info)
		if err != nil {
			return nil, nil, err
		}
		q := searchengine.NewSingleDocTypeSearch(info.Target)
		for _, s := range info.Sort {
			q.WithSort(s.Key, s.Order)
		}
		q.WithTarget(info.Target)
		q.WithSearchAfter(info.SearchAfter)
		q.WithLedgers(info.Ledgers...)
		q.WithPageSize(info.PageSize)
		q.WithPolicy(info.TermPolicy)
		q.WithTerms(info.Terms...)
		searchQuery = q
	}

	if pageSize := r.Form.Get("pageSize"); pageSize != "" {
		pageSize, err := strconv.ParseInt(pageSize, 10, 64)
		if err != nil {
			return nil, nil, errors.Wrap(err, "parsing pageSize")
		}
		searchQuery.WithPageSize(uint64(pageSize))
	}

	switch qq := searchQuery.(type) {
	case *searchengine.SingleDocTypeSearch: // Default sort
		if len(qq.Sort) == 0 {
			// TODO: Remove the sort and ask frontend to specify the sort to be agnostic
			switch qq.Target {
			case "ACCOUNT":
				qq.WithSort("address", esquery.OrderDesc)
			case "TRANSACTION":
				qq.WithSort("txid", esquery.OrderDesc)
			case "PAYMENT":
				qq.WithSort("reference", esquery.OrderDesc)
			}
		}
	}

	return info, searchQuery, nil
}

func Handler(engine searchengine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cursor, searchQuery, err := resolveQuery(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		switch qq := searchQuery.(type) {
		case *searchengine.SingleDocTypeSearch:
			qq.PageSize++
			searchResponse, err := qq.Do(r.Context(), engine)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			reverseOrder := func(sorts ...searchengine.Sort) []searchengine.Sort {
				ret := make([]searchengine.Sort, 0)
				for _, aSort := range qq.Sort { // Use of next token, to get previous token, we need to invert the sort
					order := aSort.Order
					if order == esquery.OrderAsc {
						order = esquery.OrderDesc
					} else {
						order = esquery.OrderAsc
					}
					ret = append(ret, searchengine.Sort{
						Key:   aSort.Key,
						Order: order,
					})
				}
				return ret
			}

			items := searchResponse.Items
			var (
				hasMore bool
				reverse bool
			)
			if cursor != nil && cursor.Reverse {
				reverse = true
			}
			if len(items) > int(qq.PageSize)-1 {
				hasMore = true
				items = items[0 : qq.PageSize-1]
			}
			if reverse {
				for i := 0; i < len(items)/2; i++ {
					items[i], items[len(items)-1-i] = items[len(items)-1-i], items[i]
				}
			}

			next := ""
			if hasMore || reverse {
				item := items[len(items)-1]
				sort := qq.Sort
				if reverse {
					sort = reverseOrder(sort...)
				}
				nextTokenInfo := cursorTokenInfo{
					Target:     qq.Target,
					Sort:       sort,
					Ledgers:    qq.Ledgers,
					PageSize:   qq.PageSize - 1,
					TermPolicy: qq.TermPolicy,
					Terms:      qq.Terms,
				}
				for _, s := range qq.Sort {
					value := gjson.Get(string(item), s.Key)
					nextTokenInfo.SearchAfter = append(nextTokenInfo.SearchAfter, value.Value())
				}
				next = EncodeCursorToken(nextTokenInfo)
			}
			previous := ""
			if cursor != nil && (!reverse || (reverse && hasMore)) {
				var sort []searchengine.Sort
				if cursor.Reverse {
					sort = cursor.Sort
				} else {
					sort = reverseOrder(qq.Sort...)
				}
				prevTokenInfo := cursorTokenInfo{
					Target:     qq.Target,
					Sort:       sort,
					Ledgers:    qq.Ledgers,
					PageSize:   qq.PageSize - 1,
					TermPolicy: qq.TermPolicy,
					Reverse:    true,
					Terms:      qq.Terms,
				}
				firstItem := items[0]
				for _, s := range qq.Sort {
					value := gjson.Get(string(firstItem), s.Key)
					prevTokenInfo.SearchAfter = append(prevTokenInfo.SearchAfter, value.Value())
				}
				previous = EncodeCursorToken(prevTokenInfo)
			}

			resp := api.BaseResponse[json.RawMessage]{
				Cursor: &api.Cursor[json.RawMessage]{
					PageSize: int(math.Min(float64(qq.PageSize-1), float64(len(items)))),
					Total: &api.Total{
						Value: uint64(searchResponse.Total.Value),
						Rel:   searchResponse.Total.Relation,
					},
					HasMore:  next != "",
					Previous: previous,
					Next:     next,
					Data:     items,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				logging.GetLogger(r.Context()).Errorf("error encoding json response: %s", err)
			}

		case *searchengine.MultiDocTypeSearch:
			searchResponse, err := qq.Do(r.Context(), engine)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			resp := api.BaseResponse[searchengine.MultiDocTypeSearchResponse]{
				Data: &searchResponse,
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				logging.GetLogger(r.Context()).Errorf("error encoding json response: %s", err)
			}
		}
	}
}
