package v2

import (
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"io"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/go-libs/v3/query"
)

func getDate(r *http.Request, key string) (*time.Time, error) {
	dateString := r.URL.Query().Get(key)

	if dateString == "" {
		return nil, nil
	}

	date, err := time.ParseTime(dateString)
	if err != nil {
		return nil, err
	}

	return &date, nil
}

func getPIT(r *http.Request) (*time.Time, error) {
	return getDate(r, "pit")
}

func getOOT(r *http.Request) (*time.Time, error) {
	return getDate(r, "oot")
}

func getQueryBuilder(r *http.Request) (query.Builder, error) {
	q := r.URL.Query().Get("query")
	if q == "" {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		q = string(data)
	}

	if len(q) > 0 {
		return query.ParseJSON(q)
	}
	return nil, nil
}

func getExpand(r *http.Request) []string {
	return Flatten(
		Map(r.URL.Query()["expand"], func(from string) []string {
			return strings.Split(from, ",")
		}),
	)
}

func getPaginatedQuery[Options any](
	r *http.Request,
	paginationConfig common.PaginationConfig,
	column string,
	order bunpaginate.Order,
	modifiers ...func(resourceQuery *storagecommon.ResourceQuery[Options]),
) (storagecommon.PaginatedQuery[Options], error) {
	return storagecommon.Extract[Options](
		r,
		func() (*storagecommon.InitialPaginatedQuery[Options], error) {
			rq, err := getResourceQuery[Options](r)
			if err != nil {
				return nil, err
			}

			for _, modifier := range modifiers {
				modifier(rq)
			}

			pageSize, err := bunpaginate.GetPageSize(
				r,
				bunpaginate.WithMaxPageSize(paginationConfig.MaxPageSize),
				bunpaginate.WithDefaultPageSize(paginationConfig.DefaultPageSize),
			)
			if err != nil {
				return nil, err
			}

			return &storagecommon.InitialPaginatedQuery[Options]{
				Column:   column,
				Order:    &order,
				PageSize: pageSize,
				Options:  *rq,
			}, nil
		},
		func(query *storagecommon.InitialPaginatedQuery[Options]) error {
			var err error
			query.PageSize, err = bunpaginate.GetPageSize(
				r,
				bunpaginate.WithMaxPageSize(paginationConfig.MaxPageSize),
				bunpaginate.WithDefaultPageSize(query.PageSize),
			)
			return err
		},
	)
}

func getResourceQuery[v any](r *http.Request, modifiers ...func(*v) error) (*storagecommon.ResourceQuery[v], error) {
	pit, err := getPIT(r)
	if err != nil {
		return nil, err
	}
	oot, err := getOOT(r)
	if err != nil {
		return nil, err
	}
	builder, err := getQueryBuilder(r)
	if err != nil {
		return nil, err
	}

	var options v
	for _, modifier := range modifiers {
		if err := modifier(&options); err != nil {
			return nil, err
		}
	}

	return &storagecommon.ResourceQuery[v]{
		PIT:     pit,
		OOT:     oot,
		Builder: builder,
		Expand:  getExpand(r),
		Opts:    options,
	}, nil
}
