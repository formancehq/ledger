package v2

import (
	. "github.com/formancehq/go-libs/v2/collectionutils"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"io"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/query"
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

func getOffsetPaginatedQuery[v any](r *http.Request, modifiers ...func(*v) error) (*ledgercontroller.OffsetPaginatedQuery[v], error) {
	return bunpaginate.Extract[ledgercontroller.OffsetPaginatedQuery[v]](r, func() (*ledgercontroller.OffsetPaginatedQuery[v], error) {
		rq, err := getResourceQuery[v](r, modifiers...)
		if err != nil {
			return nil, err
		}

		pageSize, err := bunpaginate.GetPageSize(r, bunpaginate.WithMaxPageSize(MaxPageSize), bunpaginate.WithDefaultPageSize(DefaultPageSize))
		if err != nil {
			return nil, err
		}

		return &ledgercontroller.OffsetPaginatedQuery[v]{
			PageSize: pageSize,
			Options:  *rq,
		}, nil
	})
}

func getColumnPaginatedQuery[v any](r *http.Request, defaultPaginationColumn string, order bunpaginate.Order, modifiers ...func(*v) error) (*ledgercontroller.ColumnPaginatedQuery[v], error) {
	return bunpaginate.Extract[ledgercontroller.ColumnPaginatedQuery[v]](r, func() (*ledgercontroller.ColumnPaginatedQuery[v], error) {
		rq, err := getResourceQuery[v](r, modifiers...)
		if err != nil {
			return nil, err
		}

		pageSize, err := bunpaginate.GetPageSize(r, bunpaginate.WithMaxPageSize(MaxPageSize), bunpaginate.WithDefaultPageSize(DefaultPageSize))
		if err != nil {
			return nil, err
		}

		return &ledgercontroller.ColumnPaginatedQuery[v]{
			PageSize: pageSize,
			Column:   defaultPaginationColumn,
			Order:    pointer.For(order),
			Options:  *rq,
		}, nil
	})
}

func getResourceQuery[v any](r *http.Request, modifiers ...func(*v) error) (*ledgercontroller.ResourceQuery[v], error) {
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

	return &ledgercontroller.ResourceQuery[v]{
		PIT:     pit,
		OOT:     oot,
		Builder: builder,
		Expand:  getExpand(r),
		Opts:    options,
	}, nil
}
