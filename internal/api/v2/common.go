package v2

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/iancoleman/strcase"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	. "github.com/formancehq/go-libs/v4/collectionutils"
	"github.com/formancehq/go-libs/v4/query"
	"github.com/formancehq/go-libs/v4/time"

	storagecommon "github.com/formancehq/ledger/internal/storage/common"
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

func getPipelineID(r *http.Request) string {
	return chi.URLParam(r, "pipelineID")
}

func getExporterID(r *http.Request) string {
	return chi.URLParam(r, "exporterID")
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
	paginationConfig storagecommon.PaginationConfig,
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

			if sort := r.URL.Query().Get("sort"); sort != "" {
				parts := strings.SplitN(sort, ":", 2)
				column = strcase.ToSnake(parts[0])
				if len(parts) > 1 {
					switch {
					case strings.ToLower(parts[1]) == "desc":
						order = bunpaginate.OrderDesc
					case strings.ToLower(parts[1]) == "asc":
						order = bunpaginate.OrderAsc
					default:
						return nil, fmt.Errorf("invalid order: %s", parts[1])
					}
				}
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
