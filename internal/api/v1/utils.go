package v1

import (
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/time"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func getCommandParameters[INPUT any](r *http.Request, input INPUT) ledgercontroller.Parameters[INPUT] {
	dryRunAsString := r.URL.Query().Get("preview")
	dryRun := strings.ToUpper(dryRunAsString) == "YES" || strings.ToUpper(dryRunAsString) == "TRUE" || dryRunAsString == "1"

	idempotencyKey := r.Header.Get("Idempotency-Key")

	return ledgercontroller.Parameters[INPUT]{
		DryRun:         dryRun,
		IdempotencyKey: idempotencyKey,
		Input:          input,
	}
}

func getPaginatedQuery[Options any](
	r *http.Request,
	defaultColumn string,
	defaultOrder bunpaginate.Order,
	modifiers ...func(resourceQuery *storagecommon.ResourceQuery[Options]) error,
) (storagecommon.PaginatedQuery[Options], error) {

	return storagecommon.Extract[Options](
		r,
		func() (*storagecommon.InitialPaginatedQuery[Options], error) {
			rq, err := getResourceQuery[Options](r)
			if err != nil {
				return nil, err
			}

			for _, modifier := range modifiers {
				if err := modifier(rq); err != nil {
					return nil, err
				}
			}

			pageSize, err := bunpaginate.GetPageSize(
				r,
				bunpaginate.WithMaxPageSize(MaxPageSize),
				bunpaginate.WithDefaultPageSize(DefaultPageSize),
			)
			if err != nil {
				return nil, err
			}

			return &storagecommon.InitialPaginatedQuery[Options]{
				Column:   defaultColumn,
				Order:    &defaultOrder,
				PageSize: pageSize,
				Options:  *rq,
			}, nil
		},
		func(query *storagecommon.InitialPaginatedQuery[Options]) error {
			var err error
			query.PageSize, err = bunpaginate.GetPageSize(
				r,
				bunpaginate.WithMaxPageSize(MaxPageSize),
				bunpaginate.WithDefaultPageSize(query.PageSize),
			)
			return err
		},
	)
}

func getResourceQuery[v any](r *http.Request, modifiers ...func(*v) error) (*storagecommon.ResourceQuery[v], error) {
	var options v
	for _, modifier := range modifiers {
		if err := modifier(&options); err != nil {
			return nil, err
		}
	}

	pit, err := getPIT(r)
	if err != nil {
		return nil, err
	}

	oot, err := getOOT(r)
	if err != nil {
		return nil, err
	}

	return &storagecommon.ResourceQuery[v]{
		Expand: r.URL.Query()["expand"],
		Opts:   options,
		PIT:    pit,
		OOT:    oot,
	}, nil
}

func getPIT(r *http.Request) (*time.Time, error) {
	return getDate(r, "pit")
}

func getOOT(r *http.Request) (*time.Time, error) {
	return getDate(r, "oot")
}

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
