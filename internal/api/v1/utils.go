package v1

import (
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"strings"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
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

	return &storagecommon.ResourceQuery[v]{
		Expand: r.URL.Query()["expand"],
		Opts:   options,
	}, nil
}
