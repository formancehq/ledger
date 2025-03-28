package v1

import (
	"github.com/formancehq/ledger/internal/storage/resources"
	"net/http"
	"strings"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"

	"github.com/formancehq/go-libs/v2/pointer"
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

func getOffsetPaginatedQuery[v any](r *http.Request, modifiers ...func(*v) error) (*resources.OffsetPaginatedQuery[v], error) {
	return bunpaginate.Extract[resources.OffsetPaginatedQuery[v]](r, func() (*resources.OffsetPaginatedQuery[v], error) {
		rq, err := getResourceQuery[v](r, modifiers...)
		if err != nil {
			return nil, err
		}

		pageSize, err := bunpaginate.GetPageSize(r, bunpaginate.WithMaxPageSize(MaxPageSize), bunpaginate.WithDefaultPageSize(DefaultPageSize))
		if err != nil {
			return nil, err
		}

		return &resources.OffsetPaginatedQuery[v]{
			PageSize: pageSize,
			Options:  *rq,
		}, nil
	})
}

func getColumnPaginatedQuery[v any](r *http.Request, column string, order bunpaginate.Order, modifiers ...func(*v) error) (*resources.ColumnPaginatedQuery[v], error) {
	return bunpaginate.Extract[resources.ColumnPaginatedQuery[v]](r, func() (*resources.ColumnPaginatedQuery[v], error) {
		rq, err := getResourceQuery[v](r, modifiers...)
		if err != nil {
			return nil, err
		}

		pageSize, err := bunpaginate.GetPageSize(r, bunpaginate.WithMaxPageSize(MaxPageSize), bunpaginate.WithDefaultPageSize(DefaultPageSize))
		if err != nil {
			return nil, err
		}

		return &resources.ColumnPaginatedQuery[v]{
			PageSize: pageSize,
			Column:   column,
			Order:    pointer.For(order),
			Options:  *rq,
		}, nil
	})
}

func getResourceQuery[v any](r *http.Request, modifiers ...func(*v) error) (*resources.ResourceQuery[v], error) {
	var options v
	for _, modifier := range modifiers {
		if err := modifier(&options); err != nil {
			return nil, err
		}
	}

	return &resources.ResourceQuery[v]{
		Expand: r.URL.Query()["expand"],
		Opts:   options,
	}, nil
}
