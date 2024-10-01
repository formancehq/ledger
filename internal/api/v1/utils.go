package v1

import (
	"net/http"
	"strings"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/query"
)

func getPITFilter(r *http.Request) (*ledgercontroller.PITFilter, error) {
	pitString := r.URL.Query().Get("pit")
	if pitString == "" {
		return &ledgercontroller.PITFilter{}, nil
	}
	pit, err := time.ParseTime(pitString)
	if err != nil {
		return nil, err
	}
	return &ledgercontroller.PITFilter{
		PIT: &pit,
	}, nil
}

func getPITFilterWithVolumes(r *http.Request) (*ledgercontroller.PITFilterWithVolumes, error) {
	pit, err := getPITFilter(r)
	if err != nil {
		return nil, err
	}
	return &ledgercontroller.PITFilterWithVolumes{
		PITFilter:              *pit,
		ExpandVolumes:          collectionutils.Contains(r.URL.Query()["expand"], "volumes"),
		ExpandEffectiveVolumes: collectionutils.Contains(r.URL.Query()["expand"], "effectiveVolumes"),
	}, nil
}

func getQueryBuilder(r *http.Request) (query.Builder, error) {
	return query.ParseJSON(r.URL.Query().Get("query"))
}

func getPaginatedQueryOptionsOfPITFilterWithVolumes(r *http.Request) (*ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], error) {
	qb, err := getQueryBuilder(r)
	if err != nil {
		return nil, err
	}

	pitFilter, err := getPITFilterWithVolumes(r)
	if err != nil {
		return nil, err
	}

	pageSize, err := bunpaginate.GetPageSize(r, bunpaginate.WithMaxPageSize(MaxPageSize), bunpaginate.WithDefaultPageSize(DefaultPageSize))
	if err != nil {
		return nil, err
	}

	return pointer.For(ledgercontroller.NewPaginatedQueryOptions(*pitFilter).
		WithQueryBuilder(qb).
		WithPageSize(pageSize)), nil
}

func getCommandParameters[INPUT any](r *http.Request, input INPUT) ledgercontroller.Parameters[INPUT] {
	dryRunAsString := r.URL.Query().Get("preview")
	dryRun := strings.ToUpper(dryRunAsString) == "YES" || strings.ToUpper(dryRunAsString) == "TRUE" || dryRunAsString == "1"

	idempotencyKey := r.Header.Get("Idempotency-Key")

	return ledgercontroller.Parameters[INPUT]{
		DryRun:         dryRun,
		IdempotencyKey: idempotencyKey,
		Input: input,
	}
}
