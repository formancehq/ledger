package v1

import (
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/ledger/internal/storage/bunpaginate"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
)

func getPITFilter(r *http.Request) (*ledgerstore.PITFilter, error) {
	pitString := r.URL.Query().Get("pit")
	if pitString == "" {
		return &ledgerstore.PITFilter{}, nil
	}
	pit, err := time.ParseTime(pitString)
	if err != nil {
		return nil, err
	}
	return &ledgerstore.PITFilter{
		PIT: &pit,
	}, nil
}

func getPITFilterWithVolumes(r *http.Request) (*ledgerstore.PITFilterWithVolumes, error) {
	pit, err := getPITFilter(r)
	if err != nil {
		return nil, err
	}
	return &ledgerstore.PITFilterWithVolumes{
		PITFilter:              *pit,
		ExpandVolumes:          collectionutils.Contains(r.URL.Query()["expand"], "volumes"),
		ExpandEffectiveVolumes: collectionutils.Contains(r.URL.Query()["expand"], "effectiveVolumes"),
	}, nil
}

func getQueryBuilder(r *http.Request) (query.Builder, error) {
	return query.ParseJSON(r.URL.Query().Get("query"))
}

func getPaginatedQueryOptionsOfPITFilterWithVolumes(r *http.Request) (*ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilterWithVolumes], error) {
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

	return pointer.For(ledgerstore.NewPaginatedQueryOptions(*pitFilter).
		WithQueryBuilder(qb).
		WithPageSize(pageSize)), nil
}

func getCommandParameters(r *http.Request) command.Parameters {
	dryRunAsString := r.URL.Query().Get("preview")
	dryRun := strings.ToUpper(dryRunAsString) == "YES" || strings.ToUpper(dryRunAsString) == "TRUE" || dryRunAsString == "1"

	idempotencyKey := r.Header.Get("Idempotency-Key")

	return command.Parameters{
		DryRun:         dryRun,
		IdempotencyKey: idempotencyKey,
	}
}
