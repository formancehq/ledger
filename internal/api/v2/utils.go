package v2

import (
	"io"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"github.com/formancehq/stack/libs/go-libs/query"
)

func getPITFilter(r *http.Request) (*ledgerstore.PITFilter, error) {
	pitString := r.URL.Query().Get("pit")
	if pitString == "" {
		return &ledgerstore.PITFilter{
			PIT: pointer.For(time.Now()),
		}, nil
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
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	if len(data) > 0 {
		return query.ParseJSON(string(data))
	}
	return nil, nil
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

	pageSize, err := bunpaginate.GetPageSize(r)
	if err != nil {
		return nil, err
	}

	return pointer.For(ledgerstore.NewPaginatedQueryOptions(*pitFilter).
		WithQueryBuilder(qb).
		WithPageSize(pageSize)), nil
}
