package v2

import (
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/api"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/query"
)

func getPITOOTFilter(r *http.Request) (*ledgercontroller.PITFilter, error) {
	pitString := r.URL.Query().Get("endTime")
	ootString := r.URL.Query().Get("startTime")

	pit := time.Time{}
	oot := time.Time{}

	if pitString != "" {
		var err error
		pit, err = time.ParseTime(pitString)
		if err != nil {
			return nil, err
		}
	}

	if ootString != "" {
		var err error
		oot, err = time.ParseTime(ootString)
		if err != nil {
			return nil, err
		}
	}

	return &ledgercontroller.PITFilter{
		PIT: &pit,
		OOT: &oot,
	}, nil
}

func getPITFilter(r *http.Request) (*ledgercontroller.PITFilter, error) {
	pitString := r.URL.Query().Get("pit")

	pit := time.Time{}
	if pitString != "" {
		var err error
		pit, err = time.ParseTime(pitString)
		if err != nil {
			return nil, err
		}
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
		ExpandVolumes:          hasExpandVolumes(r),
		ExpandEffectiveVolumes: hasExpandEffectiveVolumes(r),
	}, nil
}

func hasExpandVolumes(r *http.Request) bool {
	parts := strings.Split(r.URL.Query().Get("expand"), ",")
	return slices.Contains(parts, "volumes")
}

func hasExpandEffectiveVolumes(r *http.Request) bool {
	parts := strings.Split(r.URL.Query().Get("expand"), ",")
	return slices.Contains(parts, "effectiveVolumes")
}

func getFiltersForVolumes(r *http.Request) (*ledgercontroller.FiltersForVolumes, error) {
	pit, err := getPITOOTFilter(r)
	if err != nil {
		return nil, err
	}

	useInsertionDate := api.QueryParamBool(r, "insertionDate")
	groupLvl := 0

	groupLvlStr := r.URL.Query().Get("groupBy")
	if groupLvlStr != "" {
		groupLvlInt, err := strconv.Atoi(groupLvlStr)
		if err != nil {
			return nil, err
		}
		if groupLvlInt > 0 {
			groupLvl = groupLvlInt
		}
	}
	return &ledgercontroller.FiltersForVolumes{
		PITFilter:        *pit,
		UseInsertionDate: useInsertionDate,
		GroupLvl:         groupLvl,
	}, nil
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

func getPaginatedQueryOptionsOfPITFilterWithVolumes(r *http.Request) (*ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], error) {
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

	return pointer.For(ledgercontroller.NewPaginatedQueryOptions(*pitFilter).
		WithQueryBuilder(qb).
		WithPageSize(pageSize)), nil
}

func getPaginatedQueryOptionsOfFiltersForVolumes(r *http.Request) (*ledgercontroller.PaginatedQueryOptions[ledgercontroller.FiltersForVolumes], error) {
	qb, err := getQueryBuilder(r)
	if err != nil {
		return nil, err
	}

	filtersForVolumes, err := getFiltersForVolumes(r)
	if err != nil {
		return nil, err
	}

	pageSize, err := bunpaginate.GetPageSize(r)
	if err != nil {
		return nil, err
	}

	return pointer.For(ledgercontroller.NewPaginatedQueryOptions(*filtersForVolumes).
		WithPageSize(pageSize).
		WithQueryBuilder(qb)), nil
}
