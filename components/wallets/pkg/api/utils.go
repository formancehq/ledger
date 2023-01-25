package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	sharedapi "github.com/formancehq/go-libs/api"
	sharedlogging "github.com/formancehq/go-libs/logging"
	wallet "github.com/formancehq/wallets/pkg"
)

const defaultLimit = 15

func notFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
}

func noContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

func badRequest(w http.ResponseWriter, code string, err error) {
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(sharedapi.ErrorResponse{
		ErrorCode:    code,
		ErrorMessage: err.Error(),
	}); err != nil {
		panic(err)
	}
}

func internalError(w http.ResponseWriter, r *http.Request, err error) {
	sharedlogging.GetLogger(r.Context()).Error(err)

	w.WriteHeader(http.StatusInternalServerError)
	if err := json.NewEncoder(w).Encode(sharedapi.ErrorResponse{
		ErrorCode:    "INTERNAL_ERROR",
		ErrorMessage: err.Error(),
	}); err != nil {
		panic(err)
	}
}

func created(w http.ResponseWriter, v any) {
	w.WriteHeader(http.StatusCreated)
	ok(w, v)
}

func ok(w io.Writer, v any) {
	if err := json.NewEncoder(w).Encode(sharedapi.BaseResponse[any]{
		Data: &v,
	}); err != nil {
		panic(err)
	}
}

func cursor[T any](w io.Writer, v sharedapi.Cursor[T]) {
	if err := json.NewEncoder(w).Encode(sharedapi.BaseResponse[T]{
		Cursor: &v,
	}); err != nil {
		panic(err)
	}
}

func cursorFromListResponse[T any, V any](w io.Writer, query wallet.ListQuery[V], response *wallet.ListResponse[T]) {
	cursor(w, sharedapi.Cursor[T]{
		PageSize: query.Limit,
		HasMore:  response.HasMore,
		Previous: response.Previous,
		Next:     response.Next,
		Data:     response.Data,
	})
}

func parsePaginationToken(r *http.Request) string {
	return r.URL.Query().Get("cursor")
}

func parsePageSize(r *http.Request) int {
	pageSize := r.URL.Query().Get("pageSize")
	if pageSize == "" {
		return defaultLimit
	}

	v, err := strconv.ParseInt(pageSize, 10, 32)
	if err != nil {
		panic(err)
	}
	return int(v)
}

func readPaginatedRequest[T any](r *http.Request, f func(r *http.Request) T) wallet.ListQuery[T] {
	var payload T
	if f != nil {
		payload = f(r)
	}
	return wallet.ListQuery[T]{
		Pagination: wallet.Pagination{
			Limit:           parsePageSize(r),
			PaginationToken: parsePaginationToken(r),
		},
		Payload: payload,
	}
}

func getQueryMap(m map[string][]string, key string) map[string]any {
	dicts := make(map[string]any)
	for k, v := range m {
		if i := strings.IndexByte(k, '['); i >= 1 && k[0:i] == key {
			if j := strings.IndexByte(k[i+1:], ']'); j >= 1 {
				dicts[k[i+1:][:j]] = v[0]
			}
		}
	}
	return dicts
}
