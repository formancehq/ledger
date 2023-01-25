package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/logging"
)

const defaultLimit = 15

func NotFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
}

func NoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

func BadRequest(w http.ResponseWriter, code string, err error) {
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(ErrorResponse{
		ErrorCode:    code,
		ErrorMessage: err.Error(),
	}); err != nil {
		panic(err)
	}
}

func InternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	logging.GetLogger(r.Context()).Error(err)

	w.WriteHeader(http.StatusInternalServerError)
	if err := json.NewEncoder(w).Encode(ErrorResponse{
		ErrorCode:    "INTERNAL_ERROR",
		ErrorMessage: err.Error(),
	}); err != nil {
		panic(err)
	}
}

func Created(w http.ResponseWriter, v any) {
	w.WriteHeader(http.StatusCreated)
	Ok(w, v)
}

func Ok(w io.Writer, v any) {
	if err := json.NewEncoder(w).Encode(BaseResponse[any]{
		Data: &v,
	}); err != nil {
		panic(err)
	}
}

func RenderCursor[T any](w io.Writer, v Cursor[T]) {
	if err := json.NewEncoder(w).Encode(BaseResponse[T]{
		Cursor: &v,
	}); err != nil {
		panic(err)
	}
}

func CursorFromListResponse[T any, V any](w io.Writer, query ListQuery[V], response *ListResponse[T]) {
	RenderCursor(w, Cursor[T]{
		PageSize: query.Limit,
		HasMore:  response.HasMore,
		Previous: response.Previous,
		Next:     response.Next,
		Data:     response.Data,
	})
}

func ParsePaginationToken(r *http.Request) string {
	return r.URL.Query().Get("RenderCursor")
}

func ParsePageSize(r *http.Request) int {
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

func ReadPaginatedRequest[T any](r *http.Request, f func(r *http.Request) T) ListQuery[T] {
	var payload T
	if f != nil {
		payload = f(r)
	}
	return ListQuery[T]{
		Pagination: Pagination{
			Limit:           ParsePageSize(r),
			PaginationToken: ParsePaginationToken(r),
		},
		Payload: payload,
	}
}

func GetQueryMap(m map[string][]string, key string) map[string]any {
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

type ListResponse[T any] struct {
	Data           []T
	Next, Previous string
	HasMore        bool
}

type Pagination struct {
	Limit           int
	PaginationToken string
}

type ListQuery[T any] struct {
	Pagination
	Payload T
}

type Mapper[SRC any, DST any] func(src SRC) DST
