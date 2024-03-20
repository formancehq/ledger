package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/stack/libs/go-libs/logging"
)

const (
	defaultLimit = 15

	ErrorCodeNotFound = "NOT_FOUND"
	ErrorInternal     = "INTERNAL"
)

func writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if v != nil {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			panic(err)
		}
	}
}

func WriteErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	writeJSON(w, statusCode, ErrorResponse{
		ErrorCode:    errorCode,
		ErrorMessage: err.Error(),
	})
}

func NotFound(w http.ResponseWriter, err error) {
	WriteErrorResponse(w, http.StatusNotFound, ErrorCodeNotFound, err)
}

func NoContent(w http.ResponseWriter) {
	writeJSON(w, http.StatusNoContent, nil)
}

func Forbidden(w http.ResponseWriter, code string, err error) {
	WriteErrorResponse(w, http.StatusForbidden, code, err)
}

func BadRequest(w http.ResponseWriter, code string, err error) {
	WriteErrorResponse(w, http.StatusBadRequest, code, err)
}

func BadRequestWithDetails(w http.ResponseWriter, code string, err error, details string) {
	writeJSON(w, http.StatusBadRequest, ErrorResponse{
		ErrorCode:    code,
		ErrorMessage: err.Error(),
		Details:      details,
	})
}

func InternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	logging.FromContext(r.Context()).Error(err)
	WriteErrorResponse(w, http.StatusInternalServerError, ErrorInternal, err)
}

func Accepted(w http.ResponseWriter, v any) {
	writeJSON(w, http.StatusAccepted, BaseResponse[any]{
		Data: &v,
	})
}
func Created(w http.ResponseWriter, v any) {
	writeJSON(w, http.StatusCreated, BaseResponse[any]{
		Data: &v,
	})
}

func RawOk(w http.ResponseWriter, v any) {
	writeJSON(w, http.StatusOK, v)
}

func Ok(w http.ResponseWriter, v any) {
	writeJSON(w, http.StatusOK, BaseResponse[any]{
		Data: &v,
	})
}

func RenderCursor[T any](w http.ResponseWriter, v bunpaginate.Cursor[T]) {
	writeJSON(w, http.StatusOK, BaseResponse[T]{
		Cursor: &v,
	})
}

func WriteResponse(w http.ResponseWriter, status int, body []byte) {
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		panic(err)
	}
}

func CursorFromListResponse[T any, V any](w http.ResponseWriter, query ListQuery[V], response *ListResponse[T]) {
	RenderCursor(w, bunpaginate.Cursor[T]{
		PageSize: query.Limit,
		HasMore:  response.HasMore,
		Previous: response.Previous,
		Next:     response.Next,
		Data:     response.Data,
	})
}

func ParsePaginationToken(r *http.Request) string {
	return r.URL.Query().Get("paginationToken")
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

func GetQueryMap(m map[string][]string, key string) map[string]string {
	dicts := make(map[string]string)
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
