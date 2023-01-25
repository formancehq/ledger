package api

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

func validationError(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(api.ErrorResponse{
		ErrorCode:    "VALIDATION",
		ErrorMessage: err.Error(),
	}); err != nil {
		logging.GetLogger(r.Context()).Info("Error validating request: %s", err)
	}
}

func internalServerError(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	if err := json.NewEncoder(w).Encode(api.ErrorResponse{
		ErrorCode:    "INTERNAL",
		ErrorMessage: err.Error(),
	}); err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
	}
}

func writeJSONObject[T any](w http.ResponseWriter, r *http.Request, v T) {
	if err := json.NewEncoder(w).Encode(api.BaseResponse[T]{
		Data: &v,
	}); err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
	}
}

func writeCreatedJSONObject(w http.ResponseWriter, r *http.Request, v any, id string) {
	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Location", "./"+id)

	writeJSONObject(w, r, v)
}

func readJSONObject[T any](w http.ResponseWriter, r *http.Request) *T {
	var t T
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		validationError(w, r, err)
		return nil
	}
	return &t
}

func findById[T any](w http.ResponseWriter, r *http.Request, db *gorm.DB, params string) *T {
	var t T
	if err := db.WithContext(r.Context()).First(&t, "id = ?", mux.Vars(r)[params]).Error; err != nil {
		switch err {
		case gorm.ErrRecordNotFound:
			w.WriteHeader(http.StatusNotFound)
		default:
			internalServerError(w, r, err)
		}
		return nil
	}
	return &t
}

func saveObject(w http.ResponseWriter, r *http.Request, db *gorm.DB, v any) error {
	err := db.WithContext(r.Context()).Save(v).Error
	if err != nil {
		internalServerError(w, r, err)
	}
	return err
}

func createObject(w http.ResponseWriter, r *http.Request, db *gorm.DB, v any) error {
	err := db.WithContext(r.Context()).Create(v).Error
	if err != nil {
		internalServerError(w, r, err)
	}
	return err
}

func loadAssociation(w http.ResponseWriter, r *http.Request, db *gorm.DB, model any, name string, to any) error {
	err := db.
		WithContext(r.Context()).
		Model(model).
		Association(name).
		Find(to)
	if err != nil {
		internalServerError(w, r, err)
	}
	return err
}

func appendToAssociation(w http.ResponseWriter, r *http.Request, db *gorm.DB, model any, name string, item any) error {
	err := db.
		WithContext(r.Context()).
		Model(model).
		Association(name).
		Append(item)
	if err != nil {
		internalServerError(w, r, err)
	}
	return err
}

func removeFromAssociation(w http.ResponseWriter, r *http.Request, db *gorm.DB, model any, name string, item any) error {
	err := db.
		WithContext(r.Context()).
		Model(model).
		Association(name).
		Delete(item)
	if err != nil {
		internalServerError(w, r, err)
	}
	return err
}

func mapList[I any, O any](items []I, fn func(I) O) []O {
	ret := make([]O, 0)
	for _, item := range items {
		ret = append(ret, fn(item))
	}
	return ret
}
