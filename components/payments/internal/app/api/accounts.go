package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/formancehq/payments/internal/app/models"
	"github.com/formancehq/payments/internal/app/storage"

	"github.com/formancehq/go-libs/api"
	"github.com/pkg/errors"
)

type listAccountsRepository interface {
	ListAccounts(ctx context.Context, pagination storage.Paginator) ([]*models.Account, storage.PaginationDetails, error)
}

type accountResponse struct {
	ID        string             `json:"id"`
	Reference string             `json:"reference"`
	CreatedAt time.Time          `json:"createdAt"`
	Provider  string             `json:"provider"`
	Type      models.AccountType `json:"type"`
}

func listAccountsHandler(repo listAccountsRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var sorter storage.Sorter

		if sortParams := r.URL.Query()["sort"]; sortParams != nil {
			for _, s := range sortParams {
				parts := strings.SplitN(s, ":", 2)

				var order storage.SortOrder

				if len(parts) > 1 {
					//nolint:goconst // allow duplicate string
					switch parts[1] {
					case "asc", "ASC":
						order = storage.SortOrderAsc
					case "dsc", "desc", "DSC", "DESC":
						order = storage.SortOrderDesc
					default:
						handleValidationError(w, r, errors.New("sort order not well specified, got "+parts[1]))

						return
					}
				}

				column := parts[0]

				sorter.Add(column, order)
			}
		}

		pageSize, err := pageSizeQueryParam(r)
		if err != nil {
			handleValidationError(w, r, err)

			return
		}

		pagination, err := storage.Paginate(pageSize, r.URL.Query().Get("cursor"), sorter)
		if err != nil {
			handleValidationError(w, r, err)

			return
		}

		ret, paginationDetails, err := repo.ListAccounts(r.Context(), pagination)
		if err != nil {
			handleServerError(w, r, err)

			return
		}

		data := make([]*accountResponse, len(ret))

		for i := range ret {
			data[i] = &accountResponse{
				ID:        ret[i].ID.String(),
				Reference: ret[i].Reference,
				CreatedAt: ret[i].CreatedAt,
				Provider:  ret[i].Provider,
				Type:      ret[i].Type,
			}
		}

		err = json.NewEncoder(w).Encode(api.BaseResponse[*accountResponse]{
			Cursor: &api.Cursor[*accountResponse]{
				PageSize: paginationDetails.PageSize,
				HasMore:  paginationDetails.HasMore,
				Previous: paginationDetails.PreviousPage,
				Next:     paginationDetails.NextPage,
				Data:     data,
			},
		})
		if err != nil {
			handleServerError(w, r, err)

			return
		}
	}
}
