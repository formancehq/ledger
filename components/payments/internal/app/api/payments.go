package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/payments/internal/app/models"
	"github.com/formancehq/payments/internal/app/storage"

	"github.com/formancehq/go-libs/api"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

type listPaymentsRepository interface {
	ListPayments(ctx context.Context, pagination storage.Paginator) ([]*models.Payment, storage.PaginationDetails, error)
}

type paymentResponse struct {
	ID            string                   `json:"id"`
	Reference     string                   `json:"reference"`
	AccountID     string                   `json:"accountID"`
	Type          string                   `json:"type"`
	Provider      models.ConnectorProvider `json:"provider"`
	Status        models.PaymentStatus     `json:"status"`
	InitialAmount int64                    `json:"initialAmount"`
	Scheme        models.PaymentScheme     `json:"scheme"`
	Asset         string                   `json:"asset"`
	CreatedAt     time.Time                `json:"createdAt"`
	Raw           interface{}              `json:"raw"`
	Adjustments   []paymentAdjustment      `json:"adjustments"`
	Metadata      []paymentMetadata        `json:"metadata"`
}

type paymentMetadata struct {
	Key       string                     `json:"key"`
	Value     string                     `json:"value"`
	Changelog []paymentMetadataChangelog `json:"changelog"`
}

type paymentMetadataChangelog struct {
	Timestamp string `json:"timestamp"`
	Value     string `json:"value"`
}

type paymentAdjustment struct {
	Status   models.PaymentStatus `json:"status" bson:"status"`
	Amount   int64                `json:"amount" bson:"amount"`
	Date     time.Time            `json:"date" bson:"date"`
	Raw      interface{}          `json:"raw" bson:"raw"`
	Absolute bool                 `json:"absolute" bson:"absolute"`
}

func listPaymentsHandler(repo listPaymentsRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var sorter storage.Sorter

		if sortParams := r.URL.Query()["sort"]; sortParams != nil {
			for _, s := range sortParams {
				parts := strings.SplitN(s, ":", 2)

				var order storage.SortOrder

				if len(parts) > 1 {
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

		ret, paginationDetails, err := repo.ListPayments(r.Context(), pagination)
		if err != nil {
			handleServerError(w, r, err)

			return
		}

		data := make([]*paymentResponse, len(ret))

		for i := range ret {
			data[i] = &paymentResponse{
				ID:            ret[i].ID.String(),
				Reference:     ret[i].Reference,
				Type:          ret[i].Type.String(),
				Provider:      ret[i].Connector.Provider,
				Status:        ret[i].Status,
				InitialAmount: ret[i].Amount,
				Scheme:        ret[i].Scheme,
				Asset:         ret[i].Asset.String(),
				CreatedAt:     ret[i].CreatedAt,
				Raw:           ret[i].RawData,
				Adjustments:   make([]paymentAdjustment, len(ret[i].Adjustments)),
			}

			if ret[i].AccountID != uuid.Nil {
				data[i].AccountID = ret[i].AccountID.String()
			}

			for adjustmentIdx := range ret[i].Adjustments {
				data[i].Adjustments[adjustmentIdx] = paymentAdjustment{
					Status:   ret[i].Adjustments[adjustmentIdx].Status,
					Amount:   ret[i].Adjustments[adjustmentIdx].Amount,
					Date:     ret[i].Adjustments[adjustmentIdx].CreatedAt,
					Raw:      ret[i].Adjustments[adjustmentIdx].RawData,
					Absolute: ret[i].Adjustments[adjustmentIdx].Absolute,
				}
			}

			for metadataIDx := range ret[i].Metadata {
				data[i].Metadata = append(data[i].Metadata,
					paymentMetadata{
						Key:   ret[i].Metadata[metadataIDx].Key,
						Value: ret[i].Metadata[metadataIDx].Value,
					})

				for changelogIdx := range ret[i].Metadata[metadataIDx].Changelog {
					data[i].Metadata[metadataIDx].Changelog = append(data[i].Metadata[metadataIDx].Changelog,
						paymentMetadataChangelog{
							Timestamp: ret[i].Metadata[metadataIDx].Changelog[changelogIdx].CreatedAt.Format(time.RFC3339),
							Value:     ret[i].Metadata[metadataIDx].Changelog[changelogIdx].Value,
						})
				}
			}
		}

		err = json.NewEncoder(w).Encode(api.BaseResponse[*paymentResponse]{
			Cursor: &api.Cursor[*paymentResponse]{
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

type readPaymentRepository interface {
	GetPayment(ctx context.Context, id string) (*models.Payment, error)
}

func readPaymentHandler(repo readPaymentRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		paymentID := mux.Vars(r)["paymentID"]

		payment, err := repo.GetPayment(r.Context(), paymentID)
		if err != nil {
			handleServerError(w, r, err)

			return
		}

		data := paymentResponse{
			ID:            payment.ID.String(),
			Reference:     payment.Reference,
			Type:          payment.Type.String(),
			Provider:      payment.Connector.Provider,
			Status:        payment.Status,
			InitialAmount: payment.Amount,
			Scheme:        payment.Scheme,
			Asset:         payment.Asset.String(),
			CreatedAt:     payment.CreatedAt,
			Raw:           payment.RawData,
			Adjustments:   make([]paymentAdjustment, len(payment.Adjustments)),
			Metadata:      make([]paymentMetadata, len(payment.Metadata)),
		}

		if payment.AccountID != uuid.Nil {
			data.AccountID = payment.AccountID.String()
		}

		for i := range payment.Adjustments {
			data.Adjustments[i] = paymentAdjustment{
				Status:   payment.Adjustments[i].Status,
				Amount:   payment.Adjustments[i].Amount,
				Date:     payment.Adjustments[i].CreatedAt,
				Raw:      payment.Adjustments[i].RawData,
				Absolute: payment.Adjustments[i].Absolute,
			}
		}

		for metadataIDx := range payment.Metadata {
			data.Metadata = append(data.Metadata,
				paymentMetadata{
					Key:   payment.Metadata[metadataIDx].Key,
					Value: payment.Metadata[metadataIDx].Value,
				})

			for changelogIdx := range payment.Metadata[metadataIDx].Changelog {
				data.Metadata[metadataIDx].Changelog = append(data.Metadata[metadataIDx].Changelog,
					paymentMetadataChangelog{
						Timestamp: payment.Metadata[metadataIDx].Changelog[changelogIdx].CreatedAt.Format(time.RFC3339),
						Value:     payment.Metadata[metadataIDx].Changelog[changelogIdx].Value,
					})
			}
		}

		err = json.NewEncoder(w).Encode(api.BaseResponse[paymentResponse]{
			Data: &data,
		})
		if err != nil {
			handleServerError(w, r, err)

			return
		}
	}
}
