package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/api"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/pkg/errors"

	stripeConnector "github.com/formancehq/payments/internal/app/connectors/stripe"
	"github.com/stripe/stripe-go/v72"
	"github.com/stripe/stripe-go/v72/transfer"
)

type stripeTransferRequest struct {
	Amount      int64             `json:"amount"`
	Asset       string            `json:"asset"`
	Destination string            `json:"destination"`
	Metadata    map[string]string `json:"metadata"`

	currency string
}

func (req *stripeTransferRequest) validate() error {
	if req.Amount <= 0 {
		return errors.New("amount must be greater than 0")
	}

	if req.Asset == "" {
		return errors.New("asset is required")
	}

	if req.Asset != "USD/2" && req.Asset != "EUR/2" {
		return errors.New("asset must be USD/2 or EUR/2")
	}

	req.currency = req.Asset[:3]

	if req.Destination == "" {
		return errors.New("destination is required")
	}

	return nil
}

type stripeTransfersRepository interface {
	GetConfig(ctx context.Context, connectorName models.ConnectorProvider, cfg any) error
}

func handleStripeTransfers(repo stripeTransfersRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var cfg stripeConnector.Config

		if err := repo.GetConfig(r.Context(), stripeConnector.Name, &cfg); err != nil {
			handleError(w, r, err)

			return
		}

		stripe.Key = cfg.APIKey

		var transferRequest stripeTransferRequest

		err := json.NewDecoder(r.Body).Decode(&transferRequest)
		if err != nil {
			handleError(w, r, err)

			return
		}

		err = transferRequest.validate()
		if err != nil {
			handleError(w, r, err)

			return
		}

		params := &stripe.TransferParams{
			Params: stripe.Params{
				Context: r.Context(),
			},
			Amount:      stripe.Int64(transferRequest.Amount),
			Currency:    stripe.String(transferRequest.currency),
			Destination: stripe.String(transferRequest.Destination),
		}

		for k, v := range transferRequest.Metadata {
			params.AddMetadata(k, v)
		}

		transferResponse, err := transfer.New(params)
		if err != nil {
			handleServerError(w, r, err)

			return
		}

		err = json.NewEncoder(w).Encode(api.BaseResponse[stripe.Transfer]{
			Data: transferResponse,
		})
		if err != nil {
			handleServerError(w, r, err)

			return
		}
	}
}
