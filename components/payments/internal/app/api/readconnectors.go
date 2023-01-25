package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/api"
)

type readConnectorsRepository interface {
	ListConnectors(ctx context.Context) ([]*models.Connector, error)
}

type readConnectorsResponseElement struct {
	Provider models.ConnectorProvider `json:"provider" bson:"provider"`
	Enabled  bool                     `json:"enabled" bson:"enabled"`

	// TODO: remove disabled field when frontend switches to using enabled
	Disabled bool `json:"disabled" bson:"disabled"`
}

func readConnectorsHandler(repo readConnectorsRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := repo.ListConnectors(r.Context())
		if err != nil {
			handleError(w, r, err)

			return
		}

		data := make([]readConnectorsResponseElement, len(res))

		for i := range res {
			data[i] = readConnectorsResponseElement{
				Provider: res[i].Provider,
				Enabled:  res[i].Enabled,
				Disabled: !res[i].Enabled,
			}
		}

		err = json.NewEncoder(w).Encode(
			api.BaseResponse[[]readConnectorsResponseElement]{
				Data: &data,
			})
		if err != nil {
			panic(err)
		}
	}
}
