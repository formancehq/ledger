package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/controller/system"
)

// BucketInfo represents information about a bucket
type BucketInfo struct {
	Name            string   `json:"name"`
	Ledgers         []string `json:"ledgers"`
	MarkForDeletion bool     `json:"markForDeletion"`
}

// BucketListResponse represents the response for the list buckets endpoint
type BucketListResponse struct {
	Data []BucketInfo `json:"data"`
}

func listBuckets(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get all distinct buckets
		buckets, err := systemController.ListBuckets(r.Context())
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		// Create response data
		response := BucketListResponse{
			Data: make([]BucketInfo, 0, len(buckets)),
		}

		// For each bucket, get its ledgers and deletion status
		for _, bucket := range buckets {
			bucketInfo := BucketInfo{
				Name:            bucket.Name,
				Ledgers:         make([]string, 0),
				MarkForDeletion: bucket.MarkForDeletion,
			}

			// Get all ledgers for this bucket
			for _, ledger := range bucket.Ledgers {
				bucketInfo.Ledgers = append(bucketInfo.Ledgers, ledger)
			}

			response.Data = append(response.Data, bucketInfo)
		}

		api.Ok(w, response)
	}
}
