// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package operations

import (
	"github.com/formancehq/ledger/pkg/client/models/components"
)

type V2ListPipelinesRequest struct {
	// Name of the ledger.
	Ledger string `pathParam:"style=simple,explode=false,name=ledger"`
}

func (o *V2ListPipelinesRequest) GetLedger() string {
	if o == nil {
		return ""
	}
	return o.Ledger
}

type V2ListPipelinesResponse struct {
	HTTPMeta components.HTTPMetadata `json:"-"`
	// Pipelines list
	V2ListPipelinesResponse *components.V2ListPipelinesResponse
}

func (o *V2ListPipelinesResponse) GetHTTPMeta() components.HTTPMetadata {
	if o == nil {
		return components.HTTPMetadata{}
	}
	return o.HTTPMeta
}

func (o *V2ListPipelinesResponse) GetV2ListPipelinesResponse() *components.V2ListPipelinesResponse {
	if o == nil {
		return nil
	}
	return o.V2ListPipelinesResponse
}
