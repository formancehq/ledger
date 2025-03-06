// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package operations

import (
	"github.com/formancehq/ledger/pkg/client/models/components"
)

type V2CreatePipelineRequest struct {
	// Name of the ledger.
	Ledger                  string                              `pathParam:"style=simple,explode=false,name=ledger"`
	V2CreatePipelineRequest *components.V2CreatePipelineRequest `request:"mediaType=application/json"`
}

func (o *V2CreatePipelineRequest) GetLedger() string {
	if o == nil {
		return ""
	}
	return o.Ledger
}

func (o *V2CreatePipelineRequest) GetV2CreatePipelineRequest() *components.V2CreatePipelineRequest {
	if o == nil {
		return nil
	}
	return o.V2CreatePipelineRequest
}

type V2CreatePipelineResponse struct {
	HTTPMeta components.HTTPMetadata `json:"-"`
	// Created ipeline
	V2CreatePipelineResponse *components.V2CreatePipelineResponse
}

func (o *V2CreatePipelineResponse) GetHTTPMeta() components.HTTPMetadata {
	if o == nil {
		return components.HTTPMetadata{}
	}
	return o.HTTPMeta
}

func (o *V2CreatePipelineResponse) GetV2CreatePipelineResponse() *components.V2CreatePipelineResponse {
	if o == nil {
		return nil
	}
	return o.V2CreatePipelineResponse
}
