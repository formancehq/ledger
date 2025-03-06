// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package operations

import (
	"github.com/formancehq/ledger/pkg/client/models/components"
)

type V2StartPipelineRequest struct {
	// Name of the ledger.
	Ledger string `pathParam:"style=simple,explode=false,name=ledger"`
	// The pipeline id
	PipelineID string `pathParam:"style=simple,explode=false,name=pipelineID"`
}

func (o *V2StartPipelineRequest) GetLedger() string {
	if o == nil {
		return ""
	}
	return o.Ledger
}

func (o *V2StartPipelineRequest) GetPipelineID() string {
	if o == nil {
		return ""
	}
	return o.PipelineID
}

type V2StartPipelineResponse struct {
	HTTPMeta components.HTTPMetadata `json:"-"`
}

func (o *V2StartPipelineResponse) GetHTTPMeta() components.HTTPMetadata {
	if o == nil {
		return components.HTTPMetadata{}
	}
	return o.HTTPMeta
}
