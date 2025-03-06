// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package operations

import (
	"github.com/formancehq/ledger/pkg/client/models/components"
)

type V2GetConnectorStateRequest struct {
	// The connector id
	ConnectorID string `pathParam:"style=simple,explode=false,name=connectorID"`
}

func (o *V2GetConnectorStateRequest) GetConnectorID() string {
	if o == nil {
		return ""
	}
	return o.ConnectorID
}

type V2GetConnectorStateResponse struct {
	HTTPMeta components.HTTPMetadata `json:"-"`
	// Connector information
	V2GetConnectorStateResponse *components.V2GetConnectorStateResponse
}

func (o *V2GetConnectorStateResponse) GetHTTPMeta() components.HTTPMetadata {
	if o == nil {
		return components.HTTPMetadata{}
	}
	return o.HTTPMeta
}

func (o *V2GetConnectorStateResponse) GetV2GetConnectorStateResponse() *components.V2GetConnectorStateResponse {
	if o == nil {
		return nil
	}
	return o.V2GetConnectorStateResponse
}
