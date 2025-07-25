// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package operations

import (
	"github.com/formancehq/ledger/pkg/client/models/components"
)

type V2ListLedgersRequest struct {
	// The maximum number of results to return per page.
	//
	PageSize *int64 `queryParam:"style=form,explode=true,name=pageSize"`
	// Parameter used in pagination requests. Maximum page size is set to 15.
	// Set to the value of next for the next page of results.
	// Set to the value of previous for the previous page of results.
	// No other parameters can be set when this parameter is set.
	//
	Cursor *string `queryParam:"style=form,explode=true,name=cursor"`
	// Sort results using a field name and order (ascending or descending).
	// Format: `<field>:<order>`, where `<field>` is the field name and `<order>` is either `asc` or `desc`.
	//
	Sort        *string        `queryParam:"style=form,explode=true,name=sort"`
	RequestBody map[string]any `request:"mediaType=application/json"`
}

func (o *V2ListLedgersRequest) GetPageSize() *int64 {
	if o == nil {
		return nil
	}
	return o.PageSize
}

func (o *V2ListLedgersRequest) GetCursor() *string {
	if o == nil {
		return nil
	}
	return o.Cursor
}

func (o *V2ListLedgersRequest) GetSort() *string {
	if o == nil {
		return nil
	}
	return o.Sort
}

func (o *V2ListLedgersRequest) GetRequestBody() map[string]any {
	if o == nil {
		return map[string]any{}
	}
	return o.RequestBody
}

type V2ListLedgersResponse struct {
	HTTPMeta components.HTTPMetadata `json:"-"`
	// OK
	V2LedgerListResponse *components.V2LedgerListResponse
}

func (o *V2ListLedgersResponse) GetHTTPMeta() components.HTTPMetadata {
	if o == nil {
		return components.HTTPMetadata{}
	}
	return o.HTTPMeta
}

func (o *V2ListLedgersResponse) GetV2LedgerListResponse() *components.V2LedgerListResponse {
	if o == nil {
		return nil
	}
	return o.V2LedgerListResponse
}
