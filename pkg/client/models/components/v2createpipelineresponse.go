// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package components

// V2CreatePipelineResponse - Created ipeline
type V2CreatePipelineResponse struct {
	Data V2Pipeline `json:"data"`
}

func (o *V2CreatePipelineResponse) GetData() V2Pipeline {
	if o == nil {
		return V2Pipeline{}
	}
	return o.Data
}
