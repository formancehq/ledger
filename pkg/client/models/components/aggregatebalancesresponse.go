// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package components

type AggregateBalancesResponse struct {
	Data map[string]int64 `json:"data"`
}

func (o *AggregateBalancesResponse) GetData() map[string]int64 {
	if o == nil {
		return map[string]int64{}
	}
	return o.Data
}
