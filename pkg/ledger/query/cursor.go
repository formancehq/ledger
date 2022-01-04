package query

type Cursor struct {
	PageSize  int         `json:"page_size"`
	HasMore   bool        `json:"has_more"`
	Total     int64       `json:"total,omitempty"`
	Remaining int         `json:"remaining_results"`
	Previous  string      `json:"previous,omitempty"`
	Next      string      `json:"next,omitempty"`
	Data      interface{} `json:"data"`
}
