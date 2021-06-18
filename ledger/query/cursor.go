package query

type Cursor struct {
	PageSize int         `json:"page_size"`
	HasMore  bool        `json:"has_more"`
	Remaning int         `json:"remaning_results,omitempty"`
	Previous string      `json:"previous,omitempty"`
	Next     string      `json:"next,omitempty"`
	Data     interface{} `json:"data"`
}
