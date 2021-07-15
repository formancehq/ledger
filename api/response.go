package api

type Response struct {
	Ok    bool  `json:"ok"`
	Error error `json:"error,omitempty"`
}
