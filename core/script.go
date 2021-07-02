package core

type Script struct {
	Plain string                 `json:"plain"`
	Vars  map[string]interface{} `json:"vars"`
}
