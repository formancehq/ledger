package schemas

// Infos -
type Infos struct {
	Server  string      `json:"server"`
	Version interface{} `json:"version"`
	Config  struct {
		Storage struct {
			Driver  interface{} `json:"driver"`
			Ledgers interface{} `json:"ledgers"`
		} `json:"storage"`
	} `json:"config"`
}
