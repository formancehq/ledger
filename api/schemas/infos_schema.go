package schemas

// Infos -
type Infos struct {
	Server  string      `json:"server,omitempty"`
	Version interface{} `json:"version,omitempty"`
	Config  struct {
		Storage struct {
			Driver  interface{} `json:"driver,omitempty"`
			Ledgers interface{} `json:"ledgers,omitempty"`
		} `json:"storage,omitempty"`
	} `json:"config,omitempty"`
}
