package core

type MigrationInfo struct {
	Version string `json:"version"`
	Name    string `json:"name"`
	State   string `json:"state,omitempty"`
	Date    Time   `json:"date,omitempty"`
}
