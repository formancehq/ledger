package core

import "time"

type MigrationInfo struct {
	Version string    `json:"version"`
	Name    string    `json:"name"`
	State   string    `json:"state,omitempty"`
	Date    time.Time `json:"date,omitempty"`
}
