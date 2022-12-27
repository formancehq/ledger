package core

import "time"

type MigrationInfo struct {
	Version string    `json:"version"`
	State   string    `json:"state"`
	Date    time.Time `json:"date,omitempty"`
}
