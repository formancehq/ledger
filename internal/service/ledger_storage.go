package service

import (
	"fmt"

	_ "modernc.org/sqlite"
)

// ValidateBucketConfig validates the configuration for a bucket driver
func ValidateBucketConfig(driver string, config map[string]interface{}) error {
	switch driver {
	case "sqlite":
		// SQLite driver doesn't require config - DSN is automatically generated
		// Config can be empty or omitted
		return nil
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite)", driver)
	}
}
