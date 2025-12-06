package service

import (
	"fmt"
	_ "modernc.org/sqlite"
)

// ValidateBucketConfig validates the configuration for a bucket driver
func ValidateBucketConfig(driver string, config map[string]interface{}) error {
	switch driver {
	case "sqlite":
		dsn, ok := config["dsn"].(string)
		if !ok || dsn == "" {
			return fmt.Errorf("sqlite driver requires 'dsn' configuration (connection address)")
		}
		return nil
	case "file":
		path, ok := config["path"].(string)
		if !ok || path == "" {
			return fmt.Errorf("file driver requires 'path' configuration (directory path)")
		}
		return nil
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite, file)", driver)
	}
}