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
	case "postgres":
		dsn, ok := config["dsn"].(string)
		if !ok || dsn == "" {
			return fmt.Errorf("postgres driver requires 'dsn' configuration (connection string)")
		}
		return nil
	case "clickhouse":
		dsn, ok := config["dsn"].(string)
		if !ok || dsn == "" {
			return fmt.Errorf("clickhouse driver requires 'dsn' configuration (connection string)")
		}
		return nil
	case "file":
		// File driver doesn't require config - storage path is automatically generated
		// Config can be empty or omitted
		return nil
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite, postgres, clickhouse, file)", driver)
	}
}
