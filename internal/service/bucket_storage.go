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
		// SQLite must be file-based (not :memory: or other in-memory databases)
		if dsn == ":memory:" || dsn == "file::memory:" {
			return fmt.Errorf("sqlite driver requires a file-based database, in-memory databases are not supported")
		}
		// DSN must start with "file:" prefix
		if len(dsn) < 5 || dsn[:5] != "file:" {
			return fmt.Errorf("sqlite driver requires DSN to start with 'file:' prefix (e.g., 'file:./data/bucket.db')")
		}
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
		path, ok := config["path"].(string)
		if !ok || path == "" {
			return fmt.Errorf("file driver requires 'path' configuration (directory path)")
		}
		return nil
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite, postgres, clickhouse, file)", driver)
	}
}
