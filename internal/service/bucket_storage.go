package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// BucketStorage is an interface for bucket storage backends
type BucketStorage interface {
	// Close closes the storage and releases any resources
	Close() error
}

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

// NewBucketStorage creates a new bucket storage based on the driver and configuration
func NewBucketStorage(ctx context.Context, driver string, config map[string]interface{}, logger *zap.Logger) (BucketStorage, error) {
	// Validate configuration
	if err := ValidateBucketConfig(driver, config); err != nil {
		return nil, err
	}

	switch driver {
	case "sqlite":
		dsn := config["dsn"].(string)
		return NewSQLiteBucketStorage(ctx, dsn, logger)
	case "file":
		path := config["path"].(string)
		return NewFileBucketStorage(path, logger)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
}

// SQLiteBucketStorage stores bucket data in SQLite
type SQLiteBucketStorage struct {
	dsn    string
	logger *zap.Logger
}

// NewSQLiteBucketStorage creates a new SQLite bucket storage
func NewSQLiteBucketStorage(ctx context.Context, dsn string, logger *zap.Logger) (*SQLiteBucketStorage, error) {
	// Ensure the directory exists if dsn is a file path
	if dsn != ":memory:" {
		dir := filepath.Dir(dsn)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory for SQLite database: %w", err)
		}
	}

	storage := &SQLiteBucketStorage{
		dsn:    dsn,
		logger: logger.With(zap.String("driver", "sqlite"), zap.String("dsn", dsn)),
	}

	storage.logger.Info("SQLite bucket storage initialized")
	return storage, nil
}

// Close closes the SQLite bucket storage
func (s *SQLiteBucketStorage) Close() error {
	s.logger.Debug("Closing SQLite bucket storage")
	return nil
}

// FileBucketStorage stores bucket data in a file directory
type FileBucketStorage struct {
	path   string
	logger *zap.Logger
}

// NewFileBucketStorage creates a new file bucket storage
func NewFileBucketStorage(path string, logger *zap.Logger) (*FileBucketStorage, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("creating directory for file bucket storage: %w", err)
	}

	storage := &FileBucketStorage{
		path:   path,
		logger: logger.With(zap.String("driver", "file"), zap.String("path", path)),
	}

	storage.logger.Info("File bucket storage initialized")
	return storage, nil
}

// Close closes the file bucket storage
func (f *FileBucketStorage) Close() error {
	f.logger.Debug("Closing file bucket storage")
	return nil
}

// GetPath returns the storage path (for file storage)
func (f *FileBucketStorage) GetPath() string {
	return f.path
}
