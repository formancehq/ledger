package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.uber.org/zap"
)

// FileLogStore is a LogStore implementation that reads/writes logs to a JSON file (JSONL format)
type FileLogStore struct {
	filePath string
	file     *os.File
	encoder  *json.Encoder
	mu       sync.RWMutex
	logger   *zap.Logger
}

// NewFileLogStore creates a new FileLogStore
func NewFileLogStore(filePath string, logger *zap.Logger) (*FileLogStore, error) {
	// Open file in append mode, create if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening log file: %w", err)
	}

	store := &FileLogStore{
		filePath: filePath,
		file:     file,
		encoder:  json.NewEncoder(file),
		logger:   logger,
	}

	return store, nil
}

// InsertLogs writes logs to the JSON file (implements LogWriter)
func (f *FileLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Write each log as a JSON object on a new line (JSONL format)
	for _, log := range logs {
		if err := f.encoder.Encode(log); err != nil {
			return fmt.Errorf("encoding log to JSON: %w", err)
		}
	}

	// Flush to ensure data is written to disk
	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("syncing file: %w", err)
	}

	f.logger.Debug("Logs written to file", zap.Int("count", len(logs)), zap.String("file", f.filePath))
	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key (implements LogReader)
func (f *FileLogStore) GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledger.Log, error) {
	if idempotencyKey == "" {
		return nil, nil
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Seek to beginning of file
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	scanner := bufio.NewScanner(f.file)
	for scanner.Scan() {
		var log ledger.Log
		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			f.logger.Warn("Failed to unmarshal log line", zap.Error(err))
			continue
		}

		if log.IdempotencyKey == idempotencyKey {
			return &log, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	return nil, nil
}

// GetLastLog retrieves the last log from the file (implements LogReader)
func (f *FileLogStore) GetLastLog(ctx context.Context) (*ledger.Log, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Seek to end of file
	if _, err := f.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seeking to end of file: %w", err)
	}

	// Read backwards to find the last line
	// For simplicity, we'll read the entire file and get the last log
	// In production, you might want to optimize this
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	var lastLog *ledger.Log
	scanner := bufio.NewScanner(f.file)
	for scanner.Scan() {
		var log ledger.Log
		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			f.logger.Warn("Failed to unmarshal log line", zap.Error(err))
			continue
		}
		lastLog = &log
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	return lastLog, nil
}

// fileLogCursor implements Cursor[ledger.Log] for FileLogStore
type fileLogCursor struct {
	file    *os.File
	scanner *bufio.Scanner
	store   *FileLogStore
}

func (c *fileLogCursor) Next(ctx context.Context) (ledger.Log, error) {
	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return ledger.Log{}, fmt.Errorf("reading file: %w", err)
		}
		return ledger.Log{}, io.EOF
	}

	var log ledger.Log
	if err := json.Unmarshal(c.scanner.Bytes(), &log); err != nil {
		return ledger.Log{}, fmt.Errorf("unmarshaling log: %w", err)
	}

	return log, nil
}

func (c *fileLogCursor) Close() error {
	// File is managed by FileLogStore, don't close it here
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs in the file (implements LogReader)
func (f *FileLogStore) GetAllLogs(ctx context.Context) (*Cursor[ledger.Log], error) {
	f.mu.RLock()
	// Note: We don't unlock here because the cursor will read from the file
	// The lock will be released when the cursor is closed or when reading is done

	// Seek to beginning of file
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		f.mu.RUnlock()
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	scanner := bufio.NewScanner(f.file)
	cursor := &fileLogCursor{
		file:    f.file,
		scanner: scanner,
		store:   f,
	}

	var cursorInterface Cursor[ledger.Log] = cursor
	return &cursorInterface, nil
}

// Close closes the file
func (f *FileLogStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		if err := f.file.Close(); err != nil {
			return fmt.Errorf("closing file: %w", err)
		}
		f.file = nil
	}
	return nil
}
