package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// FileLogStore is a LogStore implementation that reads/writes logs to a JSON file (JSONL format)
type FileLogStore struct {
	filePath string
	file     *os.File
	encoder  *json.Encoder
	mu       sync.RWMutex
	logger   logging.Logger
}

// NewFileLogStore creates a new FileLogStore
func NewFileLogStore(filePath string, logger logging.Logger) (*FileLogStore, error) {
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

	f.logger.WithFields(map[string]any{"count": len(logs), "file": f.filePath}).Debugf("Logs written to file")
	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key (implements LogReader)
func (f *FileLogStore) GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error) {
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
			f.logger.WithFields(map[string]any{"error": err}).Infof("WARN: Failed to unmarshal log line")
			continue
		}

		// Filter by ledger and idempotency key
		if log.Ledger == ledgerName && log.IdempotencyKey == idempotencyKey {
			return &log, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	return nil, nil
}

// GetLastLog retrieves the last log from the file for a specific ledger (implements LogReader)
func (f *FileLogStore) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Seek to end of file
	if _, err := f.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seeking to end of file: %w", err)
	}

	// Read backwards to find the last line
	// For simplicity, we'll read the entire file and get the last log for the ledger
	// In production, you might want to optimize this
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	var lastLog *ledger.Log
	scanner := bufio.NewScanner(f.file)
	for scanner.Scan() {
		var log ledger.Log
		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			f.logger.WithFields(map[string]any{"error": err}).Infof("WARN: Failed to unmarshal log line")
			continue
		}
		// Filter by ledger
		if log.Ledger == ledgerName {
			lastLog = &log
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	return lastLog, nil
}

// logEntry represents a log entry with its ID and file offset
type logEntry struct {
	id     uint64
	offset int64
}

// fileLogCursorSorted implements Cursor[ledger.Log] for reading logs in sorted order from file
type fileLogCursorSorted struct {
	file       *os.File
	store      *FileLogStore
	ledgerName string
	entries    []logEntry
	index      int
}

func (c *fileLogCursorSorted) Next(ctx context.Context) (ledger.Log, error) {
	if c.index >= len(c.entries) {
		return ledger.Log{}, io.EOF
	}

	// Seek to the offset of the next log to read
	entry := c.entries[c.index]
	if _, err := c.file.Seek(entry.offset, io.SeekStart); err != nil {
		return ledger.Log{}, fmt.Errorf("seeking to log offset: %w", err)
	}

	// Read the line at this offset
	scanner := bufio.NewScanner(c.file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return ledger.Log{}, fmt.Errorf("reading log line: %w", err)
		}
		return ledger.Log{}, io.EOF
	}

	var log ledger.Log
	if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
		return ledger.Log{}, fmt.Errorf("unmarshaling log: %w", err)
	}

	c.index++
	return log, nil
}

func (c *fileLogCursorSorted) Close() error {
	// No resources to clean up
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger (implements LogReader)
// Logs are returned in descending order by ID
// For FileLogStore, we use a two-pass approach: first collect IDs and offsets, then read in order
func (f *FileLogStore) GetAllLogs(ctx context.Context, ledgerName string) (*Cursor[ledger.Log], error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// First pass: collect log IDs and their file offsets (minimal memory usage)
	var entries []logEntry

	// Seek to beginning of file
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	reader := bufio.NewReader(f.file)
	var currentOffset int64 = 0
	for {
		// Get offset before reading the line
		offset := currentOffset

		// Read line
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				// Try to parse last line if it doesn't end with newline
				if len(line) > 0 {
					var log struct {
						ID     *uint64 `json:"id"`
						Ledger string  `json:"ledger"`
					}
					if err := json.Unmarshal(line, &log); err == nil {
						if log.Ledger == ledgerName && log.ID != nil {
							entries = append(entries, logEntry{
								id:     *log.ID,
								offset: offset,
							})
						}
					}
				}
				break
			}
			return nil, fmt.Errorf("reading file: %w", err)
		}

		// Remove newline for parsing
		lineBytes := line[:len(line)-1]

		// Quick parse to extract just the ID and ledger (minimal JSON parsing)
		var log struct {
			ID     *uint64 `json:"id"`
			Ledger string  `json:"ledger"`
		}
		if err := json.Unmarshal(lineBytes, &log); err == nil {
			// Filter by ledger and collect ID with offset
			if log.Ledger == ledgerName && log.ID != nil {
				entries = append(entries, logEntry{
					id:     *log.ID,
					offset: offset,
				})
			}
		}

		currentOffset += int64(len(line))
	}

	// Sort entries by ID descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].id > entries[j].id
	})

	// Create cursor that will read logs in sorted order
	cursor := &fileLogCursorSorted{
		file:       f.file,
		store:      f,
		ledgerName: ledgerName,
		entries:    entries,
		index:      0,
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
