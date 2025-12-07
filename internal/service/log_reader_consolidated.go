package service

import (
	"context"
	"fmt"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// HotDiffLogsProvider provides access to in-memory logs from the FSM
type HotDiffLogsProvider interface {
	// GetInMemoryLogs returns the in-memory logs for a ledger
	// This represents the logs since the last snapshot
	GetInMemoryLogs(ledgerName string) []ledger.Log
}

// ConsolidatedLogReader combines logs from an underlying LogReader with in-memory logs from the FSM
type ConsolidatedLogReader struct {
	underlying  LogReader
	fsmProvider HotDiffLogsProvider
}

// NewConsolidatedLogReader creates a new ConsolidatedLogReader
func NewConsolidatedLogReader(underlying LogReader, fsmProvider HotDiffLogsProvider) *ConsolidatedLogReader {
	return &ConsolidatedLogReader{
		underlying:  underlying,
		fsmProvider: fsmProvider,
	}
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key
// First checks in-memory logs, then falls back to underlying store
func (r *ConsolidatedLogReader) GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error) {
	if idempotencyKey == "" {
		return nil, nil
	}

	// First check in-memory logs from FSM
	inMemoryLogs := r.fsmProvider.GetInMemoryLogs(ledgerName)
	for _, log := range inMemoryLogs {
		if log.IdempotencyKey == idempotencyKey {
			return &log, nil
		}
	}

	// Fall back to underlying store
	return r.underlying.GetLogWithIdempotencyKey(ctx, ledgerName, idempotencyKey)
}

// GetLastLog retrieves the last log for a ledger
// Returns the most recent log from either in-memory or underlying store
func (r *ConsolidatedLogReader) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {

	// Get in-memory logs
	inMemoryLogs := r.fsmProvider.GetInMemoryLogs(ledgerName)
	if len(inMemoryLogs) > 0 {
		return &inMemoryLogs[0], nil
	}

	// Get last log from underlying store
	underlyingLastLog, err := r.underlying.GetLastLog(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting last log from underlying store: %w", err)
	}

	return underlyingLastLog, nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger
// Logs are returned in descending order by ID, combining in-memory and underlying logs
func (r *ConsolidatedLogReader) GetAllLogs(ctx context.Context, ledgerName string) (*Cursor[ledger.Log], error) {
	// Get cursor from underlying store
	underlyingCursor, err := r.underlying.GetAllLogs(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting logs from underlying store: %w", err)
	}

	// Get in-memory logs (already sorted by ID descending)
	inMemoryLogs := r.fsmProvider.GetInMemoryLogs(ledgerName)

	// Create consolidated cursor
	cursor := &consolidatedLogCursor{
		underlyingCursor: *underlyingCursor,
		inMemoryLogs:     inMemoryLogs,
	}

	var cursorInterface Cursor[ledger.Log] = cursor
	return &cursorInterface, nil
}

// consolidatedLogCursor implements Cursor[ledger.Log] for consolidated logs
type consolidatedLogCursor struct {
	underlyingCursor Cursor[ledger.Log]
	inMemoryLogs     []ledger.Log
	inMemoryIndex    int
}

func (c *consolidatedLogCursor) Next(ctx context.Context) (ledger.Log, error) {
	// Load next log from in-memory logs if needed
	if c.inMemoryIndex < len(c.inMemoryLogs) {
		index := c.inMemoryIndex
		c.inMemoryIndex++
		return c.inMemoryLogs[index], nil
	}

	return c.underlyingCursor.Next(ctx)
}

func (c *consolidatedLogCursor) Close() error {
	if c.underlyingCursor != nil {
		return (c.underlyingCursor).Close()
	}
	return nil
}
