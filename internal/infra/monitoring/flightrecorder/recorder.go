package flightrecorder

import (
	"errors"
	"fmt"
	"io"
	"runtime/trace"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
)

// Recorder wraps a runtime/trace.FlightRecorder and exposes lifecycle methods
// compatible with the worker.Lifecycle interface.
type Recorder struct {
	fr     *trace.FlightRecorder
	logger logging.Logger

	mu      sync.Mutex
	started bool
}

// New creates a new flight recorder from the given configuration.
func New(cfg Config, logger logging.Logger) *Recorder {
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{
		MinAge:   cfg.MinAge,
		MaxBytes: uint64(cfg.MaxBytes),
	})

	return &Recorder{
		fr:     fr,
		logger: logger,
	}
}

// Start begins recording flight data.
func (r *Recorder) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.fr.Start(); err != nil {
		r.logger.Errorf("Failed to start flight recorder: %v", err)

		return
	}

	r.started = true

	r.logger.Infof("Flight recorder started")
}

// Stop stops recording flight data.
func (r *Recorder) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return
	}

	r.fr.Stop()
	r.started = false

	r.logger.Infof("Flight recorder stopped")
}

// Snapshot writes the current flight recorder buffer to the given writer.
// Returns an error if the recorder is not running.
func (r *Recorder) Snapshot(w io.Writer) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return errors.New("flight recorder is not running")
	}

	_, err := r.fr.WriteTo(w)
	if err != nil {
		return fmt.Errorf("writing flight recorder snapshot: %w", err)
	}

	return nil
}

// Enabled returns true if the recorder is currently active.
func (r *Recorder) Enabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.started
}
