package dummypay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/task"
	"github.com/spf13/afero"
)

const taskKeyReadFiles = "read-files"

// newTaskReadFiles creates a new task descriptor for the taskReadFiles task.
func newTaskReadFiles() TaskDescriptor {
	return TaskDescriptor{
		Name: "Read Files from directory",
		Key:  taskKeyReadFiles,
	}
}

// taskReadFiles creates a task that reads files from a given directory.
// Only reads files with the generatedFilePrefix in their name.
func taskReadFiles(config Config, fs fs) task.Task {
	return func(ctx context.Context, logger logging.Logger,
		scheduler task.Scheduler,
	) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(config.FilePollingPeriod.Duration):
				files, err := parseFilesToIngest(config, fs)
				if err != nil {
					return fmt.Errorf("error parsing files to ingest: %w", err)
				}

				for _, file := range files {
					descriptor, err := models.EncodeTaskDescriptor(newTaskIngest(file))
					if err != nil {
						return err
					}

					// schedule a task to ingest the file into the payments system.
					err = scheduler.Schedule(descriptor, true)
					if err != nil {
						return fmt.Errorf("failed to schedule task to ingest file '%s': %w", file, err)
					}
				}
			}
		}
	}
}

func parseFilesToIngest(config Config, fs fs) ([]string, error) {
	dir, err := afero.ReadDir(fs, config.Directory)
	if err != nil {
		return nil, fmt.Errorf("error reading directory '%s': %w", config.Directory, err)
	}

	var files []string //nolint:prealloc // length is unknown

	// iterate over all files in the directory.
	for _, file := range dir {
		// skip files that do not match the generatedFilePrefix.
		if !strings.HasPrefix(file.Name(), generatedFilePrefix) {
			continue
		}

		files = append(files, file.Name())
	}

	return files, nil
}
