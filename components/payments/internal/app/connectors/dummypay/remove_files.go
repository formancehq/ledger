package dummypay

import (
	"fmt"
	"strings"

	"github.com/spf13/afero"
)

// removeFiles removes all files from the given directory.
// Only removes files that has generatedFilePrefix in the name.
func removeFiles(config Config, fs fs) error {
	dir, err := afero.ReadDir(fs, config.Directory)
	if err != nil {
		return fmt.Errorf("failed to open directory '%s': %w", config.Directory, err)
	}

	// iterate over all files in the directory
	for _, file := range dir {
		// skip files that do not match the generatedFilePrefix
		if !strings.HasPrefix(file.Name(), generatedFilePrefix) {
			continue
		}

		// remove the file
		err = fs.Remove(fmt.Sprintf("%s/%s", config.Directory, file.Name()))
		if err != nil {
			return fmt.Errorf("failed to remove file '%s': %w", file.Name(), err)
		}
	}

	return nil
}
