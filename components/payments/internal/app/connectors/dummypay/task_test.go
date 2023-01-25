package dummypay

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestTasks(t *testing.T) {
	t.Parallel()

	config := Config{Directory: "/tmp"}
	fs := newTestFS()

	// test generating files
	err := generateFile(config, fs)
	assert.NoError(t, err)

	files, err := afero.ReadDir(fs, config.Directory)
	assert.NoError(t, err)
	assert.Len(t, files, 1)

	// test reading files
	filesList, err := parseFilesToIngest(config, fs)
	assert.NoError(t, err)
	assert.Len(t, filesList, 1)

	// test ingesting files
	payload, err := parseIngestionPayload(config, TaskDescriptor{Key: taskKeyIngest, FileName: files[0].Name()}, fs)
	assert.NoError(t, err)
	assert.Len(t, payload, 1)

	// test removing files
	err = removeFiles(config, fs)
	assert.NoError(t, err)

	files, err = afero.ReadDir(fs, config.Directory)
	assert.NoError(t, err)
	assert.Len(t, files, 0)
}
