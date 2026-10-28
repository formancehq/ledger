// Package bufplugin builds the repository's buf check plugin. misc/proto/buf.yaml
// declares it, so every buf command that loads that configuration (lint and
// breaking alike) must find the binary on PATH.
package bufplugin

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	sourceDir  = "tools/buf-plugin-ledger"
	binaryName = "buf-plugin-ledger"
)

// Environment builds the plugin from repoRoot into binDir and returns the
// current environment with binDir prepended to PATH.
func Environment(repoRoot, binDir string) ([]string, error) {
	cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, binaryName), ".")
	cmd.Dir = filepath.Join(repoRoot, sourceDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("building %s: %w\n%s", sourceDir, err, output)
	}

	env := os.Environ()
	for i, entry := range env {
		if value, ok := strings.CutPrefix(entry, "PATH="); ok {
			env[i] = "PATH=" + binDir + string(os.PathListSeparator) + value

			return env, nil
		}
	}

	return append(env, "PATH="+binDir), nil
}
