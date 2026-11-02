package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandFailureDiagnostics(t *testing.T) {
	t.Parallel()

	binary := filepath.Join(t.TempDir(), "lintproto")
	build := exec.Command("go", "build", "-o", binary, ".")
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	for _, tc := range []struct {
		name       string
		buildExit  string
		want       string
		unexpected string
	}{
		{
			name: "plugin build fails", buildExit: "23",
			want:       "building tools/buf-plugin-ledger: exit status 23\nsynthetic plugin compiler diagnostic",
			unexpected: "synthetic buf lint diagnostic",
		},
		{
			name: "buf lint fails", buildExit: "0",
			want:       "synthetic buf lint diagnostic",
			unexpected: "building tools/buf-plugin-ledger",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			require.NoError(t, os.MkdirAll(bin, 0o755))
			require.NoError(t, os.MkdirAll(filepath.Join(root, "tools/buf-plugin-ledger"), 0o755))
			for name, script := range map[string]string{
				"git": "printf '%s\\n' \"$LINTPROTO_TEST_ROOT\"\n",
				"go":  "if [ \"$LINTPROTO_TEST_BUILD_EXIT\" != 0 ]; then echo 'synthetic plugin compiler diagnostic' >&2; fi\nexit \"$LINTPROTO_TEST_BUILD_EXIT\"\n",
				"buf": "echo 'synthetic buf lint diagnostic' >&2\nexit 100\n",
			} {
				require.NoError(t, os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+script), 0o755))
			}

			cmd := exec.Command(binary)
			cmd.Env = append(os.Environ(),
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"LINTPROTO_TEST_ROOT="+root,
				"LINTPROTO_TEST_BUILD_EXIT="+tc.buildExit,
			)
			output, err := cmd.CombinedOutput()
			exitError, ok := errors.AsType[*exec.ExitError](err)
			require.True(t, ok, "expected process failure, got %v: %s", err, output)
			require.Equal(t, 1, exitError.ExitCode())
			require.Contains(t, string(output), tc.want)
			require.Contains(t, string(output), "lint-proto: FAIL")
			require.NotContains(t, string(output), tc.unexpected)
		})
	}
}
