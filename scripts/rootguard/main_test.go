package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(testingMain *testing.M) {
	if err := os.Setenv("GIT_CONFIG_GLOBAL", os.DevNull); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_SYSTEM", os.DevNull); err != nil {
		panic(err)
	}
	os.Exit(testingMain.Run())
}

func TestProviderMutationWinsOverNonZeroExit(t *testing.T) {
	t.Parallel()

	root := newRunnerTestRepository(t)
	var output bytes.Buffer
	status := run([]string{
		"--root", root, "--", "/bin/sh", "-c",
		"printf changed > " + shellQuote(filepath.Join(root, "ignored", "value")) + "; exit 7",
	}, strings.NewReader(""), &output, &output)
	require.Equal(t, exitError, status, output.String())
	require.Contains(t, output.String(), "ROOT_MUTATION_DETECTED")
	require.NotContains(t, output.String(), "ROOT_UNCHANGED=PASS")
}

func TestResidentRunnerDetectsMutationAfterItsOnDiskBinaryIsReplaced(t *testing.T) {
	t.Parallel()

	root := newRunnerTestRepository(t)
	binary := buildRunner(t)
	replacement := binary + ".replacement"
	script := fmt.Sprintf(
		"printf '#!/bin/sh\\nexit 0\\n' > %s; chmod 700 %s; mv %s %s; printf changed > ignored/value",
		shellQuote(replacement),
		shellQuote(replacement),
		shellQuote(replacement),
		shellQuote(binary),
	)
	command := exec.Command(binary, "--root", root, "--", "/bin/sh", "-c", script)
	command.Dir = root
	output, err := command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Equal(t, exitError, command.ProcessState.ExitCode(), string(output))
	require.Contains(t, string(output), "ROOT_MUTATION_DETECTED")
}

func TestConcurrentResidentRunnersProtectSameUnchangedRoot(t *testing.T) {
	t.Parallel()

	root := newRunnerTestRepository(t)
	binary := buildRunner(t)
	coordination := t.TempDir()
	firstReady := filepath.Join(coordination, "first-ready")
	secondReady := filepath.Join(coordination, "second-ready")
	first := guardedCommand(binary, root, fmt.Sprintf(
		": > %s; while [ ! -e %s ]; do :; done",
		shellQuote(firstReady), shellQuote(secondReady),
	))
	second := guardedCommand(binary, root, fmt.Sprintf(
		": > %s; while [ ! -e %s ]; do :; done",
		shellQuote(secondReady), shellQuote(firstReady),
	))
	firstOutput, secondOutput := runConcurrently(t, first, second)
	require.Contains(t, firstOutput, "ROOT_UNCHANGED=PASS")
	require.Contains(t, secondOutput, "ROOT_UNCHANGED=PASS")
}

func TestConcurrentResidentRunnersBothDetectOverlappingMutation(t *testing.T) {
	t.Parallel()

	root := newRunnerTestRepository(t)
	binary := buildRunner(t)
	coordination := t.TempDir()
	firstReady := filepath.Join(coordination, "first-ready")
	secondReady := filepath.Join(coordination, "second-ready")
	mutated := filepath.Join(coordination, "mutated")
	first := guardedCommand(binary, root, fmt.Sprintf(
		": > %s; while [ ! -e %s ]; do :; done; printf changed > ignored/value; : > %s",
		shellQuote(firstReady), shellQuote(secondReady), shellQuote(mutated),
	))
	second := guardedCommand(binary, root, fmt.Sprintf(
		": > %s; while [ ! -e %s ]; do :; done; while [ ! -e %s ]; do :; done",
		shellQuote(secondReady), shellQuote(firstReady), shellQuote(mutated),
	))
	firstOutput, secondOutput := runConcurrentlyExpectingFailure(t, first, second)
	require.Contains(t, firstOutput, "ROOT_MUTATION_DETECTED")
	require.Contains(t, secondOutput, "ROOT_MUTATION_DETECTED")
}

func guardedCommand(binary, root, script string) *exec.Cmd {
	command := exec.Command(binary, "--root", root, "--", "/bin/sh", "-c", script)
	command.Dir = root

	return command
}

func runConcurrently(t *testing.T, commands ...*exec.Cmd) (string, string) {
	t.Helper()
	outputs := make([]bytes.Buffer, len(commands))
	for index, command := range commands {
		command.Stdout = &outputs[index]
		command.Stderr = &outputs[index]
		require.NoError(t, command.Start())
	}
	for index, command := range commands {
		require.NoError(t, command.Wait(), outputs[index].String())
	}

	return outputs[0].String(), outputs[1].String()
}

func runConcurrentlyExpectingFailure(t *testing.T, commands ...*exec.Cmd) (string, string) {
	t.Helper()
	outputs := make([]bytes.Buffer, len(commands))
	for index, command := range commands {
		command.Stdout = &outputs[index]
		command.Stderr = &outputs[index]
		require.NoError(t, command.Start())
	}
	for index, command := range commands {
		require.Error(t, command.Wait(), outputs[index].String())
		require.Equal(t, exitError, command.ProcessState.ExitCode(), outputs[index].String())
	}

	return outputs[0].String(), outputs[1].String()
}

func buildRunner(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "rootguard")
	command := exec.Command("go", "build", "-o", binary, ".")
	command.Dir = packageRoot(t)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	return binary
}

func newRunnerTestRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "root")
	require.NoError(t, os.Mkdir(root, 0o755))
	runRunnerGit(t, root, "init")
	runRunnerGit(t, root, "config", "user.name", "Root Guard Runner Test")
	runRunnerGit(t, root, "config", "user.email", "rootguard-runner@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(root, ".gitignore"), []byte("ignored/\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "ignored"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "ignored", "value"), []byte("before"), 0o644))
	runRunnerGit(t, root, "add", ".gitignore", "base.txt")
	runRunnerGit(t, root, "commit", "-m", "base")

	return root
}

func runRunnerGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, "git %s:\n%s", strings.Join(arguments, " "), output)
}

func packageRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	require.NoError(t, err)

	return directory
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
