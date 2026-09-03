package testenv

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// SynchronizedCommand is a subprocess whose fixture command writes one line to
// file descriptor 3, then blocks reading file descriptor 4. RunSynchronized
// releases all commands only after every command has reported ready.
type SynchronizedCommand struct {
	Name    string
	Command *exec.Cmd
}

// SynchronizedResult contains the combined stdout/stderr and exit error for
// every named subprocess.
type SynchronizedResult struct {
	Output   map[string]string
	Exit     map[string]error
	Duration time.Duration
}

type synchronizedProcess struct {
	name          string
	command       *exec.Cmd
	output        bytes.Buffer
	readyReader   *os.File
	readyWriter   *os.File
	releaseReader *os.File
	releaseWriter *os.File
	started       bool
	exited        bool
}

type processEvent struct {
	index int
	err   error
}

// RunSynchronized supervises a bounded fixture barrier. A process that exits
// before ready aborts its siblings, and every error reports both subprocesses'
// output. Each command runs in its own process group so aborting it also stops
// reviewer or validator children.
func RunSynchronized(t testing.TB, timeout time.Duration, commands ...SynchronizedCommand) (SynchronizedResult, error) {
	t.Helper()
	startedAt := time.Now()
	result := SynchronizedResult{
		Output: make(map[string]string, len(commands)),
		Exit:   make(map[string]error, len(commands)),
	}
	if len(commands) == 0 {
		result.Duration = time.Since(startedAt)

		return result, nil
	}

	processes := make([]synchronizedProcess, len(commands))
	for index, item := range commands {
		if item.Name == "" || item.Command == nil {
			return result, errors.New("synchronized command requires a name and command")
		}
		if len(item.Command.ExtraFiles) != 0 {
			return result, fmt.Errorf("%s already has extra files", item.Name)
		}
		readyReader, readyWriter, err := os.Pipe()
		if err != nil {
			return result, fmt.Errorf("create %s ready pipe: %w", item.Name, err)
		}
		releaseReader, releaseWriter, err := os.Pipe()
		if err != nil {
			_ = readyReader.Close()
			_ = readyWriter.Close()

			return result, fmt.Errorf("create %s release pipe: %w", item.Name, err)
		}
		processes[index] = synchronizedProcess{
			name:          item.Name,
			command:       item.Command,
			readyReader:   readyReader,
			readyWriter:   readyWriter,
			releaseReader: releaseReader,
			releaseWriter: releaseWriter,
		}
		processes[index].command.ExtraFiles = []*os.File{readyWriter, releaseReader}
		processes[index].command.Stdout = &processes[index].output
		processes[index].command.Stderr = &processes[index].output
		processes[index].command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}

	readyEvents := make(chan processEvent, len(processes))
	exitEvents := make(chan processEvent, len(processes))
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	failure := error(nil)
	for index := range processes {
		process := &processes[index]
		if err := process.command.Start(); err != nil {
			failure = fmt.Errorf("start %s: %w", process.name, err)

			break
		}
		process.started = true
		_ = process.readyWriter.Close()
		_ = process.releaseReader.Close()
		go func(index int, reader *os.File) {
			_, err := bufio.NewReader(reader).ReadString('\n')
			_ = reader.Close()
			readyEvents <- processEvent{index: index, err: err}
		}(index, process.readyReader)
		go func(index int, command *exec.Cmd) {
			exitEvents <- processEvent{index: index, err: command.Wait()}
		}(index, process.command)
	}

	ready := make([]bool, len(processes))
	readyCount := 0
	for failure == nil && readyCount < len(processes) {
		select {
		case event := <-readyEvents:
			process := &processes[event.index]
			switch {
			case event.err != nil:
				failure = fmt.Errorf("%s failed before all peers were ready: ready signal: %w", process.name, event.err)
			case ready[event.index]:
				failure = fmt.Errorf("unexpected duplicate ready signal from %s", process.name)
			default:
				ready[event.index] = true
				readyCount++
			}
		case event := <-exitEvents:
			process := &processes[event.index]
			process.exited = true
			result.Exit[process.name] = event.err
			if event.err == nil {
				failure = fmt.Errorf("%s failed before all peers were ready: process exited successfully", process.name)
			} else {
				failure = fmt.Errorf("%s failed before all peers were ready: process exit: %w", process.name, event.err)
			}
		case <-deadline.C:
			failure = fmt.Errorf("fixture synchronization exceeded %s", timeout)
		}
	}

	if failure == nil {
		for index := range processes {
			if _, err := processes[index].releaseWriter.WriteString("release\n"); err != nil {
				failure = fmt.Errorf("release %s: %w", processes[index].name, err)

				break
			}
			_ = processes[index].releaseWriter.Close()
		}
	}

	if failure != nil {
		for index := range processes {
			_ = processes[index].releaseWriter.Close() // Wake a blocked fixture before the kill fallback.
			if processes[index].started && !processes[index].exited {
				terminateProcessGroup(processes[index].command)
			}
		}
	}

	remaining := 0
	for index := range processes {
		if processes[index].started && !processes[index].exited {
			remaining++
		}
	}
	for remaining > 0 {
		select {
		case event := <-exitEvents:
			if !processes[event.index].exited {
				processes[event.index].exited = true
				result.Exit[processes[event.index].name] = event.err
				remaining--
			}
		case <-deadline.C:
			if failure == nil {
				failure = fmt.Errorf("fixture processes exceeded %s", timeout)
			}
			for index := range processes {
				if processes[index].started && !processes[index].exited {
					terminateProcessGroup(processes[index].command)
				}
			}
		}
	}

	for index := range processes {
		process := &processes[index]
		_ = process.readyReader.Close()
		_ = process.readyWriter.Close()
		_ = process.releaseReader.Close()
		_ = process.releaseWriter.Close()
		result.Output[process.name] = process.output.String()
		if failure == nil && result.Exit[process.name] != nil {
			failure = fmt.Errorf("%s failed: %w", process.name, result.Exit[process.name])
		}
	}
	result.Duration = time.Since(startedAt)
	if failure != nil {
		return result, fmt.Errorf("%w\n%s", failure, synchronizedDiagnostics(processes))
	}

	return result, nil
}

func terminateProcessGroup(command *exec.Cmd) {
	if command.Process == nil {
		return
	}
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		_ = command.Process.Kill()
	}
}

func synchronizedDiagnostics(processes []synchronizedProcess) string {
	var diagnostics strings.Builder
	for index := range processes {
		if index > 0 {
			diagnostics.WriteByte('\n')
		}
		fmt.Fprintf(&diagnostics, "%s stdout/stderr:\n%s", processes[index].name, processes[index].output.String())
	}

	return diagnostics.String()
}
