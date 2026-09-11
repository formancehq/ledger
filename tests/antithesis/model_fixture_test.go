package antithesis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Reuse the already running test executable instead of executing newly written
// shell scripts: first execution of those scripts can take seconds on macOS.
func TestMain(m *testing.M) {
	if os.Getenv("MODEL_TEST_HELPER") == "1" {
		if err := runModelFixtureHelper(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runModelFixtureHelper() error {
	switch filepath.Base(os.Args[0]) {
	case "cancellation-parent":
		return runModelFixtureCancellationParent()
	case "go":
		for i, arg := range os.Args[1:] {
			if arg == "-o" && i+2 < len(os.Args) {
				output := os.Args[i+2]
				switch filepath.Base(output) {
				case "ledger-server", "model-driver":
					return os.Symlink(os.Getenv("MODEL_TEST_EXECUTABLE"), output)
				}
			}
		}

		return fmt.Errorf("unexpected fake go invocation: %v", os.Args)
	case "seq":
		if len(os.Args) == 3 && os.Args[1] == "1" && os.Args[2] == "0" {
			return nil
		}

		return syscall.Exec(os.Getenv("MODEL_TEST_SEQ"), os.Args, os.Environ())
	case "date":
		// A token remains in the pipe for every subsequent clock read. No polling
		// or guessed startup delay is needed; cancellation kills blocked readers.
		reader := os.NewFile(3, "scenario-ready-reader")
		writer := os.NewFile(4, "scenario-ready-writer")
		var ready [1]byte
		if _, err := io.ReadFull(reader, ready[:]); err != nil {
			return err
		}
		if _, err := writer.Write(ready[:]); err != nil {
			return err
		}
		// Keep the scenario deadline open until the early-exit branch observes
		// termination. A stalled helper still fails the outer test deadline.
		if os.Getenv("FAKE_MODEL_SCENARIO") == "early-exit" {
			fmt.Println(0)

			return nil
		}

		return syscall.Exec(os.Getenv("MODEL_TEST_DATE"), os.Args, os.Environ())
	case "ledger-server":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		fmt.Println("Became leader")
		<-ctx.Done()

		return nil
	case "model-driver":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		scenario := os.Getenv("FAKE_MODEL_SCENARIO")
		var assertions string
		switch scenario {
		case "blocked", "registration-only":
			assertions = `{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":false}}`
		case "early-exit":
		case "setup-only":
			assertions = `{"antithesis_assert":{"display_type":"Sometimes","message":"should be able to create ledger","condition":true,"hit":true}}
{"antithesis_assert":{"display_type":"Sometimes","message":"should always be able to get created ledger","condition":true,"hit":true}}`
		case "unverified-output":
			assertions = `{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: unrelated path exercised","condition":true,"hit":true}}
{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":false}}`
		case "verified":
			assertions = `{"antithesis_assert":{"display_type":"Reachable","message":"singleton_driver_model: model outcome verified","condition":true,"hit":true}}`
		default:
			return fmt.Errorf("unknown scenario: %s", scenario)
		}
		if assertions != "" {
			if err := os.WriteFile(os.Getenv("ANTITHESIS_SDK_LOCAL_OUTPUT"), []byte(assertions+"\n"), 0o600); err != nil {
				return err
			}
		}
		if scenario == "blocked" {
			fmt.Println("first setup Apply entered")
		}
		if _, err := os.NewFile(4, "scenario-ready-writer").Write([]byte{1}); err != nil {
			return err
		}
		if scenario == "early-exit" {
			return nil
		}
		<-ctx.Done()

		return nil
	default:
		return fmt.Errorf("unexpected helper invocation: %s", os.Args[0])
	}
}

func configureModelFixtureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}

		return err
	}
	cmd.WaitDelay = time.Second
}

// Use a small environment allowlist; parent model flags and shell startup hooks
// must not change the fixture's topology, duration or trusted fake builder.
func modelFixtureEnvironment(inherited []string) []string {
	var result []string
	for _, item := range inherited {
		name, _, _ := strings.Cut(item, "=")
		switch name {
		case "PATH", "HOME", "TMPDIR", "SYSTEMROOT":
			result = append(result, item)
		}
	}

	return result
}
