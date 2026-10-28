package cmdutil

import (
	"errors"
	"os"
	"testing"

	"github.com/pterm/pterm"
)

func TestPromptsRejectNonTerminalStdin(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
	_ = write.Close() // EOF must not cause a prompt loop.

	old := os.Stdin
	os.Stdin = read
	t.Cleanup(func() { os.Stdin = old })

	if CanPrompt() {
		t.Fatal("pipe reported as terminal")
	}
	if _, err := PromptText(pterm.DefaultInteractiveTextInput); !errors.Is(err, ErrNonInteractiveInput) {
		t.Fatalf("text prompt: %v", err)
	}
	if _, err := PromptConfirm(pterm.DefaultInteractiveConfirm); !errors.Is(err, ErrNonInteractiveInput) {
		t.Fatalf("confirm prompt: %v", err)
	}
	if _, err := PromptSelect(&pterm.DefaultInteractiveSelect); !errors.Is(err, ErrNonInteractiveInput) {
		t.Fatalf("select prompt: %v", err)
	}
}
