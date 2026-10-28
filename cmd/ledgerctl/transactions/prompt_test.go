package transactions

import (
	"os"
	"strings"
	"testing"
)

func TestPromptVariableAtEOF(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
	_ = write.Close()

	old := os.Stdin
	os.Stdin = read
	t.Cleanup(func() { os.Stdin = old })

	_, err = promptVariable("amt", "monetary")
	if err == nil || !strings.Contains(err.Error(), "missing --var for $amt") {
		t.Fatalf("expected actionable non-terminal error, got %v", err)
	}
}
