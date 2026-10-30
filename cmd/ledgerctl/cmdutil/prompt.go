package cmdutil

import (
	"errors"
	"os"

	"golang.org/x/term"
)

// ErrNonInteractiveInput is returned before starting a pterm prompt when stdin
// cannot supply key presses. pterm's keyboard reader otherwise loops on EOF.
var ErrNonInteractiveInput = errors.New("stdin is not a terminal; supply the required argument or flag")

// CanPrompt reports whether stdin supports pterm's interactive keyboard reader.
func CanPrompt() bool { return term.IsTerminal(int(os.Stdin.Fd())) }

func PromptText(prompt interface {
	Show(...string) (string, error)
}) (string, error) {
	if !CanPrompt() {
		return "", ErrNonInteractiveInput
	}

	return prompt.Show()
}

func PromptSelect(prompt interface {
	Show(...string) (string, error)
}) (string, error) {
	if !CanPrompt() {
		return "", ErrNonInteractiveInput
	}

	return prompt.Show()
}

func PromptConfirm(prompt interface{ Show(...string) (bool, error) }) (bool, error) {
	if !CanPrompt() {
		return false, ErrNonInteractiveInput
	}

	return prompt.Show()
}
