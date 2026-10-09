package cmdutil

import (
	"errors"
	"io"
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// ConfirmNextPage waits on the command context, independently of the completed
// page RPC's timeout. Enter loads the next page; n or q ends the walk.
func ConfirmNextPage(cmd *cobra.Command) (bool, error) {
	ctx := CommandContextOrBackground(cmd)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	input, ok := cmd.InOrStdin().(*os.File)
	if !ok || !term.IsTerminal(int(input.Fd())) {
		return false, errors.New("interactive pagination requires a terminal (use --json, --yaml, or --all)")
	}
	prompt := &paginationPrompt{Confirm: survey.Confirm{
		Message: "Load next page? (q to quit)",
		Default: true,
	}}
	var next bool
	err := AskOneContext(ctx, prompt, &next, input, paginationPromptOutput{Writer: cmd.ErrOrStderr()}, cmd.ErrOrStderr())

	return next, err
}

// Read one key, as the native pager does, while Survey and AskOneContext own
// terminal restoration and cancellation. A line-based Confirm needs Enter even
// for n, and does not recognize the pager's documented q shortcut.
type paginationPrompt struct{ survey.Confirm }

func (prompt *paginationPrompt) Prompt(config *survey.PromptConfig) (answer any, promptErr error) {
	reader := prompt.NewRuneReader()
	if err := reader.SetTermMode(); err != nil {
		return false, err
	}
	defer func() { promptErr = errors.Join(promptErr, reader.RestoreTermMode()) }()
	if err := prompt.Render(survey.ConfirmQuestionTemplate, survey.ConfirmTemplateData{Confirm: prompt.Confirm, Config: config}); err != nil {
		return false, err
	}
	for {
		key, _, err := reader.ReadRune()
		if err != nil {
			return false, err
		}
		switch key {
		case '\r', '\n', 'y', 'Y':
			return true, nil
		case 'n', 'N', 'q', 'Q':
			return false, nil
		case terminal.KeyInterrupt:
			return false, terminal.InterruptErr
		case terminal.KeyEndTransmission:
			return false, io.EOF
		}
	}
}

// Survey uses stderr's terminal descriptor while prompt bytes stay scoped to
// the command's diagnostic writer, including when that writer captures output.
type paginationPromptOutput struct{ io.Writer }

func (paginationPromptOutput) Fd() uintptr { return os.Stderr.Fd() }
