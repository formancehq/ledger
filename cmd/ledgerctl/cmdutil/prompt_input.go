package cmdutil

import (
	"cmp"
	"context"
	"io"
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/spf13/cobra"
)

// CommandContextOrBackground also supports commands prepared before Execute.
func CommandContextOrBackground(cmd *cobra.Command) context.Context {
	return cmp.Or(cmd.Context(), context.Background())
}

// AskOneContext waits for Survey to finish restoring the terminal on cancellation.
// Input and terminal ownership stay with the caller; no standard stream is closed.
func AskOneContext(ctx context.Context, prompt survey.Prompt, response any, input *os.File, output terminal.FileWriter, diagnostics io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	err := runPrompt(ctx, func() error {
		return survey.AskOne(prompt, response, survey.WithStdio(&promptInput{ctx: ctx, file: input}, output, diagnostics))
	})
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	return err
}

type promptInput struct {
	ctx  context.Context
	file *os.File
}

func (input *promptInput) Fd() uintptr { return input.file.Fd() }

func (input *promptInput) Read(buffer []byte) (int, error) {
	if err := input.ctx.Err(); err != nil {
		return 0, err
	}
	if len(buffer) == 0 {
		return 0, nil
	}

	n, err := input.read(buffer)
	if ctxErr := input.ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}

	return n, err
}
