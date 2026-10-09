package transactions

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlecAivazis/survey/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestPrepareCreateCanceledContextDoesNotReadInput(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cmd := NewCreateCommand()
	cmd.SetContext(ctx)
	cmd.SetIn(unreadCreateInput{})
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	payload, err := PrepareCreate(cmd, "books")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, payload)
	answer := "unchanged"
	require.ErrorIs(t, askCreate(cmd, &survey.Input{Message: "Posting"}, &answer), context.Canceled)
	require.Equal(t, "unchanged", answer)
	_, err = promptVariable(cmd, "amount", "monetary")
	require.ErrorIs(t, err, context.Canceled)
	_, err = promptPosting(cmd, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, output.String())
}

type unreadCreateInput struct{}

func (unreadCreateInput) Read([]byte) (int, error) { panic("canceled preparation read input") }
func TestPrepareCreateRejectsInputConflictsWithoutRPCOrOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		flags []string
		error string
	}{
		{"script and posting", []string{"--script", "unread.num", "--posting", "world,bank,1,USD"}, "--script and --posting are mutually exclusive"},
		{"variables without script", []string{"--var", "amount=1"}, "--var can only be used with --script"},
		{"noninteractive empty input", nil, "transaction input is required (use --posting, --script, or --data)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := NewCreateCommand()
			cmd.SetIn(strings.NewReader(""))
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			require.NoError(t, cmd.ParseFlags(tc.flags))
			payload, err := PrepareCreate(cmd, "books")
			require.Nil(t, payload)
			require.EqualError(t, err, tc.error)
			var displayed *cmdutil.CLIError
			require.NotErrorAs(t, err, &displayed)
			require.Empty(t, output.String())
		})
	}
}

func TestPrepareCreatePreservesLargePostingAndNativeOptions(t *testing.T) {
	t.Parallel()
	const amount = "18446744073709551616000000001"
	cmd := NewCreateCommand()
	cmd.SetIn(strings.NewReader(""))
	require.NoError(t, cmd.ParseFlags([]string{
		"--posting", "world,bank," + amount + ",USD/2,blue",
		"--reference", "payment-42", "--metadata", "team=ops", "--force",
	}))
	payload, err := PrepareCreate(cmd, "books")
	require.NoError(t, err)
	require.Len(t, payload.GetPostings(), 1)
	require.Equal(t, amount, payload.GetPostings()[0].GetAmount().Dec())
	require.Equal(t, "blue", payload.GetPostings()[0].GetColor())
	require.Equal(t, "USD/2", payload.GetPostings()[0].GetAsset())
	require.Equal(t, "payment-42", payload.GetReference())
	require.Equal(t, "ops", commonpb.MetadataValueToString(payload.GetMetadata()["team"]))
	require.True(t, payload.GetForce())
}

func TestPrepareCreatePreservesScriptVariablesAndRejectsMissingInput(t *testing.T) {
	t.Parallel()
	const script = "vars {\n monetary $amount\n account $destination\n}\nsend $amount (\n source = @world\n destination = $destination\n)\n"
	path := filepath.Join(t.TempDir(), "transfer.num")
	require.NoError(t, os.WriteFile(path, []byte(script), 0o600))
	cmd := NewCreateCommand()
	cmd.SetIn(strings.NewReader(""))
	require.NoError(t, cmd.ParseFlags([]string{"--script", path, "--var", "amount=USD/2 9007199254740993", "--var", "destination=bank"}))
	payload, err := PrepareCreate(cmd, "books")
	require.NoError(t, err)
	require.Equal(t, script, payload.GetScript().GetPlain())
	require.Equal(t, "USD/2 9007199254740993", payload.GetScript().GetVars()["amount"])
	require.Equal(t, "bank", payload.GetScript().GetVars()["destination"])
	missing := NewCreateCommand()
	missing.SetIn(strings.NewReader(""))
	require.NoError(t, missing.ParseFlags([]string{"--script", path}))
	payload, err = PrepareCreate(missing, "books")
	require.Nil(t, payload)
	require.EqualError(t, err, "missing script variables (use --var): amount, destination")
}

func TestRenderCreateReturnsUndisplayedResponseError(t *testing.T) {
	t.Parallel()
	err := RenderCreate(NewCreateCommand(), &servicepb.ApplyResponse{})
	require.EqualError(t, err, "no response received")
	var displayed *cmdutil.CLIError
	require.NotErrorAs(t, err, &displayed)
}

func TestRenderCreateStructuredStdoutContainsOnlyJSON(t *testing.T) {
	const helper = "LEDGER_TRANSACTIONS_CREATE_JSON_HELPER"
	if os.Getenv(helper) == "1" {
		cmd := NewCreateCommand()
		cmd.SetOut(os.Stdout)
		require.NoError(t, cmd.ParseFlags([]string{"--json"}))
		resp := &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{
					Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{
							Transaction: &commonpb.Transaction{Id: 42, Reference: "payment-42"},
						},
					}},
				}},
			}},
		}}}
		// Catch the native spinner's prefix as well as any human rendering.
		require.NoError(t, renderCreate(cmd, resp, cmdutil.StartSpinner("Creating transaction...")))
		os.Exit(0)
	}
	t.Parallel()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRenderCreateStructuredStdoutContainsOnlyJSON$")
	command.Env = append(os.Environ(), helper+"=1")
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	output, err := command.Output()
	require.NoError(t, err, diagnostics.String())
	var created struct {
		Transaction struct {
			Reference string `json:"reference"`
		} `json:"transaction"`
	}
	require.NoError(t, json.Unmarshal(output, &created), "stdout must contain exactly one JSON value: %s", output)
	require.Equal(t, "payment-42", created.Transaction.Reference)
	require.Empty(t, diagnostics.String())
}
