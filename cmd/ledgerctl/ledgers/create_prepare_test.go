package ledgers

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
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
	requests, err := PrepareCreate(cmd)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, requests)
	answer := "unchanged"
	require.ErrorIs(t, askCreate(cmd, &survey.Input{Message: "Ledger name"}, &answer), context.Canceled)
	require.Equal(t, "unchanged", answer)
	require.Empty(t, output.String())
}

type unreadCreateInput struct{}

func (unreadCreateInput) Read([]byte) (int, error) { panic("canceled preparation read input") }

func TestPrepareCreateRequiresNameWithoutTerminal(t *testing.T) {
	t.Parallel()
	cmd := NewCreateCommand()
	cmd.SetIn(strings.NewReader(""))
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	requests, err := PrepareCreate(cmd)
	require.Nil(t, requests)
	require.EqualError(t, err, "ledger name is required (use --name flag)")
	var displayed *cmdutil.CLIError
	require.NotErrorAs(t, err, &displayed)
	require.Empty(t, output.String())
}

func TestPrepareCreateKeepsLedgerAndIndexesInOneProposal(t *testing.T) {
	t.Parallel()
	cmd := NewCreateCommand()
	cmd.SetIn(strings.NewReader(""))
	require.NoError(t, cmd.ParseFlags([]string{
		"--name", "books", "--schema", "account:age:int64",
		"--index", "reference", "--index", "metadata:account:role",
		"--mode", "mirror", "--mirror-base-url", "https://mirror.example",
		"--default-enforcement-mode", "AUDIT",
	}))
	requests, err := PrepareCreate(cmd)
	require.NoError(t, err)
	require.Len(t, requests, 3)
	ledger := requests[0].GetCreateLedger()
	require.Equal(t, "books", ledger.GetName())
	require.Len(t, ledger.GetInitialSchema(), 1)
	require.Equal(t, "age", ledger.GetInitialSchema()[0].GetKey())
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, ledger.GetMode())
	require.Equal(t, "books", ledger.GetMirrorSource().GetLedgerName())
	require.Equal(t, "https://mirror.example", ledger.GetMirrorSource().GetHttp().GetBaseUrl())
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, ledger.GetDefaultEnforcementMode())
	for _, request := range requests[1:] {
		require.Equal(t, "books", request.GetCreateIndex().GetLedger())
	}
	require.Equal(t, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, requests[1].GetCreateIndex().GetId().GetTxBuiltin())
	require.Equal(t, "role", requests[2].GetCreateIndex().GetId().GetMetadata().GetKey())
}

func TestPrepareCreateReturnsNoPartialProposalOnInvalidFlags(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		flags []string
		error string
	}{
		{"duplicate index", []string{"--index", "reference", "--index", "reference"}, "duplicate initial index"},
		{"mirror conflicts with normal mode", []string{"--mode", "normal", "--mirror-base-url", "https://mirror.example"}, "mirror flags provided"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := NewCreateCommand()
			cmd.SetIn(strings.NewReader(""))
			require.NoError(t, cmd.ParseFlags(append([]string{"--name", "books"}, tc.flags...)))
			requests, err := PrepareCreate(cmd)
			require.ErrorContains(t, err, tc.error)
			require.Nil(t, requests)
		})
	}
}

func TestRenderCreateReturnsUndisplayedResponseError(t *testing.T) {
	t.Parallel()
	err := RenderCreate(NewCreateCommand(), &servicepb.ApplyResponse{})
	require.EqualError(t, err, "no response received")
	var displayed *cmdutil.CLIError
	require.NotErrorAs(t, err, &displayed)
}

func TestRenderCreateStructuredStdoutContainsOnlyJSON(t *testing.T) {
	const helper = "LEDGER_LEDGERS_CREATE_JSON_HELPER"
	if os.Getenv(helper) == "1" {
		cmd := NewCreateCommand()
		cmd.SetOut(os.Stdout)
		require.NoError(t, cmd.ParseFlags([]string{"--json"}))
		resp := &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name: "books",
					Metadata: map[string]*commonpb.MetadataValue{
						"number": commonpb.NewIntValue(9007199254740993),
						"label":  commonpb.NewStringValue("initial"),
					},
				},
			}},
		}}}
		// Catch the native spinner's prefix as well as any human rendering.
		require.NoError(t, renderCreate(cmd, resp, cmdutil.StartSpinner("Creating ledger...")))
		os.Exit(0)
	}
	t.Parallel()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRenderCreateStructuredStdoutContainsOnlyJSON$")
	command.Env = append(os.Environ(), helper+"=1")
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	output, err := command.Output()
	require.NoError(t, err, diagnostics.String())
	var ledger map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(output, &ledger), "stdout must contain exactly one JSON value: %s", output)
	require.JSONEq(t, `"books"`, string(ledger["name"]))
	require.JSONEq(t, `{"number":9007199254740993,"label":"initial"}`, string(ledger["metadata"]))
	require.Empty(t, diagnostics.String())
}
