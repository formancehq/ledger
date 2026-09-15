package ledgerv3

import (
	"errors"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func commandByPath(t *testing.T, path ...string) sdk.Command {
	t.Helper()
	want := strings.Join(path, " ")
	for _, command := range (Plugin{}).Commands() {
		if strings.Join(command.Path, " ") == want {
			return command
		}
	}
	t.Fatalf("no command %q in the catalogue", want)
	return sdk.Command{}
}

func failureCode(t *testing.T, err error) sdk.FailureCode {
	t.Helper()
	var failure sdk.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("error %v is not an sdk.Failure", err)
	}
	return sdk.FailureCode(failure.Code)
}

func TestDecodeBindsArgumentsFlagsAndDeclaredDefaults(t *testing.T) {
	t.Parallel()

	command := commandByPath(t, "accounts", "list")
	got, err := decode(command, sdk.ExecuteRequest{
		Arguments: []string{"main"},
		Flags:     []sdk.FlagOccurrence{{Name: flagCursor, Value: "abc"}},
	})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.text(argLedger) != "main" {
		t.Fatalf("ledger = %q", got.text(argLedger))
	}
	if got.text(flagCursor) != "abc" {
		t.Fatalf("cursor = %q", got.text(flagCursor))
	}
	if size := got.int32(flagPageSize); size != 50 {
		t.Fatalf("page-size = %d, want the declared default 50", size)
	}
}

func TestDecodeRejectsAnUndeclaredFlag(t *testing.T) {
	t.Parallel()

	_, err := decode(commandByPath(t, "accounts", "list"), sdk.ExecuteRequest{
		Arguments: []string{"main"},
		Flags:     []sdk.FlagOccurrence{{Name: "not-a-flag", Value: "x"}},
	})
	if err == nil {
		t.Fatal("decode accepted an undeclared flag")
	}
	if code := failureCode(t, err); code != sdk.FailureInvalidArgument {
		t.Fatalf("failure code = %q, want %q", code, sdk.FailureInvalidArgument)
	}
}

func TestDecodeRejectsAMissingRequiredArgument(t *testing.T) {
	t.Parallel()

	_, err := decode(commandByPath(t, "accounts", "list"), sdk.ExecuteRequest{})
	if err == nil {
		t.Fatal("decode accepted a missing required argument")
	}
	if code := failureCode(t, err); code != sdk.FailureInvalidArgument {
		t.Fatalf("failure code = %q", code)
	}
}

func TestDecodeRejectsSurplusPositionalArguments(t *testing.T) {
	t.Parallel()

	_, err := decode(commandByPath(t, "accounts", "list"), sdk.ExecuteRequest{
		Arguments: []string{"main", "surplus"},
	})
	if err == nil {
		t.Fatal("decode accepted a surplus positional argument")
	}
	if code := failureCode(t, err); code != sdk.FailureInvalidArgument {
		t.Fatalf("failure code = %q", code)
	}
}

func TestDecodeRejectsARepeatedScalarFlag(t *testing.T) {
	t.Parallel()

	_, err := decode(commandByPath(t, "accounts", "list"), sdk.ExecuteRequest{
		Arguments: []string{"main"},
		Flags: []sdk.FlagOccurrence{
			{Name: flagCursor, Value: "one"},
			{Name: flagCursor, Value: "two"},
		},
	})
	if err == nil {
		t.Fatal("decode accepted a repeated scalar flag")
	}
	if code := failureCode(t, err); code != sdk.FailureInvalidArgument {
		t.Fatalf("failure code = %q", code)
	}
}

func TestDecodeRejectsAValueOutsideADeclaredCompletionSet(t *testing.T) {
	t.Parallel()

	_, err := decode(commandByPath(t, "account-types", "set-default-enforcement"), sdk.ExecuteRequest{
		Arguments: []string{"main"},
		Flags:     []sdk.FlagOccurrence{{Name: flagEnforcementMode, Value: "permissive"}},
	})
	if err == nil {
		t.Fatal("decode accepted an enum value the descriptor does not declare")
	}
	if code := failureCode(t, err); code != sdk.FailureInvalidArgument {
		t.Fatalf("failure code = %q", code)
	}
}

func TestDecodeRejectsPageSizesOutsideTheProductBound(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"-1", "0", "1001"} {
		_, err := decode(commandByPath(t, "accounts", "list"), sdk.ExecuteRequest{
			Arguments: []string{"main"},
			Flags:     []sdk.FlagOccurrence{{Name: flagPageSize, Value: value}},
		})
		if err == nil {
			t.Fatalf("decode accepted page-size %s", value)
		}
		if code := failureCode(t, err); code != sdk.FailureInvalidArgument {
			t.Fatalf("page-size %s failure code = %q", value, code)
		}
	}
}

func TestUint64RejectsAValueThatWouldTruncate(t *testing.T) {
	t.Parallel()

	command := commandByPath(t, "transactions", "get")
	input, err := decode(command, sdk.ExecuteRequest{Arguments: []string{"main", "18446744073709551616"}})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, err := input.uint64(argTransactionID); err == nil {
		t.Fatal("uint64 accepted a value above the 64-bit range")
	}
}

func TestUint64AcceptsTheFullSixtyFourBitRange(t *testing.T) {
	t.Parallel()

	command := commandByPath(t, "transactions", "get")
	input, err := decode(command, sdk.ExecuteRequest{Arguments: []string{"main", "18446744073709551615"}})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	value, err := input.uint64(argTransactionID)
	if err != nil {
		t.Fatalf("uint64: %v", err)
	}
	if value != 1<<64-1 {
		t.Fatalf("uint64 = %d, want the maximum fixed64", value)
	}
}
