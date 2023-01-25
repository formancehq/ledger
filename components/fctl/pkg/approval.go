package fctl

import (
	"fmt"
	"strings"

	"github.com/formancehq/fctl/membershipclient"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var ErrMissingApproval = errors.New("Missing approval.")

var interactiveContinue = pterm.InteractiveContinuePrinter{
	DefaultValueIndex: 0,
	DefaultText:       "Do you want to continue",
	TextStyle:         &pterm.ThemeDefault.PrimaryStyle,
	Options:           []string{"y", "n"},
	OptionsStyle:      &pterm.ThemeDefault.SuccessMessageStyle,
	SuffixStyle:       &pterm.ThemeDefault.SecondaryStyle,
}

const (
	ProtectedStackMetadata = "github.com/formancehq/fctl/protected"
	confirmFlag            = "confirm"
)

func IsProtectedStack(stack *membershipclient.Stack) bool {
	return stack.Metadata != nil && (stack.Metadata)[ProtectedStackMetadata] == "Yes"
}

func NeedConfirm(cmd *cobra.Command, stack *membershipclient.Stack) bool {
	if !IsProtectedStack(stack) {
		return false
	}
	if GetBool(cmd, confirmFlag) {
		return false
	}
	return true
}

func CheckStackApprobation(cmd *cobra.Command, stack *membershipclient.Stack, disclaimer string, args ...any) bool {
	if !IsProtectedStack(stack) {
		return true
	}
	if GetBool(cmd, confirmFlag) {
		return true
	}

	disclaimer = fmt.Sprintf(disclaimer, args...)

	result, err := interactiveContinue.WithDefaultText(disclaimer + ".\r\n" + pterm.DefaultInteractiveContinue.DefaultText).Show()
	if err != nil {
		panic(err)
	}
	return strings.ToLower(result) == "y"
}

func CheckOrganizationApprobation(cmd *cobra.Command, disclaimer string, args ...any) bool {
	if GetBool(cmd, confirmFlag) {
		return true
	}

	result, err := interactiveContinue.WithDefaultText(disclaimer + ".\r\n" + pterm.DefaultInteractiveContinue.DefaultText).Show()
	if err != nil {
		panic(err)
	}
	return strings.ToLower(result) == "yes"
}
