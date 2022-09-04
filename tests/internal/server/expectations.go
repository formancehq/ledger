package server

import (
	"encoding/json"
	"fmt"

	ledgerclient "github.com/numary/ledger/tests/internal/client"
	"github.com/onsi/gomega/types"
	"github.com/pkg/errors"
)

type isLedgerErrorCode struct {
	code ledgerclient.ErrorCode
}

func (a isLedgerErrorCode) Match(actual interface{}) (success bool, err error) {
	err, ok := actual.(error)
	if !ok {
		return false, errors.New("have trace expect an object of type error")
	}

	ledgerErr, ok := err.(ledgerclient.GenericOpenAPIError)
	if !ok {
		return false, errors.New("error is not of type ledgerclient.GenericOpenAPIError")
	}

	response := &ledgerclient.ErrorResponse{}
	if err := json.Unmarshal(ledgerErr.Body(), response); err != nil {
		return false, err
	}

	if response.ErrorCode != a.code {
		return false, errors.New("error is not of type ledgerclient.GenericOpenAPIError")
	}

	return true, nil
}

func (a isLedgerErrorCode) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected '%s' to have code \r\n%#v", actual, a.code)
}

func (a isLedgerErrorCode) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected '%s' to not have code \r\n%#v", actual, a.code)
}

var _ types.GomegaMatcher = &isLedgerErrorCode{}

func HaveLedgerErrorCode(code ledgerclient.ErrorCode) *isLedgerErrorCode {
	return &isLedgerErrorCode{
		code: code,
	}
}
