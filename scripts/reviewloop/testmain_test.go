package main

import (
	"os"
	"testing"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestMain(testingMain *testing.M) {
	if err := testenv.SanitizeProcess(); err != nil {
		panic(err)
	}
	os.Exit(testingMain.Run())
}
