package command

import (
	"fmt"

	"github.com/onsi/gomega/format"
)

func init() {
	format.UseStringerRepresentation = true
}

func BoolFlag(flag string) string {
	return fmt.Sprintf("--%s", flag)
}

func Flag(flag, value string) string {
	return fmt.Sprintf("--%s=%s", flag, value)
}
