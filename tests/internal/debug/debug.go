package debug

import (
	"fmt"
	"testing"
)

func Debug(format string, args ...interface{}) {
	if testing.Verbose() {
		fmt.Printf(format+"\r\n", args...)
	}
}
