package core

import (
	"strings"
	"testing"
)

func TestNotBetween0And1(t *testing.T) {
	_, err := ParsePortionSpecific("3/2")
	if err == nil {
		t.Fatal("should have errored")
	}
	if !strings.Contains(err.Error(), "between") {
		t.Fatal("wrong error")
	}
}

func TestInvalidFormat(t *testing.T) {
	_, err := ParsePortionSpecific("this is not a portion")
	if err == nil {
		t.Fatal("should have errored")
	}
	if !strings.Contains(err.Error(), "format") {
		t.Fatal("wrong error")
	}
}
