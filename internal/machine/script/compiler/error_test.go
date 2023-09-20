package compiler

import (
	"testing"
)

func TestEndCharacter(t *testing.T) {
	src := `
	send [CREDIT 200] (
		source = @a
		destination = {
			500% to @b
			50% to @c
		}
	)
	`

	_, err := Compile(src)
	if err == nil {
		t.Fatal("expected error and got none")
	}

	if _, ok := err.(*CompileErrorList); !ok {
		t.Fatal("error had wrong type")
	}

	compErr := err.(*CompileErrorList).Errors[0]

	if compErr.StartL != 5 {
		t.Fatalf("start line was %v", compErr.StartL)
	}
	if compErr.StartC != 3 {
		t.Fatalf("start character was %v", compErr.StartC)
	}
	if compErr.EndL != 5 {
		t.Fatalf("end line was %v", compErr.EndL)
	}
	if compErr.EndC != 7 {
		t.Fatalf("end character was %v", compErr.EndC)
	}
}
