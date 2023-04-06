package cmd

import (
	"os"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/pgtesting"
)

// TODO(gfyrag): remove the need of pg on this package
func TestMain(t *testing.M) {
	if err := pgtesting.CreatePostgresServer(); err != nil {
		panic(err)
	}

	code := t.Run()
	_ = pgtesting.DestroyPostgresServer()
	os.Exit(code)
}
