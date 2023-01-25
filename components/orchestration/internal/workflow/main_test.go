package workflow

import (
	"log"
	"os"
	"testing"

	"github.com/formancehq/go-libs/pgtesting"
	flag "github.com/spf13/pflag"
)

func TestMain(m *testing.M) {
	flag.Parse()

	if err := pgtesting.CreatePostgresServer(); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		log.Println("unable to stop postgres server", err)
	}
	os.Exit(code)
}
