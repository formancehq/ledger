package cmd

import (
	"context"
	"fmt"
	"go.uber.org/fx"
	"testing"
)

func TestContainer(t *testing.T) {
	run := make(chan struct{}, 1)
	app := NewContainer(
		fx.Invoke(func() {
			fmt.Println("run invoke")
			run <- struct{}{}
		}),
	)
	err := app.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Stop(context.Background())

	select {
	case <-run:
	default:
		t.Fatal("application not started correctly")
	}
}
